/**
 * Harness v1 — per-type subagent step and token budgets (PF-7474).
 *
 * A real parent agent runs a real child through the genuine agentic loop; only
 * the language models are mocked. The child never reports an outcome and keeps
 * calling a tool, so only the framework budget can stop it.
 */

import { describe, expect, it, vi } from 'vitest';
import { z } from 'zod';

import { Agent } from '../../agent';
import { convertArrayToReadableStream, MockLanguageModelV2 } from '../../agent/__tests__/mock-model';
import { mergeAgentExecutionOptions } from '../../agent/merge-execution-options';
import { InMemoryStore } from '../../storage';
import { createTool } from '../../tools';

import { setupHarness } from './__test-utils__';

import type { HarnessEvent } from './events';
import { Harness } from './harness';
import type { SubagentDefinition } from './types';

const testUsage = { inputTokens: 10, outputTokens: 20, totalTokens: 30 };

function textStream(text: string) {
  return convertArrayToReadableStream([
    { type: 'stream-start', warnings: [] },
    { type: 'response-metadata', id: 'id-text', modelId: 'mock-model-id', timestamp: new Date(0) },
    { type: 'text-start', id: 'text-1' },
    { type: 'text-delta', id: 'text-1', delta: text },
    { type: 'text-end', id: 'text-1' },
    { type: 'finish', finishReason: 'stop', usage: testUsage },
  ]);
}

/** Visible progress text, then one tool call; the provider finishes `tool-calls`. */
function progressToolCallStream(toolCallId: string, toolName: string, inputJson: string, text = '') {
  return convertArrayToReadableStream([
    { type: 'stream-start', warnings: [] },
    { type: 'response-metadata', id: `id-${toolCallId}`, modelId: 'mock-model-id', timestamp: new Date(0) },
    ...(text === ''
      ? []
      : [
          { type: 'text-start', id: `text-${toolCallId}` },
          { type: 'text-delta', id: `text-${toolCallId}`, delta: text },
          { type: 'text-end', id: `text-${toolCallId}` },
        ]),
    { type: 'tool-call', toolCallId, toolName, input: inputJson, providerExecuted: false },
    { type: 'finish', finishReason: 'tool-calls', usage: testUsage },
  ]);
}

/**
 * Child that searches forever and never reports an outcome. The parent first
 * calls `parentToolName` with `parentInput`, then ends its turn with text.
 */
function budgetFixture(
  budget: Pick<SubagentDefinition, 'maxSteps' | 'maxTotalTokens'>,
  parentCalls: (call: number) => ReturnType<typeof progressToolCallStream>,
  childStream: (call: number) => ReturnType<typeof progressToolCallStream> = call =>
    progressToolCallStream(`search-${call}`, 'searchPapers', '{}', `Searched ${call}. `),
  childAgentOptions: { durable?: boolean; defaultOptions?: Record<string, unknown> } = {},
) {
  const search = vi.fn(async () => ({ hits: 1 }));
  const searchPapers = createTool({
    id: 'searchPapers',
    description: 'search',
    inputSchema: z.object({}),
    execute: search,
  });
  let childCalls = 0;
  const childAgent = new Agent({
    id: 'child-agent',
    name: 'child-agent',
    instructions: 'Search until done, then report.',
    ...childAgentOptions,
    model: new MockLanguageModelV2({
      doStream: async () => {
        childCalls++;
        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream: childStream(childCalls),
        };
      },
    }),
  });
  let parentCall = 0;
  const parentAgent = new Agent({
    id: 'parent-agent',
    name: 'parent-agent',
    instructions: 'Delegate the search.',
    model: new MockLanguageModelV2({
      doStream: async () => ({
        rawCall: { rawPrompt: null, rawSettings: {} },
        warnings: [],
        stream: parentCalls(++parentCall),
      }),
    }),
  });
  const harness = new Harness({
    agents: { 'parent-agent': parentAgent, 'child-agent': childAgent },
    storage: new InMemoryStore(),
    modes: [
      { id: 'default', agentId: 'parent-agent' },
      {
        id: 'search-only',
        agentId: 'child-agent',
        tools: { searchPapers },
        harnessBuiltins: 'exclude',
        permissions: { categories: {}, tools: { searchPapers: 'allow' } },
      },
    ],
    defaultModeId: 'default',
    subagents: {
      maxDepth: 2,
      types: {
        searcher: {
          agentId: 'child-agent',
          modeId: 'search-only',
          description: 'Search papers',
          toolAllowlist: ['searchPapers'],
          ...budget,
        },
      },
    },
  });
  return { harness, search, childCalls: () => childCalls };
}

const spawnThenAnswer = (call: number) =>
  call === 1
    ? progressToolCallStream(
        'parent-spawn',
        'spawn_subagent',
        JSON.stringify({ agentType: 'searcher', task: 'Find papers on X.' }),
      )
    : textStream('The search was partial.');

describe('Harness v1 — per-type subagent budgets', () => {
  it('stops an inline child at its step budget and hands the parent a typed partial result', async () => {
    const { harness, search, childCalls } = budgetFixture({ maxSteps: 2 }, spawnThenAnswer);
    try {
      const session = await harness.session({ resourceId: 'budget-user', threadId: { fresh: true } });
      const events: HarnessEvent[] = [];
      session.subscribe(event => events.push(event));

      await session.message({ content: 'Find papers on X.' });

      expect(childCalls()).toBe(2);
      expect(search).toHaveBeenCalledTimes(2);
      const spawnEnd = events.find(
        event => event.type === 'tool_end' && (event as { toolName?: string }).toolName === 'spawn_subagent',
      ) as { output: any } | undefined;
      expect(spawnEnd?.output).toMatchObject({
        isError: true,
        result: {
          status: 'error',
          stepCount: 2,
          text: expect.stringContaining('Searched 2.'),
          budgetExhausted: { budget: 'steps', limit: 2, used: 2 },
          toolTrace: [{ toolName: 'searchPapers', calls: 2, errors: 0 }],
          usage: { totalTokens: 60 },
          error: { code: 'harness.subagent_budget_exhausted' },
        },
      });

      const end = events.find(event => event.type === 'subagent_end');
      expect(end).toMatchObject({
        parentId: session.id,
        agentType: 'searcher',
        depth: 1,
        isError: true,
        durationMs: expect.any(Number),
        output: {
          budgetExhausted: { budget: 'steps', limit: 2, used: 2 },
          usage: { inputTokens: 20, outputTokens: 40, totalTokens: 60 },
          error: { code: 'harness.subagent_budget_exhausted' },
        },
      });
      // The tool trace serves parent continuation only; the event keeps its cap.
      expect((end as { output: { toolTrace?: unknown } }).output.toolTrace).toBeUndefined();
    } finally {
      await harness.shutdown();
    }
  });

  it('stops an inline child at its token budget on the evented loop', async () => {
    const previous = process.env.MASTRA_EVENTED_EXECUTION;
    process.env.MASTRA_EVENTED_EXECUTION = 'true';
    const { harness, childCalls } = budgetFixture({ maxTotalTokens: 50 }, spawnThenAnswer);
    try {
      const session = await harness.session({ resourceId: 'budget-user', threadId: { fresh: true } });
      const events: HarnessEvent[] = [];
      session.subscribe(event => events.push(event));

      await session.message({ content: 'Find papers on X.' });

      expect(childCalls()).toBe(2);
      expect(events.find(event => event.type === 'subagent_end')).toMatchObject({
        isError: true,
        output: { budgetExhausted: { budget: 'tokens', limit: 50, used: 60 } },
      });
    } finally {
      await harness.shutdown();
      if (previous === undefined) delete process.env.MASTRA_EVENTED_EXECUTION;
      else process.env.MASTRA_EVENTED_EXECUTION = previous;
    }
  });

  it('lets a child that reports its outcome on the last allowed step complete normally', async () => {
    const { harness, childCalls } = budgetFixture({ maxSteps: 2 }, spawnThenAnswer, call =>
      call === 1
        ? progressToolCallStream('search-1', 'searchPapers', '{}')
        : progressToolCallStream(
            'child-outcome',
            'report_subagent_outcome',
            JSON.stringify({
              outcome: 'completed',
              summary: 'Found one paper.',
              evidence: [{ kind: 'tool-result', toolName: 'searchPapers', status: 'success', description: 'One hit.' }],
            }),
          ),
    );
    try {
      const session = await harness.session({ resourceId: 'budget-user', threadId: { fresh: true } });
      const events: HarnessEvent[] = [];
      session.subscribe(event => events.push(event));

      await session.message({ content: 'Find papers on X.' });

      expect(childCalls()).toBe(2);
      const end = events.find(event => event.type === 'subagent_end') as { isError: boolean; output: any };
      expect(end).toMatchObject({ isError: false, output: { status: 'success', outcome: 'completed' } });
      expect(end.output.budgetExhausted).toBeUndefined();
    } finally {
      await harness.shutdown();
    }
  });

  it('stops a child that goes silent on its last allowed step without a report-only step (PF-7507)', async () => {
    // Under its budget a silent child gets one report-only step; on the last
    // allowed step that step would be a provider call past the budget.
    const { harness, childCalls } = budgetFixture({ maxSteps: 2 }, spawnThenAnswer, call =>
      call === 1
        ? progressToolCallStream('search-1', 'searchPapers', '{}')
        : convertArrayToReadableStream([
            { type: 'stream-start', warnings: [] },
            { type: 'response-metadata', id: 'id-silent', modelId: 'mock-model-id', timestamp: new Date(0) },
            { type: 'finish', finishReason: 'stop', usage: testUsage },
          ]),
    );
    try {
      const session = await harness.session({ resourceId: 'budget-user', threadId: { fresh: true } });
      const events: HarnessEvent[] = [];
      session.subscribe(event => events.push(event));

      await session.message({ content: 'Find papers on X.' });

      expect(childCalls()).toBe(2);
      expect(events.find(event => event.type === 'subagent_end')).toMatchObject({
        isError: true,
        output: {
          stepCount: 2,
          budgetExhausted: { budget: 'steps', limit: 2, used: 2 },
          error: { code: 'harness.subagent_budget_exhausted' },
        },
      });
    } finally {
      await harness.shutdown();
    }
  });

  it('stops an inline child once its provider-reported tokens reach the token budget', async () => {
    // 30 tokens per step: 30 and 60 stay under 70, the third step reaches 90.
    const { harness, childCalls } = budgetFixture({ maxTotalTokens: 70 }, spawnThenAnswer);
    try {
      const session = await harness.session({ resourceId: 'budget-user', threadId: { fresh: true } });
      const events: HarnessEvent[] = [];
      session.subscribe(event => events.push(event));

      await session.message({ content: 'Find papers on X.' });

      expect(childCalls()).toBe(3);
      expect(events.find(event => event.type === 'subagent_end')).toMatchObject({
        isError: true,
        output: {
          stepCount: 3,
          budgetExhausted: { budget: 'tokens', limit: 70, used: 90 },
          error: { code: 'harness.subagent_budget_exhausted' },
        },
      });
    } finally {
      await harness.shutdown();
    }
  });

  it('stops a delegated child at its step budget and fails its plan task with the typed result', async () => {
    let taskId: string | undefined;
    const { harness, childCalls } = budgetFixture({ maxSteps: 2 }, call =>
      call === 1
        ? progressToolCallStream('parent-add', 'task_add', JSON.stringify({ content: 'Find papers on X' }))
        : call === 2
          ? progressToolCallStream(
              'parent-delegate',
              'task_delegate',
              JSON.stringify({ taskId, agentType: 'searcher' }),
            )
          : textStream('Delegated.'),
    );
    try {
      const session = await harness.session({ resourceId: 'budget-user', threadId: { fresh: true } });
      const events: HarnessEvent[] = [];
      session.subscribe(event => {
        events.push(event);
        const output = (event as { output?: { taskId?: string } }).output;
        if (event.type === 'tool_end' && (event as { toolName?: string }).toolName === 'task_add') {
          taskId = output?.taskId;
        }
      });

      await session.message({ content: 'Delegate the search.' });
      const deadline = Date.now() + 5_000;
      while (!events.some(event => event.type === 'subagent_end') && Date.now() < deadline) {
        await new Promise(resolve => setTimeout(resolve, 10));
      }

      expect(childCalls()).toBe(2);
      expect(events.find(event => event.type === 'subagent_end')).toMatchObject({
        parentId: session.id,
        agentType: 'searcher',
        depth: 1,
        isError: true,
        durationMs: expect.any(Number),
        output: {
          status: 'error',
          budgetExhausted: { budget: 'steps', limit: 2, used: 2 },
          usage: { totalTokens: 60 },
          error: { code: 'harness.subagent_budget_exhausted' },
        },
      });
      const page = await (session as any)._internalStorage.listPlanTasks({ sessionId: session.id, limit: 10 });
      expect(page.tasks.find((task: { taskId: string }) => task.taskId === taskId)?.status).toBe('failed');

      // Crash recovery reads the child's real stored result: the budget stop
      // persisted with it keeps the typed outcome without the live driver.
      const end = events.find(event => event.type === 'subagent_end') as { subagentSessionId: string };
      const childRecord = await (session as any)._internalStorage.loadSession({
        harnessName: session.getRecord().harnessName,
        sessionId: end.subagentSessionId,
      });
      const recovered = await (session as any)._loadDelegationOutcomeFromEvidence(
        childRecord,
        taskId,
        Date.now() + 60_000,
      );
      expect(recovered).toMatchObject({
        outcome: 'failed',
        result: {
          budgetExhausted: { budget: 'steps', limit: 2, used: 2 },
          error: { code: 'harness.subagent_budget_exhausted' },
        },
      });
    } finally {
      await harness.shutdown();
    }
  });

  it('counts a final step a hook reopens, so a continuing hook cannot spend past the budget', async () => {
    const report = progressToolCallStream(
      'child-outcome',
      'report_subagent_outcome',
      JSON.stringify({
        outcome: 'completed',
        summary: 'Done after many turns.',
        evidence: [{ kind: 'analysis', description: 'Reasoned it through.' }],
      }),
    );
    const { harness, childCalls } = budgetFixture(
      { maxSteps: 1 },
      spawnThenAnswer,
      call => (call < 5 ? textStream(`Thinking ${call}.`) : report),
      { defaultOptions: { onIterationComplete: () => ({ continue: true }) } },
    );
    try {
      const session = await harness.session({ resourceId: 'budget-user', threadId: { fresh: true } });
      const events: HarnessEvent[] = [];
      session.subscribe(event => events.push(event));

      await session.message({ content: 'Find papers on X.' });

      expect(childCalls()).toBe(1);
      expect(events.find(event => event.type === 'subagent_end')).toMatchObject({
        isError: true,
        output: {
          budgetExhausted: { budget: 'steps', limit: 1, used: 1 },
          error: { code: 'harness.subagent_budget_exhausted' },
        },
      });
    } finally {
      await harness.shutdown();
    }
  });

  it('does not let hook feedback reopen a durable child stopped at its budget', async () => {
    const report = progressToolCallStream(
      'child-outcome',
      'report_subagent_outcome',
      JSON.stringify({
        outcome: 'completed',
        summary: 'Done after many turns.',
        evidence: [{ kind: 'analysis', description: 'Reasoned it through.' }],
      }),
    );
    const { harness, childCalls } = budgetFixture(
      { maxSteps: 1 },
      spawnThenAnswer,
      call => (call < 5 ? textStream(`Thinking ${call}.`) : report),
      { durable: true, defaultOptions: { onIterationComplete: () => ({ continue: false, feedback: 'Keep going.' }) } },
    );
    try {
      const session = await harness.session({ resourceId: 'budget-user', threadId: { fresh: true } });
      const events: HarnessEvent[] = [];
      session.subscribe(event => events.push(event));

      await session.message({ content: 'Find papers on X.' });

      expect(childCalls()).toBe(1);
      expect(events.find(event => event.type === 'subagent_end')).toMatchObject({
        isError: true,
        output: {
          budgetExhausted: { budget: 'steps', limit: 1, used: 1 },
          error: { code: 'harness.subagent_budget_exhausted' },
        },
      });
    } finally {
      await harness.shutdown();
    }
  });

  it('meters tokens for a durable child agent', async () => {
    const { harness, childCalls } = budgetFixture({ maxTotalTokens: 50 }, spawnThenAnswer, undefined, {
      durable: true,
    });
    try {
      const session = await harness.session({ resourceId: 'budget-user', threadId: { fresh: true } });
      const events: HarnessEvent[] = [];
      session.subscribe(event => events.push(event));

      await session.message({ content: 'Find papers on X.' });

      expect(childCalls()).toBe(2);
      expect(events.find(event => event.type === 'subagent_end')).toMatchObject({
        isError: true,
        output: {
          budgetExhausted: { budget: 'tokens', limit: 50, used: 60 },
          error: { code: 'harness.subagent_budget_exhausted' },
        },
      });
    } finally {
      await harness.shutdown();
    }
  });
});

/**
 * A budgeted child whose every step calls an approval-gated tool, so each
 * step ends in a suspension and the next step runs in a resumed segment.
 */
function approvalChildFixture(storage: InMemoryStore, maxSteps: number) {
  let modelCalls = 0;
  const searchPapers = createTool({
    id: 'searchPapers',
    description: 'search',
    inputSchema: z.object({}),
    requireApproval: true,
    execute: async () => ({ hits: 1 }),
  });
  const agent = new Agent({
    id: 'child-agent',
    name: 'child-agent',
    instructions: 'Search.',
    tools: { searchPapers },
    model: new MockLanguageModelV2({
      doStream: async () => {
        modelCalls++;
        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream: progressToolCallStream(`search-${modelCalls}`, 'searchPapers', '{}'),
        };
      },
    }),
  });
  const harness = new Harness({
    agents: { 'child-agent': agent },
    storage,
    modes: [{ id: 'default', agentId: 'child-agent' }],
    defaultModeId: 'default',
    subagents: { types: { searcher: { agentId: 'child-agent', description: 'Search papers', maxSteps } } },
  });
  return { harness, modelCalls: () => modelCalls };
}

describe('Harness v1 — subagent budgets across resumed segments', () => {
  it('counts every resumed step instead of treating a restarted iteration number as a replay', async () => {
    const { harness, modelCalls } = approvalChildFixture(new InMemoryStore(), 2);
    try {
      const child = await harness.session({
        resourceId: 'budget-user',
        threadId: { fresh: true },
        subagentTypeId: 'searcher',
      });
      expect(((await child.message({ content: 'Search.' })) as { finishReason: string }).finishReason).toBe(
        'suspended',
      );
      for (let approval = 0; approval < 3 && child.getRecord().pendingResume !== undefined; approval++) {
        await child.respondToToolApproval({ approved: true });
      }

      expect(modelCalls()).toBe(2);
      expect(child.getRecord().pendingResume).toBeUndefined();
      expect(child._internalSubagentBudgetExhaustion()).toEqual({ budget: 'steps', limit: 2, used: 2 });
    } finally {
      await harness.shutdown();
    }
  });

  it('keeps the spent budget across a restart while the child is suspended', async () => {
    const storage = new InMemoryStore();
    const first = approvalChildFixture(storage, 2);
    let second: ReturnType<typeof approvalChildFixture> | undefined;
    try {
      const child = await first.harness.session({
        resourceId: 'budget-user',
        threadId: { fresh: true },
        subagentTypeId: 'searcher',
      });
      await child.message({ content: 'Search.' });
      await child.respondToToolApproval({ approved: true });
      expect(child.getRecord().pendingResume?.subagentBudgetSpent).toEqual({ steps: 1, tokens: 30 });
      const threadId = child.threadId;
      await first.harness.shutdown();

      second = approvalChildFixture(storage, 2);
      const rehydrated = await second.harness.session({ resourceId: 'budget-user', threadId });
      await rehydrated.respondToToolApproval({ approved: true });

      // The restored step plus the resumed one reach the budget: no new provider call.
      expect(second.modelCalls()).toBe(0);
      expect(rehydrated.getRecord().pendingResume).toBeUndefined();
      expect(rehydrated._internalSubagentBudgetExhaustion()).toEqual({ budget: 'steps', limit: 2, used: 2 });
    } finally {
      await second?.harness.shutdown();
      await first.harness.shutdown();
    }
  });
});

describe('Harness v1 — subagent budget across a pre-registered question and a restart', () => {
  it('keeps the spent budget when a pre-registered question suspends the child across a restart', async () => {
    const storage = new InMemoryStore();
    const registrations: string[] = [];
    function questionChild() {
      let modelCalls = 0;
      // Pre-registers its question through the Harness slot during the tool
      // call, as durable question tools do, then suspends for the answer.
      const askYear = createTool({
        id: 'ask_user',
        description: 'ask the user',
        inputSchema: z.object({ question: z.string() }),
        execute: async ({ question }, context) => {
          if (context?.agent?.resumeData !== undefined) return { answer: context.agent.resumeData };
          const slot = context?.requestContext?.get('harness') as
            | { registerQuestion?: (params: Record<string, unknown>) => Promise<void> }
            | undefined;
          const toolCallId = context?.agent?.toolCallId ?? 'question';
          await slot?.registerQuestion?.({ questionId: toolCallId, question, toolCallId });
          registrations.push(toolCallId);
          await context?.agent?.suspend?.({ question });
          return undefined;
        },
      });
      const searchPapers = createTool({
        id: 'searchPapers',
        description: 'search',
        inputSchema: z.object({}),
        execute: async () => ({ hits: 1 }),
      });
      const agent = new Agent({
        id: 'child-agent',
        name: 'child-agent',
        instructions: 'Search, then ask.',
        tools: { searchPapers, ask_user: askYear },
        model: new MockLanguageModelV2({
          doStream: async () => {
            modelCalls++;
            return {
              rawCall: { rawPrompt: null, rawSettings: {} },
              warnings: [],
              stream:
                modelCalls === 1
                  ? progressToolCallStream('search-1', 'searchPapers', '{}')
                  : progressToolCallStream(
                      `question-${modelCalls}`,
                      'ask_user',
                      JSON.stringify({ question: 'Which year?' }),
                    ),
            };
          },
        }),
      });
      const harness = new Harness({
        agents: { 'child-agent': agent },
        storage,
        modes: [{ id: 'default', agentId: 'child-agent', harnessBuiltins: 'exclude' }],
        defaultModeId: 'default',
        subagents: { types: { searcher: { agentId: 'child-agent', description: 'Search papers', maxSteps: 2 } } },
      });
      return { harness, modelCalls: () => modelCalls };
    }

    const first = questionChild();
    let second: ReturnType<typeof questionChild> | undefined;
    try {
      const child = await first.harness.session({
        resourceId: 'budget-user',
        threadId: { fresh: true },
        subagentTypeId: 'searcher',
      });
      expect(((await child.message({ content: 'Search.' })) as { finishReason: string }).finishReason).toBe(
        'suspended',
      );
      expect(first.modelCalls()).toBe(2);
      expect(registrations).toEqual(['question-2']);
      expect(child.getRecord().pendingResume).toMatchObject({ kind: 'question', subagentBudgetSpent: { steps: 1 } });
      const threadId = child.threadId;
      await first.harness.shutdown();

      second = questionChild();
      const rehydrated = await second.harness.session({ resourceId: 'budget-user', threadId });
      await rehydrated.respondToQuestion({ answer: '2024' });

      expect(second.modelCalls()).toBe(0);
      expect(rehydrated._internalSubagentBudgetExhaustion()).toEqual({ budget: 'steps', limit: 2, used: 2 });
    } finally {
      await second?.harness.shutdown();
      await first.harness.shutdown();
    }
  });
});

describe('Harness v1 — subagent budget and the report-only step (PF-7507)', () => {
  it('does not meter the report-only step, so a silent child that reports keeps its unspent steps', async () => {
    const registrations: string[] = [];
    const askUser = createTool({
      id: 'ask_user',
      description: 'ask the user',
      inputSchema: z.object({ question: z.string() }),
      execute: async ({ question }, context) => {
        const slot = context?.requestContext?.get('harness') as
          | { registerQuestion?: (params: Record<string, unknown>) => Promise<void> }
          | undefined;
        const toolCallId = context?.agent?.toolCallId ?? 'question';
        await slot?.registerQuestion?.({ questionId: toolCallId, question, toolCallId });
        registrations.push(toolCallId);
        await context?.agent?.suspend?.({ question });
        return undefined;
      },
    });
    const searchPapers = createTool({
      id: 'searchPapers',
      description: 'search',
      inputSchema: z.object({}),
      execute: async () => ({ hits: 1 }),
    });
    const report = JSON.stringify({
      outcome: 'completed',
      summary: 'Found one paper.',
      evidence: [{ kind: 'tool-result', toolName: 'searchPapers', status: 'success', description: 'One hit.' }],
    });
    let modelCalls = 0;
    const agent = new Agent({
      id: 'child-agent',
      name: 'child-agent',
      instructions: 'Search, then report.',
      tools: { searchPapers, ask_user: askUser },
      model: new MockLanguageModelV2({
        doStream: async () => {
          modelCalls++;
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream:
              modelCalls === 1
                ? progressToolCallStream('search-1', 'searchPapers', '{}')
                : modelCalls === 2
                  ? // Silent final after tool work: the one report-only step follows.
                    convertArrayToReadableStream([
                      { type: 'stream-start', warnings: [] },
                      { type: 'response-metadata', id: 'id-silent', modelId: 'mock-model-id', timestamp: new Date(0) },
                      { type: 'finish', finishReason: 'stop', usage: testUsage },
                    ])
                  : modelCalls === 3
                    ? progressToolCallStream('child-outcome', 'report_subagent_outcome', report)
                    : progressToolCallStream('question-4', 'ask_user', JSON.stringify({ question: 'Which year?' })),
          };
        },
      }),
    });
    const harness = new Harness({
      agents: { 'child-agent': agent },
      storage: new InMemoryStore(),
      modes: [{ id: 'default', agentId: 'child-agent', harnessBuiltins: 'exclude' }],
      defaultModeId: 'default',
      subagents: { types: { searcher: { agentId: 'child-agent', description: 'Search papers', maxSteps: 5 } } },
    });
    try {
      const parent = await harness.session({ resourceId: 'budget-user', threadId: { fresh: true } });
      const child = await harness.session({
        resourceId: 'budget-user',
        threadId: { fresh: true },
        parentSessionId: parent.id,
        origin: 'subagent-tool',
        subagentTypeId: 'searcher',
      });
      const events: HarnessEvent[] = [];
      child.subscribe(event => events.push(event));

      await child.message({ content: 'Search.' });

      expect(modelCalls).toBe(3);
      expect(
        events.find(
          event => event.type === 'tool_end' && (event as { toolName: string }).toolName === 'report_subagent_outcome',
        ),
      ).toMatchObject({ isError: false, output: { outcome: 'completed' } });
      expect(child._internalSubagentBudgetExhaustion()).toBeUndefined();

      // The next turn suspends on a question, persisting the spend a restart
      // resumes from. A suspending step is counted once it completes on resume
      // (see the pre-registered question test above), so the spend is the
      // search and the silent final: 2. The report-only step is a Harness
      // recovery step and is not metered; metering it would persist 3.
      await child.message({ content: 'And the year?' });

      expect(modelCalls).toBe(4);
      expect(registrations).toEqual(['question-4']);
      expect(child.getRecord().pendingResume).toMatchObject({ kind: 'question', subagentBudgetSpent: { steps: 2 } });
    } finally {
      await harness.shutdown();
    }
  });
});

/** The Harness iteration hook of a budgeted child session, driven directly. */
async function budgetedIterationHook(maxSteps: number) {
  const setup = setupHarness({
    subagents: { types: { searcher: { agentId: 'default', description: 'Search', maxSteps } } },
  });
  const session = await setup.harness.session({
    resourceId: 'u1',
    threadId: { fresh: true },
    subagentTypeId: 'searcher',
  });
  await session.message({ content: 'go' });
  const call = (setup.agent as unknown as { streamCalls: Array<{ options?: Record<string, unknown> }> }).streamCalls.at(
    -1,
  );
  const merged = mergeAgentExecutionOptions({}, call?.options ?? {});
  const hook = merged.onIterationComplete as (context: Record<string, unknown>) => Promise<unknown>;
  return { hook, session, harness: setup.harness };
}

const toolResult = { id: 'tc1', name: 'searchPapers', result: { hits: 1 } };

describe('Harness v1 — subagent budget stop is terminal', () => {
  it('keeps a budget stop terminal when the stopping iteration is replayed', async () => {
    const { hook, session, harness } = await budgetedIterationHook(2);
    try {
      await hook({ iteration: 1, text: '', toolResults: [toolResult], isFinal: false, finishReason: 'tool-calls' });
      // Silent final step after tools: the empty-reply recovery would reopen it.
      const finalStep = { iteration: 2, text: '', toolResults: [], isFinal: true, finishReason: 'stop' };
      expect(await hook(finalStep)).toEqual({ continue: false });
      expect(await hook(finalStep)).toEqual({ continue: false });
      expect(session._internalSubagentBudgetExhaustion()).toEqual({ budget: 'steps', limit: 2, used: 2 });
    } finally {
      await harness.shutdown();
    }
  });

  it('does not mark a child that reports its outcome at the Harness step ceiling as stopped', async () => {
    const { hook, session, harness } = await budgetedIterationHook(1000);
    try {
      for (let iteration = 1; iteration < 1000; iteration++) {
        await hook({ iteration, text: '', toolResults: [toolResult], isFinal: false, finishReason: 'tool-calls' });
      }
      await hook({
        iteration: 1000,
        text: '',
        toolResults: [
          {
            id: 'report',
            name: 'report_subagent_outcome',
            result: {
              kind: 'subagent-outcome-report',
              outcome: 'completed',
              summary: 'Found it.',
              evidence: [{ kind: 'analysis', description: 'Checked the sources.' }],
            },
          },
        ],
        isFinal: true,
        finishReason: 'tool-calls',
      });
      expect(session._internalSubagentBudgetExhaustion()).toBeUndefined();
    } finally {
      await harness.shutdown();
    }
  });

  it('records the budget stop when an invalid outcome report is cut off by the Harness step ceiling', async () => {
    const { hook, session, harness } = await budgetedIterationHook(1000);
    try {
      for (let iteration = 1; iteration < 1000; iteration++) {
        await hook({ iteration, text: '', toolResults: [toolResult], isFinal: false, finishReason: 'tool-calls' });
      }
      // The loop reports a rejected report call under the report tool's name.
      await hook({
        iteration: 1000,
        text: '',
        toolResults: [{ id: 'report', name: 'report_subagent_outcome', result: {}, error: new Error('invalid input') }],
        isFinal: true,
        finishReason: 'tool-calls',
      });
      expect(session._internalSubagentBudgetExhaustion()).toEqual({ budget: 'steps', limit: 1000, used: 1000 });
    } finally {
      await harness.shutdown();
    }
  });

  it('records the budget stop when the Harness step ceiling ends a child that wanted to continue', async () => {
    const { hook, session, harness } = await budgetedIterationHook(1000);
    try {
      for (let iteration = 1; iteration < 1000; iteration++) {
        await hook({ iteration, text: '', toolResults: [toolResult], isFinal: false, finishReason: 'tool-calls' });
      }
      expect(session._internalSubagentBudgetExhaustion()).toBeUndefined();
      await hook({
        iteration: 1000,
        text: 'Partial.',
        toolResults: [toolResult],
        isFinal: true,
        finishReason: 'tool-calls',
      });
      expect(session._internalSubagentBudgetExhaustion()).toEqual({ budget: 'steps', limit: 1000, used: 1000 });
    } finally {
      await harness.shutdown();
    }
  });
});
