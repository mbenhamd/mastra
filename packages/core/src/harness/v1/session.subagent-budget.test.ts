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
import { InMemoryStore } from '../../storage';
import { createTool } from '../../tools';

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
    } finally {
      await harness.shutdown();
    }
  });
});
