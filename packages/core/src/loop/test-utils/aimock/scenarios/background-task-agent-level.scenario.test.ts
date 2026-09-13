import { createOpenAI } from '@ai-sdk/openai-v5';
import { stepCountIs } from '@internal/ai-sdk-v5';
import { it, expect } from 'vitest';
import { z } from 'zod';
import { Agent } from '../../../../agent';
import type { ChunkType } from '../../../../stream/types';
import { createTool } from '../../../../tools';
import { runLoopScenario, useLoopScenarioAimock, describeForAllEngines } from '../aimock-scenario';
import { SCENARIO_MODEL_ID } from '../types';

/**
 * Tests agent-level background task opt-in and resolution order.
 *
 * Per the docs, the resolved background config is computed in this priority:
 *   1. Agent-level `backgroundTasks.tools` entry for the tool.
 *   2. Tool-level `backgroundTasks` config.
 *   3. LLM `_background.enabled` override (only when opted in at 1 or 2).
 *   4. Manager defaults.
 *
 * This scenario pins the regression class where agent-level opt-in fails to
 * elevate a tool to background dispatch when the tool itself has not opted in.
 */
describeForAllEngines('background-task-agent-level scenario', engine => {
  const getMock = useLoopScenarioAimock();

  it('opts in a non-background tool at agent level and dispatches it in the background', async () => {
    const onChunks: ChunkType[] = [];

    // Tool WITHOUT tool-level `background: { enabled: true }` — agent-level must
    // elevate it.
    const plainTool = createTool({
      id: 'plain-work',
      description: 'Performs work (no tool-level opt-in)',
      inputSchema: z.object({ topic: z.string() }),
      outputSchema: z.object({ summary: z.string() }),
      execute: async ({ topic }) => {
        await new Promise(resolve => setTimeout(resolve, 10));
        return { summary: `Summary of ${topic}` };
      },
    });

    const { chunks } = await runLoopScenario({
      engine,
      llm: getMock(),
      prompt: 'Research quantum',
      tools: { 'plain-work': plainTool },
      agentBackgroundTasks: { tools: { 'plain-work': true } },
      stopWhen: stepCountIs(3),
      backgroundTasks: { enabled: true },
      collectChunks: true,
      ...(engine !== 'durable'
        ? {
            onChunk: (chunk: ChunkType) => {
              onChunks.push(chunk);
            },
          }
        : {}),
      fixtures: llm => {
        llm.on(
          { endpoint: 'chat', sequenceIndex: 0 },
          { toolCalls: [{ id: 'call_plain', name: 'plain-work', arguments: { topic: 'quantum' } }] },
        );
        llm.on({ endpoint: 'chat', sequenceIndex: 1 }, { content: 'Agent-level background dispatch worked.' });
      },
    });

    // Agent-level opt-in elevated the tool: background-task-started chunk emitted
    // even though the tool itself did not declare `background: { enabled: true }`.
    const startedChunk = chunks?.find(c => c.type === 'background-task-started');
    expect(startedChunk).toBeDefined();
    expect(startedChunk?.payload).toMatchObject({
      toolName: 'plain-work',
    });

    if (engine !== 'durable') {
      const onChunkStarted = onChunks.find(c => c.type === 'background-task-started');
      expect(onChunkStarted).toBeDefined();
      expect(onChunkStarted?.payload).toMatchObject({
        toolName: 'plain-work',
        toolCallId: 'call_plain',
      });
    }
  });
});

describeForAllEngines(
  'background-task-agent-level nested awaited scenario',
  engine => {
    const getMock = useLoopScenarioAimock();

    it.each([
      { globalConcurrency: 1, expectedLeafTasks: 0, mode: 'falls back the nested task' },
      { globalConcurrency: 2, expectedLeafTasks: 1, mode: 'dispatches the nested task when capacity is available' },
    ])('$mode', async ({ globalConcurrency, expectedLeafTasks }) => {
      const mock = getMock();
      const openai = createOpenAI({
        apiKey: 'aimock-test-key',
        baseURL: `${mock.url.replace(/\/+$/, '')}/v1`,
      });
      let leafExecutions = 0;

      const leafTool = createTool({
        id: 'nested-leaf',
        description: 'Performs the leaf operation',
        inputSchema: z.object({ value: z.string() }),
        outputSchema: z.object({ value: z.string() }),
        background: { enabled: true },
        execute: async ({ value }) => {
          leafExecutions++;
          return { value: `leaf:${value}` };
        },
      });
      const child = new Agent({
        id: 'nested-child',
        name: 'Nested Child',
        description: 'Runs the leaf operation',
        instructions: 'Use the nested-leaf tool when asked.',
        model: openai(SCENARIO_MODEL_ID),
        tools: { 'nested-leaf': leafTool },
        backgroundTasks: { tools: { 'nested-leaf': true } },
      });

      const { output, mastra } = await runLoopScenario({
        engine,
        llm: mock,
        prompt: 'Ask the child to use the leaf operation.',
        agents: { 'nested-child': child },
        agentBackgroundTasks: { tools: { 'agent-nested-child': true } },
        backgroundTasks: {
          enabled: true,
          globalConcurrency,
          perAgentConcurrency: 1,
          backpressure: 'queue',
        },
        stopWhen: stepCountIs(5),
        fixtures: llm => {
          llm.on(
            { endpoint: 'chat', hasToolResult: false, userMessage: /Ask the child/i },
            {
              toolCalls: [
                {
                  id: 'call-nested-child',
                  name: 'agent-nested-child',
                  arguments: { prompt: 'Use the leaf operation.', _background: { disposition: 'awaited' } },
                },
              ],
            },
          );
          llm.on(
            { endpoint: 'chat', hasToolResult: false, userMessage: /Use the leaf operation/i },
            {
              toolCalls: [
                {
                  id: 'call-nested-leaf',
                  name: 'nested-leaf',
                  arguments: { value: 'work', _background: { disposition: 'awaited' } },
                },
              ],
            },
          );
          llm.on(
            { endpoint: 'chat', toolCallId: 'call-nested-leaf', hasToolResult: true },
            { content: 'Leaf operation complete.' },
          );
          llm.on(
            { endpoint: 'chat', toolCallId: 'call-nested-child', hasToolResult: true },
            { content: 'Parent received the leaf result.' },
          );
        },
      });

      expect(await output.text).toContain('Parent received the leaf result.');
      expect(leafExecutions).toBe(1);
      const tasks = (await mastra?.backgroundTaskManager?.listTasks({}))?.tasks ?? [];
      const leafTasks = tasks.filter(task => task.toolName === 'nested-leaf');
      expect(leafTasks).toHaveLength(expectedLeafTasks);
      if (expectedLeafTasks === 1) {
        expect(leafTasks[0]?.status).toBe('completed');
      }
    });
  },
  { skip: ['durable'] },
);
