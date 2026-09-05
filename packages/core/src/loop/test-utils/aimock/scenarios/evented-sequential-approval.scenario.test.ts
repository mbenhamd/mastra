import { createOpenAI } from '@ai-sdk/openai-v5';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { z } from 'zod/v4';
import { Agent } from '../../../../agent';
import { createTool } from '../../../../tools';
import { createSharedAgent, useLoopScenarioAimock } from '../aimock-scenario';
import { SCENARIO_MODEL_ID } from '../types';

describe('AIMock loop scenario: evented sequential delegation approvals', () => {
  const getMock = useLoopScenarioAimock();
  afterEach(() => vi.unstubAllEnvs());

  it('resumes both suspended delegations once and preserves both results in the next model turn', async () => {
    const llm = getMock();
    const executions: string[] = [];
    vi.stubEnv('MASTRA_EVENTED_EXECUTION', 'true');
    const openai = createOpenAI({ apiKey: 'aimock-test-key', baseURL: `${llm.url.replace(/\/+$/, '')}/v1` });
    const worker = new Agent({
      id: 'approval-worker',
      name: 'Approval worker',
      instructions: 'Process the requested item, then report its result.',
      model: openai.chat(SCENARIO_MODEL_ID),
      tools: {
        process_item: createTool({
          id: 'process_item',
          description: 'Process one item after approval.',
          inputSchema: z.object({ item: z.string() }),
          outputSchema: z.object({ processed: z.string() }),
          requireApproval: true,
          execute: async ({ item }) => {
            executions.push(item);
            return { processed: item };
          },
        }),
      },
    });

    llm.on(
      { endpoint: 'chat', userMessage: 'Delegate two approvals.', hasToolResult: false },
      {
        toolCalls: ['alpha', 'beta'].map(item => ({
          id: `delegate_${item}`,
          name: 'agent-worker',
          arguments: { prompt: `Process ${item}.`, maxSteps: 3 },
        })),
      },
    );
    for (const item of ['alpha', 'beta']) {
      llm.on(
        { endpoint: 'chat', userMessage: `Process ${item}.`, hasToolResult: false },
        { toolCalls: [{ id: `process_${item}`, name: 'process_item', arguments: { item } }] },
      );
      llm.on(
        { endpoint: 'chat', toolCallId: `process_${item}`, hasToolResult: true },
        { content: `Processed ${item}.` },
      );
    }
    llm.on(
      { endpoint: 'chat', toolCallId: 'delegate_beta', hasToolResult: true },
      { content: 'Both items processed.' },
    );

    const { agent, mastra } = await createSharedAgent(llm, {
      engine: 'evented',
      agents: { worker },
      model: openai.chat(SCENARIO_MODEL_ID),
    });
    try {
      let output = await agent.stream('Delegate two approvals.', { maxSteps: 5 });
      const runId = output.runId;
      const approvals: string[] = [];
      const errors: unknown[] = [];
      for await (const chunk of output.fullStream) {
        if (chunk.type === 'tool-call-approval') approvals.push(chunk.payload.toolCallId);
        if (chunk.type === 'error' || chunk.type === 'tool-error') errors.push(chunk);
      }
      expect(errors).toEqual([]);
      expect(approvals.slice().sort()).toEqual(['delegate_alpha', 'delegate_beta']);
      expect(executions).toEqual([]);

      for (const item of ['alpha', 'beta']) {
        output = await agent.approveToolCall({ runId, toolCallId: `delegate_${item}` });
        for await (const chunk of output.fullStream) {
          if (chunk.type === 'error' || chunk.type === 'tool-error') errors.push(chunk);
        }
        expect(errors).toEqual([]);
        if (item === 'alpha') {
          expect(executions).toEqual(['alpha']);
          const { runs } = await agent.listSuspendedRuns();
          expect(runs.flatMap(run => run.toolCalls.map(call => call.toolCallId))).toEqual(['delegate_beta']);
        }
      }

      expect(executions).toEqual(['alpha', 'beta']);
      expect(await output.text).toBe('Both items processed.');
      const toolMessages = llm
        .getRequests()
        .at(-1)
        ?.body.messages?.filter(message => message.role === 'tool');
      expect(toolMessages).toEqual([
        expect.objectContaining({
          tool_call_id: 'delegate_alpha',
          content: expect.stringContaining('Processed alpha.'),
        }),
        expect.objectContaining({ tool_call_id: 'delegate_beta', content: expect.stringContaining('Processed beta.') }),
      ]);
    } finally {
      await mastra.shutdown();
    }
  });
});
