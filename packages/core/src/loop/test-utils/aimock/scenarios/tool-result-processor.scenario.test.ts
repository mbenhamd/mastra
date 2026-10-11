/**
 * AIMock Scenario: processToolResult-only output processor
 *
 * An output processor that implements only `processToolResult` (no
 * `processOutputStream`) must still run on every engine. The tool-result chunk
 * streaming clients see and the tool message the next model turn receives both
 * carry the processed value, never the raw tool return.
 */

import { stepCountIs } from '@internal/ai-sdk-v5';
import { it, expect } from 'vitest';
import { z } from 'zod/v4';
import { createTool } from '../../../../tools';
import { describeForAllEngines, runLoopScenario, useLoopScenarioAimock } from '../aimock-scenario';

describeForAllEngines('AIMock loop scenario: processToolResult-only processor', engine => {
  const getMock = useLoopScenarioAimock();

  it('replaces the tool result for the stream and the next model turn', async () => {
    const secretTool = createTool({
      id: 'read_secret',
      description: 'Read a secret value.',
      inputSchema: z.object({}),
      execute: async () => 'RAW_SECRET_VALUE',
    });
    const redactor = {
      id: 'tool-result-redactor',
      processToolResult: async ({
        messageList,
        toolCallId,
        toolName,
        args,
      }: {
        messageList: { updateToolInvocation: (part: unknown) => unknown };
        toolCallId: string;
        toolName: string;
        args: unknown;
      }) => {
        messageList.updateToolInvocation({
          type: 'tool-invocation',
          toolInvocation: { state: 'result', toolCallId, toolName, args, result: 'REDACTED_VALUE' },
        });
      },
    };

    const { chunks, requests } = await runLoopScenario({
      llm: getMock(),
      prompt: 'Read the secret.',
      tools: { read_secret: secretTool },
      outputProcessors: [redactor as never],
      stopWhen: stepCountIs(3),
      engine,
      collectChunks: true,
      fixtures: llm => {
        llm.on(
          { endpoint: 'chat', hasToolResult: false },
          { toolCalls: [{ id: 'call_secret', name: 'read_secret', arguments: {} }] },
        );
        llm.on({ endpoint: 'chat', toolCallId: 'call_secret', hasToolResult: true }, { content: 'Handled.' });
      },
    });

    const toolResults = (chunks ?? [])
      .filter(chunk => chunk.type === 'tool-result')
      .map(chunk => (chunk.payload as { result?: unknown }).result);
    expect(toolResults).toEqual(['REDACTED_VALUE']);

    expect(requests).toHaveLength(2);
    const toolMessage = (requests[1]?.body?.messages ?? []).find(
      message => (message as { role?: string }).role === 'tool',
    ) as { tool_call_id?: string; content?: unknown } | undefined;
    expect(toolMessage?.tool_call_id).toBe('call_secret');
    expect(JSON.stringify(toolMessage?.content)).toContain('REDACTED_VALUE');
    expect(JSON.stringify(toolMessage?.content)).not.toContain('RAW_SECRET_VALUE');
  });
});
