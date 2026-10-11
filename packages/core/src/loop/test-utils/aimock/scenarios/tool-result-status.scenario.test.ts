import { stepCountIs } from '@internal/ai-sdk-v5';
import { expect, it } from 'vitest';
import { z } from 'zod/v4';
import { createTool } from '../../../../tools';
import { describeForAllEngines, runLoopScenario, useLoopScenarioAimock } from '../aimock-scenario';

/**
 * A tool can return a failure as data so the model reads and fixes it.
 * `resultStatus` classifies that output for framework tool-call receipts: the
 * emitted tool-result chunk carries `providerMetadata.mastra.resultStatus`,
 * and the model still receives the returned output unchanged.
 */
describeForAllEngines('AIMock loop scenario: tool result status', engine => {
  const getMock = useLoopScenarioAimock();

  async function runAnalysis(succeeds: boolean) {
    const analysisTool = createTool({
      id: 'run_analysis',
      description: 'Run an analysis.',
      inputSchema: z.object({}),
      outputSchema: z.object({ success: z.boolean(), stderr: z.string() }),
      resultStatus: output => (output.success ? 'success' : 'error'),
      execute: async () => ({ success: succeeds, stderr: succeeds ? '' : 'NameError: total' }),
    });
    return runLoopScenario({
      llm: getMock(),
      prompt: 'Run the analysis.',
      tools: { run_analysis: analysisTool },
      stopWhen: stepCountIs(3),
      engine,
      collectChunks: true,
      fixtures: llm => {
        llm.on(
          { endpoint: 'chat', hasToolResult: false },
          { toolCalls: [{ id: 'call_analysis', name: 'run_analysis', arguments: {} }] },
        );
        llm.on(
          { endpoint: 'chat', toolCallId: 'call_analysis', hasToolResult: true },
          { content: 'Analysis handled.' },
        );
      },
    });
  }

  const toolResultStatus = (chunks: Array<{ type: string; payload?: unknown }> | undefined) =>
    (
      chunks?.find(chunk => chunk.type === 'tool-result')?.payload as
        | { providerMetadata?: { mastra?: { resultStatus?: unknown } } }
        | undefined
    )?.providerMetadata?.mastra?.resultStatus;

  it('marks a returned failure as an error result while the model still reads it', async () => {
    const { chunks, requests } = await runAnalysis(false);

    expect(toolResultStatus(chunks as never)).toBe('error');
    expect(JSON.stringify(requests[1]?.body?.messages ?? [])).toContain('NameError: total');
  });

  it('leaves a successful result unmarked', async () => {
    const { chunks } = await runAnalysis(true);

    expect(chunks?.some(chunk => chunk.type === 'tool-result')).toBe(true);
    expect(toolResultStatus(chunks as never)).toBeUndefined();
  });
});
