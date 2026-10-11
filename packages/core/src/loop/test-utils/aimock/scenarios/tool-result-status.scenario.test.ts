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

  it('classifies a background completion and never its dispatch placeholder', async () => {
    const analysisTool = createTool({
      id: 'run_background_analysis',
      description: 'Run an analysis in the background.',
      inputSchema: z.object({}),
      outputSchema: z.object({ success: z.boolean(), stderr: z.string() }),
      background: { enabled: true, timeoutMs: 5000 },
      resultStatus: output => (output.success ? 'success' : 'error'),
      execute: async () => {
        await new Promise(resolve => setTimeout(resolve, 10));
        return { success: false, stderr: 'NameError: total' };
      },
    });

    const { chunks } = await runLoopScenario({
      engine,
      llm: getMock(),
      prompt: 'Run the background analysis.',
      tools: { run_background_analysis: analysisTool },
      agentBackgroundTasks: { tools: { run_background_analysis: true } },
      backgroundTasks: { enabled: true },
      stopWhen: stepCountIs(3),
      collectChunks: true,
      fixtures: llm => {
        llm.on(
          { endpoint: 'chat', sequenceIndex: 0 },
          {
            toolCalls: [
              {
                id: 'call_background_analysis',
                name: 'run_background_analysis',
                // The awaited disposition blocks the turn on the authoritative result.
                arguments: { _background: { disposition: 'awaited' } },
              },
            ],
          },
        );
        llm.on({ endpoint: 'chat', sequenceIndex: 1 }, { content: 'Background analysis failed.' });
      },
    });

    const results = (chunks ?? [])
      .filter(chunk => chunk.type === 'tool-result')
      .map(
        chunk =>
          chunk.payload as {
            result?: unknown;
            providerMetadata?: { mastra?: { resultStatus?: unknown; backgroundTask?: { status?: unknown } } };
          },
      );
    const completions = results.filter(
      payload => typeof payload.result === 'object' && (payload.result as { success?: unknown }).success === false,
    );
    expect(completions.length).toBeGreaterThan(0);
    for (const payload of completions) expect(payload.providerMetadata?.mastra?.resultStatus).toBe('error');
    for (const payload of results.filter(p => p.providerMetadata?.mastra?.backgroundTask?.status === 'running')) {
      expect(payload.providerMetadata?.mastra?.resultStatus).toBeUndefined();
    }
  });
});
