import { stepCountIs } from '@internal/ai-sdk-v5';
import { expect, it, vi } from 'vitest';
import { z } from 'zod/v4';
import { createTool } from '../../../../tools';
import { describeForAllEngines, runLoopScenario, useLoopScenarioAimock } from '../aimock-scenario';
import { isDurableEngineVariant } from '../types';

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

  it('never classifies a deferred background dispatch placeholder', async () => {
    const classified: unknown[] = [];
    let release!: () => void;
    const released = new Promise<void>(resolve => {
      release = resolve;
    });
    const analysisTool = createTool({
      id: 'run_deferred_analysis',
      description: 'Run an analysis in the background.',
      inputSchema: z.object({}),
      outputSchema: z.object({ success: z.boolean(), stderr: z.string() }),
      background: { enabled: true, timeoutMs: 5000 },
      resultStatus: output => {
        classified.push(output);
        return output.success ? 'success' : 'error';
      },
      execute: async () => {
        // Held until the dispatch turn has streamed its placeholder.
        await released;
        return { success: false, stderr: 'NameError: total' };
      },
    });

    const { chunks } = await runLoopScenario({
      engine,
      llm: getMock(),
      prompt: 'Run the deferred analysis.',
      tools: { run_deferred_analysis: analysisTool },
      agentBackgroundTasks: { tools: { run_deferred_analysis: true } },
      backgroundTasks: { enabled: true },
      stopWhen: stepCountIs(3),
      collectChunks: true,
      fixtures: llm => {
        llm.on(
          { endpoint: 'chat', sequenceIndex: 0 },
          { toolCalls: [{ id: 'call_deferred_analysis', name: 'run_deferred_analysis', arguments: {} }] },
        );
        llm.on({ endpoint: 'chat', sequenceIndex: 1 }, { content: 'Deferred analysis dispatched.' });
      },
    });
    release();
    // The released task completes and is classified on its real output.
    await vi.waitFor(() => expect(classified.length).toBeGreaterThan(0), { timeout: 5_000 });

    const placeholders = (chunks ?? []).filter(
      chunk =>
        chunk.type === 'tool-result' &&
        (chunk.payload as { providerMetadata?: { mastra?: { backgroundTask?: { status?: unknown } } } })
          .providerMetadata?.mastra?.backgroundTask?.status === 'running',
    );
    expect(chunks?.some(chunk => chunk.type === 'background-task-started')).toBe(true);
    // The in-process engines stream the dispatch placeholder as a tool-result.
    if (!isDurableEngineVariant(engine)) expect(placeholders.length).toBeGreaterThan(0);
    for (const chunk of placeholders) {
      expect(
        (chunk.payload as { providerMetadata?: { mastra?: { resultStatus?: unknown } } }).providerMetadata?.mastra
          ?.resultStatus,
      ).toBeUndefined();
    }
    // The classifier only ever saw real tool outputs, never the placeholder text.
    expect(classified.every(output => typeof output === 'object' && output !== null)).toBe(true);
  });

  it('classifies a tool the model called by its id rather than its registration key', async () => {
    const analysisTool = createTool({
      id: 'run_keyed_analysis',
      description: 'Run an analysis.',
      inputSchema: z.object({}),
      outputSchema: z.object({ success: z.boolean(), stderr: z.string() }),
      resultStatus: output => (output.success ? 'success' : 'error'),
      execute: async () => ({ success: false, stderr: 'NameError: total' }),
    });

    const { chunks } = await runLoopScenario({
      llm: getMock(),
      prompt: 'Run the keyed analysis.',
      tools: { analysisUnderAnotherKey: analysisTool },
      stopWhen: stepCountIs(3),
      engine,
      collectChunks: true,
      fixtures: llm => {
        llm.on(
          { endpoint: 'chat', hasToolResult: false },
          { toolCalls: [{ id: 'call_keyed', name: 'run_keyed_analysis', arguments: {} }] },
        );
        llm.on({ endpoint: 'chat', toolCallId: 'call_keyed', hasToolResult: true }, { content: 'Handled.' });
      },
    });

    const result = chunks?.find(chunk => chunk.type === 'tool-result')?.payload as
      | { result?: { success?: unknown }; providerMetadata?: { mastra?: { resultStatus?: unknown } } }
      | undefined;
    expect(result?.result?.success).toBe(false);
    expect(result?.providerMetadata?.mastra?.resultStatus).toBe('error');
  });

  async function runProcessedAnalysis(rawSuccess: boolean, processedSuccess: boolean) {
    const analysisTool = createTool({
      id: 'run_processed_analysis',
      description: 'Run an analysis.',
      inputSchema: z.object({}),
      outputSchema: z.object({ success: z.boolean() }),
      resultStatus: output => (output.success ? 'success' : 'error'),
      execute: async () => ({ success: rawSuccess }),
    });
    const replacer = {
      id: 'replace-analysis-result',
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
          toolInvocation: { state: 'result', toolCallId, toolName, args, result: { success: processedSuccess } },
        });
      },
    };
    const { chunks } = await runLoopScenario({
      llm: getMock(),
      prompt: 'Run the processed analysis.',
      tools: { run_processed_analysis: analysisTool },
      outputProcessors: [replacer as never],
      stopWhen: stepCountIs(3),
      engine,
      collectChunks: true,
      fixtures: llm => {
        llm.on(
          { endpoint: 'chat', hasToolResult: false },
          { toolCalls: [{ id: 'call_processed', name: 'run_processed_analysis', arguments: {} }] },
        );
        llm.on({ endpoint: 'chat', toolCallId: 'call_processed', hasToolResult: true }, { content: 'Handled.' });
      },
    });
    return (chunks ?? []).filter(chunk => chunk.type === 'tool-result').at(-1)?.payload as
      | { result?: { success?: unknown }; providerMetadata?: { mastra?: { resultStatus?: unknown } } }
      | undefined;
  }

  it('classifies the result processToolResult recorded, not the raw return', async () => {
    const failed = await runProcessedAnalysis(true, false);
    expect(failed?.result?.success).toBe(false);
    expect(failed?.providerMetadata?.mastra?.resultStatus).toBe('error');

    const recovered = await runProcessedAnalysis(false, true);
    expect(recovered?.result?.success).toBe(true);
    expect(recovered?.providerMetadata?.mastra?.resultStatus).toBeUndefined();
  });

  it('classifies a provider-defined tool found by its provider name under another key', async () => {
    const providerTool = {
      type: 'provider-defined' as const,
      id: 'test.bash' as const,
      name: 'bash',
      args: {},
      description: 'Run a command locally.',
      inputSchema: z.object({ command: z.string() }),
      execute: async () => ({ success: false, stderr: 'boom' }),
      resultStatus: (output: { success: boolean }) => (output.success ? 'success' : 'error'),
    };

    const { chunks } = await runLoopScenario({
      llm: getMock(),
      prompt: 'Run the command.',
      tools: { shellUnderAnotherKey: providerTool as never },
      stopWhen: stepCountIs(3),
      engine,
      collectChunks: true,
      fixtures: llm => {
        llm.on(
          { endpoint: 'chat', hasToolResult: false },
          { toolCalls: [{ id: 'call_bash', name: 'bash', arguments: { command: 'ls' } }] },
        );
        llm.on({ endpoint: 'chat', toolCallId: 'call_bash', hasToolResult: true }, { content: 'Handled.' });
      },
    });

    const result = chunks?.find(chunk => chunk.type === 'tool-result')?.payload as
      | { result?: { success?: unknown }; providerMetadata?: { mastra?: { resultStatus?: unknown } } }
      | undefined;
    expect(result?.result?.success).toBe(false);
    expect(result?.providerMetadata?.mastra?.resultStatus).toBe('error');
  });
});
