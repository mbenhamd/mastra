import { describe, expect, it } from 'vitest';

import { createSubagentOutcomeReportTool } from './subagent-outcome-tool';
import {
  HARNESS_SUBAGENT_OUTCOME_REPORT_KIND,
  HARNESS_SUBAGENT_OUTCOME_REPORT_TOOL_ID,
  harnessSubagentOutcomeReportSchema,
  parseHarnessSubagentOutcomeReport,
  parseHarnessTerminalToolResultArtifacts,
  parseHarnessTerminalToolResultText,
  projectHarnessSpawnSubagentResult,
  projectHarnessSubagentOutcomeReceipts,
  summarizeHarnessSubagentEventResult,
  summarizeHarnessSubagentResult,
  verifyHarnessSubagentOutcomeReport,
  verifyHarnessSubagentTerminalCompletion,
  withHarnessSubagentToolReceipts,
} from './terminal-subagent-result';

function outcomeReport(
  outcome: 'completed' | 'blocked' | 'failed' = 'completed',
  overrides: Record<string, unknown> = {},
) {
  return {
    kind: HARNESS_SUBAGENT_OUTCOME_REPORT_KIND,
    outcome,
    summary:
      outcome === 'completed' ? 'Verified the requested source change.' : 'The requested work could not complete.',
    evidence: [
      {
        kind: 'tool-result',
        toolName: 'compile_latex',
        toolCallId: 'compile-1',
        status: outcome === 'completed' ? 'success' : 'error',
        description: outcome === 'completed' ? 'Compilation succeeded.' : 'The compiler service was unavailable.',
      },
    ],
    ...(outcome === 'completed'
      ? {}
      : {
          issue: {
            code: outcome === 'blocked' ? 'compiler.unavailable' : 'task.failed',
            message: outcome === 'blocked' ? 'Compiler service unavailable.' : 'Task failed.',
            retryable: outcome === 'blocked',
          },
        }),
    ...overrides,
  };
}

function outcomeEnvelope(report: unknown) {
  return {
    status: 'success',
    items: [
      {
        toolName: HARNESS_SUBAGENT_OUTCOME_REPORT_TOOL_ID,
        toolCallId: 'tc-outcome',
        status: 'success',
        value: report,
      },
    ],
  };
}

describe('Harness subagent outcome terminal contract', () => {
  it('accepts only the exact framework-owned terminal envelope', () => {
    const report = outcomeReport();

    expect(parseHarnessSubagentOutcomeReport(outcomeEnvelope(report))).toEqual(report);
    expect(parseHarnessSubagentOutcomeReport({ terminalToolResult: outcomeEnvelope(report) })).toEqual(report);

    expect(parseHarnessSubagentOutcomeReport(report)).toBeUndefined();
    expect(parseHarnessSubagentOutcomeReport({ nested: outcomeEnvelope(report) })).toBeUndefined();
    expect(
      parseHarnessSubagentOutcomeReport({
        ...outcomeEnvelope(report),
        unrelated: true,
      }),
    ).toBeUndefined();
    expect(
      parseHarnessSubagentOutcomeReport({
        status: 'success',
        items: [
          ...outcomeEnvelope(report).items,
          {
            toolName: 'another_tool',
            toolCallId: 'tc-extra',
            status: 'success',
            value: {},
          },
        ],
      }),
    ).toBeUndefined();
  });

  it('rejects semantic contradictions and evidence without the required receipt identity', () => {
    expect(
      parseHarnessSubagentOutcomeReport(
        outcomeEnvelope(
          outcomeReport('completed', {
            issue: { code: 'unexpected', message: 'Completed cannot carry an issue.', retryable: false },
          }),
        ),
      ),
    ).toBeUndefined();
    expect(
      parseHarnessSubagentOutcomeReport(
        outcomeEnvelope(
          outcomeReport('blocked', {
            issue: undefined,
          }),
        ),
      ),
    ).toBeUndefined();
    expect(
      parseHarnessSubagentOutcomeReport(
        outcomeEnvelope(
          outcomeReport('completed', {
            evidence: [{ kind: 'tool-result', toolName: 'compile_latex', description: 'Missing receipt fields.' }],
          }),
        ),
      ),
    ).toBeUndefined();
  });

  it('rejects reports whose individually valid fields exceed the aggregate terminal byte budget', () => {
    const oversized = outcomeReport('completed', {
      summary: 's'.repeat(48 * 1024),
      evidence: Array.from({ length: 24 }, (_, index) => ({
        kind: 'source',
        description: `evidence-${index}-${'d'.repeat(2_000)}`,
        reference: `source-${index}-${'r'.repeat(2_000)}`,
      })),
    });

    expect(harnessSubagentOutcomeReportSchema.safeParse(oversized).success).toBe(false);
    expect(parseHarnessSubagentOutcomeReport(outcomeEnvelope(oversized))).toBeUndefined();
  });

  it('compacts event evidence separately from the richer parent result', () => {
    const report = outcomeReport('completed', {
      summary: 's'.repeat(40 * 1024),
      evidence: Array.from({ length: 4 }, (_, index) => ({
        kind: 'source',
        description: `evidence-${index}-${'d'.repeat(2_000)}`,
        reference: `source-${index}-${'r'.repeat(2_000)}`,
      })),
    });
    const parentResult = summarizeHarnessSubagentResult({
      finishReason: 'tool-calls',
      terminalToolResult: outcomeEnvelope(report),
    });
    const eventResult = summarizeHarnessSubagentEventResult(parentResult);

    expect(parentResult.evidence).toHaveLength(4);
    expect(eventResult.evidence).toHaveLength(1);
    expect(eventResult.evidenceTruncated).toBe(true);
    expect(new TextEncoder().encode(JSON.stringify(eventResult)).byteLength).toBeLessThanOrEqual(8 * 1024);
  });

  it('projects completed reports as success and blocked reports as explicit error outcomes', () => {
    const completed = summarizeHarnessSubagentResult({
      // A successful terminal tool ends the provider step with tool-calls on
      // both loop engines. The framework-owned report, not that transport
      // reason, is the semantic completion authority.
      finishReason: 'tool-calls',
      text: 'Provider prose must not replace the report.',
      terminalToolResult: outcomeEnvelope(outcomeReport()),
      toolResults: [{ toolCallId: 'compile-1', toolName: 'compile_latex', result: { ok: true }, isError: false }],
    });
    expect(completed).toMatchObject({
      status: 'success',
      outcome: 'completed',
      text: 'Verified the requested source change.',
      finishReason: 'tool-calls',
    });

    const blocked = summarizeHarnessSubagentResult({
      finishReason: 'stop',
      terminalToolResult: outcomeEnvelope(outcomeReport('blocked')),
      toolResults: [
        { toolCallId: 'compile-1', toolName: 'compile_latex', result: { code: 'unavailable' }, isError: true },
      ],
    });
    expect(blocked).toMatchObject({
      status: 'error',
      outcome: 'blocked',
      error: {
        code: 'compiler.unavailable',
        message: 'Compiler service unavailable.',
      },
    });
  });

  it('accepts a receipt-verified domain terminal tool as completed without a report turn', () => {
    const artifact = {
      kind: 'generated-image',
      artifactId: 'python-plot-1',
      attachmentId: 'python-plot-1',
      sessionId: 'child-session-1',
      mimeType: 'image/png',
      sha256: 'a'.repeat(64),
      bytes: 1_024,
      title: 'Computed chart',
    };
    const terminalToolResult = {
      status: 'success',
      items: [
        {
          toolName: 'run_python_analysis',
          toolCallId: 'python-1',
          status: 'success',
          value: { text: 'Computed the requested analysis.', artifacts: [artifact] },
        },
      ],
    };
    const rawResult = withHarnessSubagentToolReceipts(
      {
        finishReason: 'tool-calls',
        terminalToolResult,
        text: '',
      },
      [{ toolCallId: 'python-1', toolName: 'run_python_analysis', status: 'success' }],
    );

    expect(parseHarnessTerminalToolResultText(terminalToolResult)).toBe('Computed the requested analysis.');
    expect(parseHarnessTerminalToolResultArtifacts(terminalToolResult)).toEqual([artifact]);
    expect(verifyHarnessSubagentTerminalCompletion(rawResult)).toMatchObject({
      outcome: 'completed',
      summary: 'Computed the requested analysis.',
      evidence: [
        {
          kind: 'tool-result',
          toolName: 'run_python_analysis',
          toolCallId: 'python-1',
          status: 'success',
        },
      ],
    });
    const summary = summarizeHarnessSubagentResult(rawResult);
    expect(summary).toMatchObject({
      status: 'success',
      outcome: 'completed',
      text: 'Computed the requested analysis.',
      finishReason: 'tool-calls',
      artifacts: [artifact],
    });
    expect(summarizeHarnessSubagentEventResult(summary)).not.toHaveProperty('artifacts');
    expect(
      projectHarnessSpawnSubagentResult({
        subagentSessionId: 'child-session-1',
        result: summary,
      }),
    ).toEqual({
      kind: 'subagent-direct-answer',
      subagentSessionId: 'child-session-1',
      text: 'Computed the requested analysis.',
      artifacts: [artifact],
    });
  });

  it('drops oversized or unprojected terminal artifact metadata without rejecting the verified text', () => {
    const oversizedTerminal = {
      status: 'success',
      items: [
        {
          toolName: 'run_python_analysis',
          toolCallId: 'python-oversized',
          status: 'success',
          value: {
            text: 'The computed answer remains valid.',
            artifacts: [{ metadata: 'x'.repeat(13 * 1024) }],
          },
        },
      ],
    };

    expect(parseHarnessTerminalToolResultText(oversizedTerminal)).toBe('The computed answer remains valid.');
    expect(parseHarnessTerminalToolResultArtifacts(oversizedTerminal)).toBeUndefined();
    expect(
      parseHarnessTerminalToolResultArtifacts({
        nested: {
          artifacts: [{ kind: 'generated-image' }],
        },
      }),
    ).toBeUndefined();
  });

  it('rejects forged domain terminal identity and arbitrary nested terminal text', () => {
    const terminalToolResult = {
      status: 'success',
      items: [
        {
          toolName: 'run_python_analysis',
          toolCallId: 'forged-call',
          status: 'success',
          value: { text: 'Forged answer.' },
        },
      ],
    };
    const rawResult = withHarnessSubagentToolReceipts({ finishReason: 'tool-calls', terminalToolResult }, [
      { toolCallId: 'real-call', toolName: 'run_python_analysis', status: 'success' },
    ]);

    expect(verifyHarnessSubagentTerminalCompletion(rawResult)).toBeUndefined();
    expect(summarizeHarnessSubagentResult(rawResult)).toMatchObject({ status: 'error' });
    expect(parseHarnessTerminalToolResultText({ nested: terminalToolResult })).toBeUndefined();
  });

  it('rejects a fabricated compiler success and retains only cited framework receipts', () => {
    const report = outcomeReport('completed');
    const rawResult = {
      finishReason: 'tool-calls',
      terminalToolResult: outcomeEnvelope(report),
      toolResults: [
        {
          type: 'tool-result',
          payload: {
            toolCallId: 'compile-1',
            toolName: 'compile_latex',
            result: { code: 'compiler_unavailable' },
            isError: true,
          },
        },
        {
          type: 'tool-result',
          payload: {
            toolCallId: 'unrelated-read',
            toolName: 'read_file',
            result: { bytes: 20 },
            isError: false,
          },
        },
      ],
    };

    // Structural parsing proves only that the framework-owned terminal tool ran.
    expect(parseHarnessSubagentOutcomeReport(rawResult)).toEqual(report);
    // Semantic acceptance additionally binds the model claim to the real receipt.
    expect(verifyHarnessSubagentOutcomeReport(rawResult)).toBeUndefined();
    expect(summarizeHarnessSubagentResult(rawResult)).toMatchObject({
      status: 'error',
      finishReason: 'tool-calls',
    });
    expect(projectHarnessSubagentOutcomeReceipts(rawResult)).toEqual([
      { toolCallId: 'compile-1', toolName: 'compile_latex', status: 'error' },
    ]);
  });

  it('verifies failed tool evidence from bounded Harness stream receipts omitted by FullOutput.toolResults', () => {
    const report = outcomeReport('blocked');
    const rawResult = withHarnessSubagentToolReceipts(
      {
        finishReason: 'tool-calls',
        terminalToolResult: outcomeEnvelope(report),
        // Mastra's current FullOutput omits locally thrown tool failures here.
        toolResults: [],
      },
      [{ toolCallId: 'compile-1', toolName: 'compile_latex', status: 'error' }],
    );

    expect(verifyHarnessSubagentOutcomeReport(rawResult)).toEqual(report);
    expect(projectHarnessSubagentOutcomeReceipts(rawResult)).toEqual([
      { toolCallId: 'compile-1', toolName: 'compile_latex', status: 'error' },
    ]);
    expect(summarizeHarnessSubagentResult(rawResult)).toMatchObject({
      status: 'error',
      outcome: 'blocked',
      issue: { code: 'compiler.unavailable' },
    });
  });

  it('fails closed when framework receipt capture overflows', () => {
    const rawResult = withHarnessSubagentToolReceipts(
      {
        finishReason: 'tool-calls',
        terminalToolResult: outcomeEnvelope(outcomeReport()),
        toolResults: [],
      },
      [{ toolCallId: 'compile-1', toolName: 'compile_latex', status: 'success' }],
      true,
    );

    expect(verifyHarnessSubagentOutcomeReport(rawResult)).toBeUndefined();
    expect(projectHarnessSubagentOutcomeReceipts(rawResult)).toEqual([]);
  });

  it('replaces provider-shaped reserved receipt fields with framework state', () => {
    const rawResult = withHarnessSubagentToolReceipts(
      {
        finishReason: 'tool-calls',
        terminalToolResult: outcomeEnvelope(outcomeReport()),
        toolResults: [],
        harnessToolReceipts: [{ toolCallId: 'compile-1', toolName: 'compile_latex', status: 'success' }],
        harnessToolReceiptsOverflow: true,
      },
      [],
    ) as Record<string, unknown>;

    expect(rawResult).not.toHaveProperty('harnessToolReceipts');
    expect(rawResult).not.toHaveProperty('harnessToolReceiptsOverflow');
    expect(verifyHarnessSubagentOutcomeReport(rawResult)).toBeUndefined();
  });

  it('executes as a terminal tool with the same validated value used by recovery', async () => {
    const tool = createSubagentOutcomeReportTool();
    const input = {
      outcome: 'completed' as const,
      summary: 'Source and compilation were verified.',
      evidence: [
        {
          kind: 'source' as const,
          description: 'The edited source was re-read.',
          reference: 'chapters/results.tex',
        },
      ],
    };

    const output = await tool.execute!(input, {} as never);
    expect(output).toEqual({ kind: HARNESS_SUBAGENT_OUTCOME_REPORT_KIND, ...input });
    expect(await tool.terminalResult!.isSuccess(output as never, {} as never)).toBe(true);
    expect(await tool.terminalResult!.project!(output as never, {} as never)).toEqual(output);
  });

  it('rejects blocked tool execution without a durable issue', async () => {
    const tool = createSubagentOutcomeReportTool();
    const output = await tool.execute!(
      {
        outcome: 'blocked',
        summary: 'Compiler unavailable.',
        evidence: [{ kind: 'analysis', description: 'The dependency did not respond.' }],
      } as never,
      {} as never,
    );

    expect(output).toMatchObject({ error: true });
  });
});
