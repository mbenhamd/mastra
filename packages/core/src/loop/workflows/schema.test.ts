import { describe, expect, it } from 'vitest';
import { llmIterationOutputSchema, toolCallOutputSchema } from './schema';

// Guards the request-abort fields on toolCallOutputSchema (#17995). No engine actually
// validates step outputs against this schema today — the workflows engine has no
// output-side validation, and input validation is disabled (`validateInputs: false`) in
// both loop builders — so the schema exists for type/schema honesty. Zod strips
// undeclared keys on parse, so if validation is ever (re-)enabled, an undeclared field
// would silently drop `{ aborted: true }` before llm-mapping-step sees it, defeating the
// fix. Pins that both the incomplete-call marker and its terminal-only error survive the
// single-object and array shapes.
describe('toolCallOutputSchema aborted field survival', () => {
  const aborted = {
    toolCallId: 'srv-1',
    toolName: 'slowServerTool',
    args: { q: 'important' },
    aborted: true,
    abortError: { name: 'Error', message: 'local_project.operation_cancelled' },
  };

  it('preserves request-abort metadata through a single-object parse', () => {
    const parsed = toolCallOutputSchema.parse(aborted);
    expect(parsed).toMatchObject({
      aborted: true,
      abortError: { name: 'Error', message: 'local_project.operation_cancelled' },
    });
  });

  it('preserves request-abort metadata through the evented-engine array boundary', () => {
    const parsed = toolCallOutputSchema.array().parse([aborted]);
    expect(parsed[0]).toMatchObject({
      aborted: true,
      abortError: { name: 'Error', message: 'local_project.operation_cancelled' },
    });
  });

  it('still allows the normal result/error shapes without an `aborted` flag', () => {
    const withResult = toolCallOutputSchema.parse({
      toolCallId: 'ok-1',
      toolName: 't',
      args: {},
      result: { ok: true },
    });
    expect(withResult.aborted).toBeUndefined();
    expect(withResult.result).toEqual({ ok: true });
  });
});

describe('private signal-preemption continuation', () => {
  it('survives schema and JSON boundaries without becoming a processor retry or contaminating accepted steps', () => {
    const input = {
      messageId: 'message',
      messages: { all: [], user: [], nonUser: [] },
      output: { text: '', toolCalls: [], usage: {}, steps: [] },
      metadata: {},
      stepResult: { reason: 'other', warnings: [], isContinued: true, signalPreempted: true },
      processorRetryCount: 0,
    };
    const parsed = llmIterationOutputSchema.parse(JSON.parse(JSON.stringify(input)));
    expect(parsed.stepResult).toEqual(input.stepResult);
    expect(parsed.processorRetryCount).toBe(0);
    const accepted = llmIterationOutputSchema.parse({
      ...parsed,
      stepResult: { reason: 'stop', warnings: [], isContinued: false },
    });
    expect(accepted.stepResult.signalPreempted).toBeUndefined();
  });
});
