import { describe, expect, it } from 'vitest';
import { toolCallOutputSchema } from './schema';

// Guards the request-abort fields on toolCallOutputSchema (#17995). The evented engine
// validates step outputs against this schema, and Zod strips undeclared keys. Both the
// incomplete-call marker and its terminal-only error must survive into llm-mapping-step.
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
