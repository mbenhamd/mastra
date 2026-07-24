import { describe, expect, it } from 'vitest';
import { mergeAgentExecutionOptions } from './merge-execution-options';

describe('mergeAgentExecutionOptions', () => {
  it('keeps caller scalar options authoritative', () => {
    expect(mergeAgentExecutionOptions({ maxSteps: 8 }, { maxSteps: 1 })).toMatchObject({ maxSteps: 1 });
  });

  it('does not retain default tools in a caller replacement surface', () => {
    const adminTool = { execute: () => 'admin' };
    const safeTool = { execute: () => 'safe' };

    const merged = mergeAgentExecutionOptions(
      {
        toolsetsMode: 'replace',
        toolsets: { defaults: { adminTool } },
      },
      {
        toolsetsMode: 'replace',
        toolsets: { caller: { safeTool } },
      },
    );

    expect(merged.toolsets).toEqual({ caller: { safeTool } });
  });
});
