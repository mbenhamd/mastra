import { describe, expect, it } from 'vitest';
import { AGENT_EXECUTION_OPTION_COMPOSERS, mergeAgentExecutionOptions } from './merge-execution-options';

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

  it('composes internal hooks after configured callbacks without exposing the composer marker', async () => {
    const calls: string[] = [];
    const configured = async () => {
      calls.push('configured');
      return { activeTools: ['safe'], workspace: 'configured-workspace' };
    };
    const merged = mergeAgentExecutionOptions(
      { prepareStep: configured },
      {
        [AGENT_EXECUTION_OPTION_COMPOSERS]: () => ({
          prepareStep: (existing: typeof configured | undefined) => async () => {
            const prepared = await existing?.();
            calls.push('internal');
            return { ...prepared, activeTools: [] };
          },
        }),
      },
    );

    await expect(merged.prepareStep()).resolves.toEqual({ activeTools: [], workspace: 'configured-workspace' });
    expect(calls).toEqual(['configured', 'internal']);
    expect(Reflect.ownKeys(merged)).not.toContain(AGENT_EXECUTION_OPTION_COMPOSERS);
  });
});
