import { describe, expect, it, vi } from 'vitest';
import { globalRunRegistry } from '../../run-registry';
import { createDurableToolCallStep } from './tool-call';

vi.mock('../../utils/resolve-runtime', () => ({
  resolveTool: vi.fn(),
  toolApprovalRequirement: vi.fn().mockResolvedValue({ required: false, reasons: [] }),
  rebuildRunToolsFromMastra: vi.fn().mockResolvedValue(undefined),
}));

const RUNTIME_BINDING_ID = 'durable-tool-fga-binding';

function makeParams(runId: string, overrides: Record<string, any> = {}) {
  return {
    inputData: {
      toolCallId: 'call-1',
      toolName: 'secureTool',
      args: { query: 'mastra' },
    },
    mastra: { getLogger: () => undefined },
    suspend: vi.fn(),
    getInitData: () => ({
      runId,
      runtimeBindingId: RUNTIME_BINDING_ID,
      agentId: 'agent-1',
      options: {},
      state: {},
    }),
    ...overrides,
  };
}

describe('durable tool-call FGA error propagation', () => {
  it('re-throws FGADeniedError instead of serializing it as a recoverable tool error', async () => {
    const runId = 'durable-tool-fga-run';
    const denial = new Error('access denied');
    denial.name = 'FGADeniedError';
    const execute = vi.fn().mockRejectedValue(denial);
    globalRunRegistry.set(runId, { runtimeBindingId: RUNTIME_BINDING_ID, tools: { secureTool: { execute } } } as any);

    try {
      await expect((createDurableToolCallStep() as any).execute(makeParams(runId))).rejects.toThrow('access denied');
    } finally {
      globalRunRegistry.delete(runId);
    }
  });

  it('serializes non-FGA errors as recoverable tool errors', async () => {
    const runId = 'durable-tool-generic-error-run';
    const execute = vi.fn().mockRejectedValue(new Error('boom'));
    globalRunRegistry.set(runId, { runtimeBindingId: RUNTIME_BINDING_ID, tools: { secureTool: { execute } } } as any);

    try {
      const result = await (createDurableToolCallStep() as any).execute(makeParams(runId));
      expect(result.error).toMatchObject({ name: 'Error', message: 'boom' });
    } finally {
      globalRunRegistry.delete(runId);
    }
  });
});
