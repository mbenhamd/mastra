import { describe, expect, it } from 'vitest';
import type { DurableAgenticWorkflowInput } from '../types';
import { mapDurableIterationToLLMInput } from './map-llm-input';
import { createBaseIterationStateUpdate } from './shared/iteration-state';
import { baseIterationStateSchema } from './shared/schemas';

describe('mapDurableIterationToLLMInput', () => {
  it('preserves runtime identity and recovery guards for nested LLM steps', () => {
    const input = {
      runId: 'registry-required-run',
      runtimeBindingId: 'binding-1',
      agentId: 'registry-required-agent',
      runtimeBindings: { memory: 'memory-1', workspace: 'workspace-1' },
      runtimeResolution: 'registry-required',
      messageListState: {},
      toolsMetadata: [],
      modelConfig: {},
      options: {},
      responseRecoveryPhase: 'reserved',
      state: {},
      messageId: 'message-1',
    } as DurableAgenticWorkflowInput;

    expect(mapDurableIterationToLLMInput(input)).toEqual(
      expect.objectContaining({
        runId: 'registry-required-run',
        runtimeBindingId: 'binding-1',
        runtimeBindings: { memory: 'memory-1', workspace: 'workspace-1' },
        runtimeResolution: 'registry-required',
        responseRecoveryPhase: 'reserved',
      }),
    );
  });

  it('round-trips and advances the JSON-safe recovery phase without callback state', () => {
    const state = baseIterationStateSchema.parse({
      __workflowKind: 'durable-agent',
      runId: 'cold-run',
      agentId: 'agent',
      messageListState: {},
      toolsMetadata: [],
      modelConfig: {},
      options: {},
      responseRecoveryPhase: 'reserved',
      state: {},
      messageId: 'message',
      iterationCount: 1,
      accumulatedSteps: [],
      accumulatedUsage: { inputTokens: 0, outputTokens: 0, totalTokens: 0 },
    });

    const updated = createBaseIterationStateUpdate({
      currentState: state,
      executionOutput: {
        messageListState: {},
        messageId: 'message',
        stepResult: { reason: 'stop', warnings: [], isContinued: false },
        toolResults: [],
        output: { text: '', usage: { inputTokens: 0, outputTokens: 0, totalTokens: 0 }, steps: [] },
        state: {},
        responseRecoveryPhase: 'consumed',
      },
    });

    expect(JSON.parse(JSON.stringify(updated)).responseRecoveryPhase).toBe('consumed');
  });
});
