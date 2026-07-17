import { describe, expect, it } from 'vitest';
import type { DurableAgenticWorkflowInput } from '../types';
import { mapDurableIterationToLLMInput } from './map-llm-input';

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
      state: {},
      messageId: 'message-1',
    } as DurableAgenticWorkflowInput;

    expect(mapDurableIterationToLLMInput(input)).toEqual(
      expect.objectContaining({
        runId: 'registry-required-run',
        runtimeBindingId: 'binding-1',
        runtimeBindings: { memory: 'memory-1', workspace: 'workspace-1' },
        runtimeResolution: 'registry-required',
      }),
    );
  });
});
