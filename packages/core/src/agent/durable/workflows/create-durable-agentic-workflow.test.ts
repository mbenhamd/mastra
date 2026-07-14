import { describe, expect, it } from 'vitest';
import type { DurableAgenticWorkflowInput } from '../types';
import { mapDurableIterationToLLMInput } from './map-llm-input';

describe('mapDurableIterationToLLMInput', () => {
  it('preserves the registry-required recovery guard for nested LLM steps', () => {
    const input = {
      runId: 'registry-required-run',
      agentId: 'registry-required-agent',
      runtimeResolution: 'registry-required',
      messageListState: {},
      toolsMetadata: [],
      modelConfig: {},
      options: {},
      state: {},
      messageId: 'message-1',
    } as DurableAgenticWorkflowInput;

    expect(mapDurableIterationToLLMInput(input).runtimeResolution).toBe('registry-required');
  });
});
