import { describe, expect, it, vi } from 'vitest';
import { AGENT_RESPONSE_RECOVERY_CONTINUATION } from '../../merge-execution-options';
import { MessageList } from '../../message-list';
import { globalRunRegistry } from '../run-registry';
import type { DurableAgenticWorkflowInput } from '../types';
import { createDurableAgenticWorkflow } from './create-durable-agentic-workflow';
import { mapDurableIterationToLLMInput } from './map-llm-input';

function findLoopEntry(steps: any[]): any {
  for (const entry of steps ?? []) {
    if (entry.type === 'loop' && entry.loopType === 'dowhile') return entry;
    if (entry.step?.executionGraph) {
      const nested = findLoopEntry(entry.step.executionGraph.steps);
      if (nested) return nested;
    }
    if (entry.steps) {
      const nested = findLoopEntry(entry.steps);
      if (nested) return nested;
    }
  }
  return undefined;
}

describe('mapDurableIterationToLLMInput', () => {
  it('preserves runtime identity and recovery guards for nested LLM steps', () => {
    const input = {
      __workflowKind: 'durable-agent',
      runId: 'registry-required-run',
      runtimeBindingId: 'binding-1',
      agentId: 'registry-required-agent',
      runtimeBindings: { memory: 'memory-1', workspace: 'workspace-1' },
      runtimeResolution: 'registry-required',
      messageListState: {},
      toolsMetadata: [],
      modelConfig: {},
      options: {},
      responseRecovery: { phase: 'reserved', reservedAtIteration: 7 },
      state: {},
      messageId: 'message-1',
    } as DurableAgenticWorkflowInput;

    expect(mapDurableIterationToLLMInput(input)).toEqual(
      expect.objectContaining({
        runId: 'registry-required-run',
        runtimeBindingId: 'binding-1',
        runtimeBindings: { memory: 'memory-1', workspace: 'workspace-1' },
        runtimeResolution: 'registry-required',
        responseRecovery: { phase: 'reserved', reservedAtIteration: 7 },
      }),
    );
  });
});

describe('createDurableAgenticWorkflow response recovery arbitration', () => {
  it('keeps a matching caller stop condition terminal at the ordinary maxSteps boundary', async () => {
    const runId = 'stop-when-recovery-run';
    const runtimeBindingId = 'stop-when-recovery-binding';
    const workflow = createDurableAgenticWorkflow() as any;
    const loopEntry = findLoopEntry(workflow.executionGraph.steps);
    const stopWhen = vi.fn(async () => true);
    const onIterationComplete = vi.fn(async () => ({
      continue: true,
      [AGENT_RESPONSE_RECOVERY_CONTINUATION]: true,
    }));
    globalRunRegistry.set(runId, { runtimeBindingId, stopWhen, onIterationComplete } as any);
    const state: any = {
      __workflowKind: 'durable-agent',
      runId,
      runtimeBindingId,
      agentId: 'recovery-agent',
      options: { maxSteps: 1, recoveryMaxSteps: 1 },
      iterationCount: 1,
      accumulatedSteps: [{ text: '', toolCalls: [], toolResults: [], finishReason: 'stop' }],
      messageListState: new MessageList().serialize(),
      messageId: 'message-1',
      state: {},
      lastStepResult: { reason: 'stop', warnings: [], isContinued: true },
    };

    try {
      await expect(
        loopEntry.condition({
          inputData: state,
          getInitData: () => state,
          mastra: undefined,
        }),
      ).resolves.toBe(false);

      expect(stopWhen).toHaveBeenCalledTimes(1);
      expect(state.responseRecovery).toBeUndefined();
      expect(state.lastStepResult.isContinued).toBe(false);
    } finally {
      globalRunRegistry.delete(runId);
    }
  });

  it('reserves a marked response-only continuation before the ordinary maxSteps ceiling', async () => {
    const runId = 'ordinary-budget-recovery-run';
    const runtimeBindingId = 'ordinary-budget-recovery-binding';
    const workflow = createDurableAgenticWorkflow() as any;
    const loopEntry = findLoopEntry(workflow.executionGraph.steps);
    const onIterationComplete = vi.fn(async () => ({
      continue: true,
      feedback: 'Report the completed tool result without calling tools.',
      [AGENT_RESPONSE_RECOVERY_CONTINUATION]: true,
    }));
    globalRunRegistry.set(runId, { runtimeBindingId, onIterationComplete } as any);
    const state: any = {
      __workflowKind: 'durable-agent',
      runId,
      runtimeBindingId,
      agentId: 'recovery-agent',
      options: { maxSteps: 1000, recoveryMaxSteps: 1 },
      iterationCount: 2,
      accumulatedSteps: [
        {
          text: '',
          toolCalls: [],
          toolResults: [{ toolCallId: 'call-1', toolName: 'write', result: { ok: true } }],
          finishReason: 'stop',
        },
      ],
      messageListState: new MessageList().serialize(),
      messageId: 'message-1',
      state: {},
      lastStepResult: { reason: 'stop', warnings: [], isContinued: false },
    };

    try {
      await expect(
        loopEntry.condition({
          inputData: state,
          getInitData: () => state,
          mastra: undefined,
        }),
      ).resolves.toBe(true);

      expect(state.responseRecovery).toEqual({ phase: 'reserved', reservedAtIteration: 2 });
      expect(state.lastStepResult.isContinued).toBe(true);
      expect(onIterationComplete).toHaveBeenCalledTimes(1);
    } finally {
      globalRunRegistry.delete(runId);
    }
  });
});
