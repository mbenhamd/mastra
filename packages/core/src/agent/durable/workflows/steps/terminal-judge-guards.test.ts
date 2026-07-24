import { describe, expect, it, vi } from 'vitest';
import { createDurableGoalStep } from './goal';
import { createDurableIsTaskCompleteStep } from './is-task-complete';

describe('durable terminal-result judge guards', () => {
  it.each([
    ['isTaskComplete', createDurableIsTaskCompleteStep()],
    ['goal', createDurableGoalStep()],
  ])('does not run the %s judge after direct terminal delivery', async (_name, step) => {
    const state = {
      runId: 'terminal-judge-guard',
      iterationCount: 1,
      messageId: 'assistant-1',
      messageListState: {},
      accumulatedSteps: [],
      lastStepResult: { isContinued: false },
      terminalToolResult: {
        status: 'success',
        items: [{ toolName: 'answer', toolCallId: 'call-1', status: 'success', value: { answer: 'done' } }],
      },
    };
    const getAgentById = vi.fn(() => {
      throw new Error('judge resolution must not run');
    });

    await expect(
      (step as any).execute({
        inputData: state,
        getInitData: () => ({ agentId: 'judge-agent' }),
        mastra: { getAgentById },
      }),
    ).resolves.toBe(state);
    expect(getAgentById).not.toHaveBeenCalled();
  });
});
