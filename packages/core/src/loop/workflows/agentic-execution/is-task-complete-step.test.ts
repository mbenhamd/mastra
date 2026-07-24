import { describe, expect, it, vi, beforeEach } from 'vitest';
import { MessageList } from '../../../agent/message-list';
import { RequestContext } from '../../../request-context';
import * as validation from '../../network/validation';
import { createIsTaskCompleteStep } from './is-task-complete-step';

function baseParams() {
  return {
    isTaskComplete: {
      scorers: [{ id: 'test-scorer' } as any],
      strategy: 'any' as const,
    },
    maxSteps: 10,
    messageList: new MessageList(),
    requestContext: new RequestContext(),
    mastra: { generateId: () => 'test-id' } as any,
    controller: { enqueue: vi.fn() } as any,
    runId: 'run-1',
    _internal: {},
    agentId: 'agent-1',
    agentName: 'agent-1',
  };
}

function executeStep(step: any, inputData: any) {
  return step.execute({ inputData });
}

function makeInput(
  opts: {
    toolCalls?: Array<{ toolName: string }>;
    isContinued?: boolean;
    bgPending?: boolean;
    terminalToolResult?: unknown;
  } = {},
) {
  return {
    backgroundTaskPending: opts.bgPending ?? false,
    terminalToolResult: opts.terminalToolResult,
    stepResult: { isContinued: opts.isContinued ?? false },
    output: {
      text: 'done',
      toolCalls: opts.toolCalls ?? [],
      toolResults: [],
    },
  };
}

describe('isTaskCompleteStep — working memory skip', () => {
  let runScorersSpy: ReturnType<typeof vi.spyOn>;

  beforeEach(() => {
    vi.restoreAllMocks();
    runScorersSpy = vi
      .spyOn(validation, 'runStreamCompletionScorers')
      .mockResolvedValue({ complete: true, scorers: [] } as any);
    vi.spyOn(validation, 'formatStreamCompletionFeedback').mockReturnValue('' as any);
  });

  it('skips scorers when only tool call was updateWorkingMemory', async () => {
    const step = createIsTaskCompleteStep(baseParams() as any);

    await executeStep(step, makeInput({ toolCalls: [{ toolName: 'updateWorkingMemory' }] }));

    expect(runScorersSpy).not.toHaveBeenCalled();
  });

  it('skips scorers for the kebab-case id too', async () => {
    const step = createIsTaskCompleteStep(baseParams() as any);

    await executeStep(step, makeInput({ toolCalls: [{ toolName: 'update-working-memory' }] }));

    expect(runScorersSpy).not.toHaveBeenCalled();
  });

  it('runs scorers when a non-working-memory tool is also called', async () => {
    const step = createIsTaskCompleteStep(baseParams() as any);

    await executeStep(step, makeInput({ toolCalls: [{ toolName: 'updateWorkingMemory' }, { toolName: 'searchWeb' }] }));

    expect(runScorersSpy).toHaveBeenCalled();
  });

  it('runs scorers when no tool calls were made', async () => {
    const step = createIsTaskCompleteStep(baseParams() as any);

    await executeStep(step, makeInput({ toolCalls: [] }));

    expect(runScorersSpy).toHaveBeenCalled();
  });

  it('skips scorers and feedback after a terminal tool result', async () => {
    const params = baseParams();
    const step = createIsTaskCompleteStep(params as any);
    const input = makeInput({
      terminalToolResult: {
        status: 'success',
        items: [{ toolName: 'answer', toolCallId: 'call-1', status: 'success', value: { answer: 'done' } }],
      },
    });

    const result = await executeStep(step, input);

    expect(result).toBe(input);
    expect(runScorersSpy).not.toHaveBeenCalled();
    expect(params.controller.enqueue).not.toHaveBeenCalled();
    expect(params.messageList.get.response.db()).toEqual([]);
  });
});
