import { describe, expect, it } from 'vitest';
import type { MastraDBMessage } from '../../../agent/message-list';
import { MessageList } from '../../../agent/message-list';
import { commitToolResult, withToolResultStatusProviderMetadata } from './tool-result-commit-core';

function makeMessageList() {
  const messageList = new MessageList();
  const message: MastraDBMessage = {
    id: 'message-1',
    role: 'assistant',
    content: {
      format: 2,
      parts: [
        {
          type: 'tool-invocation',
          toolInvocation: {
            state: 'call',
            toolCallId: 'call-1',
            toolName: 'test-tool',
            args: {},
          },
        },
      ],
    },
    createdAt: new Date(),
  };
  messageList.add(message, 'response');
  return messageList;
}

describe('commitToolResult', () => {
  it.each([0, '', false])('preserves a falsy structured result on an error outcome: %j', result => {
    const messageList = makeMessageList();

    expect(
      commitToolResult({
        messageList,
        outcome: { kind: 'error', errorText: 'failed', result },
        toolCallId: 'call-1',
        toolName: 'test-tool',
        toolArgs: {},
      }),
    ).toBe(true);

    const invocation = messageList.get.all.db()[0]?.content.parts?.[0];
    expect(invocation).toMatchObject({
      type: 'tool-invocation',
      toolInvocation: { state: 'output-error', errorText: 'failed', result },
    });
  });
});

describe('withToolResultStatusProviderMetadata', () => {
  const classify = (output: unknown) => ((output as { success?: boolean }).success ? 'success' : 'error');

  it('replaces an earlier error classification when the recorded result succeeds', () => {
    // A background completion carries the classification of its raw result;
    // the recorded result is classified again after processToolResult.
    const metadata = withToolResultStatusProviderMetadata({
      tool: { resultStatus: classify },
      result: { success: true },
      providerMetadata: { openai: { itemId: 'item-1' }, mastra: { resultStatus: 'error', modelOutput: 'x' } },
    });

    expect(metadata).toEqual({ openai: { itemId: 'item-1' }, mastra: { modelOutput: 'x' } });
  });

  it('marks a recorded failure and records a throwing classifier as a failure', () => {
    expect(
      withToolResultStatusProviderMetadata({
        tool: { resultStatus: classify },
        result: { success: false },
        providerMetadata: undefined,
      }),
    ).toEqual({ mastra: { resultStatus: 'error' } });
    expect(
      withToolResultStatusProviderMetadata({
        tool: {
          resultStatus: () => {
            throw new Error('classifier bug');
          },
        },
        result: { success: true },
        providerMetadata: undefined,
      }),
    ).toEqual({ mastra: { resultStatus: 'error' } });
  });
});
