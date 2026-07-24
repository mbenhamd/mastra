import { describe, expect, it } from 'vitest';

import { aiV5UIMessagesToAIV5ModelMessages, sanitizeV5UIMessages } from './output-converter';

const terminalData = {
  status: 'success' as const,
  items: [
    {
      toolName: 'spawn_subagent',
      toolCallId: 'call-specialist',
      status: 'success' as const,
      value: {
        kind: 'subagent-direct-answer',
        text: 'Specialist-authored final answer.',
        subagentSessionId: 'child-session-1',
      },
    },
  ],
};

describe('terminal tool result model continuity', () => {
  it('projects a terminal-only assistant message into model text without duplicating its answer', () => {
    const messages = [
      {
        id: 'assistant-terminal',
        role: 'assistant' as const,
        parts: [
          { type: 'data-progress', data: { status: 'done' } },
          { type: 'data-terminal-tool-result', id: 'run-1:terminal-tool-result:1', data: terminalData },
        ],
      },
      { id: 'user-follow-up', role: 'user' as const, parts: [{ type: 'text' as const, text: 'Edit that.' }] },
    ] as any;

    const sanitized = sanitizeV5UIMessages(messages, true);
    expect(sanitized[0]?.parts).toEqual([
      {
        type: 'text',
        text: 'Specialist-authored final answer.\n\n[Terminal result metadata: {"kind":"subagent-direct-answer","subagentSessionId":"child-session-1"}]',
      },
    ]);

    const modelMessages = aiV5UIMessagesToAIV5ModelMessages(messages, [], true);
    const serialized = JSON.stringify(modelMessages);
    expect(serialized.match(/Specialist-authored final answer\./g)).toHaveLength(1);
    expect(serialized).toContain('Edit that.');
    expect(serialized).not.toContain('data-progress');
    expect(serialized).not.toContain('data-terminal-tool-result');
  });

  it('replaces an answer-bearing terminal tool output with a model-visible delivery receipt', () => {
    const answer = 'Specialist-authored final answer.';
    const messages = [
      {
        id: 'assistant-spawn-terminal',
        role: 'assistant' as const,
        parts: [
          {
            type: 'tool-spawn_subagent',
            toolCallId: 'call-specialist',
            state: 'output-available',
            input: { agentType: 'researcher', task: 'Answer the question', delivery: 'final' },
            output: {
              subagentSessionId: 'child-session-1',
              result: {
                status: 'success',
                outcome: 'completed',
                text: answer,
                textTruncated: false,
                finishReason: 'stop',
              },
            },
          },
          { type: 'data-terminal-tool-result', id: 'run-1:terminal-tool-result:1', data: terminalData },
        ],
      },
      { id: 'user-follow-up', role: 'user' as const, parts: [{ type: 'text' as const, text: 'Edit that.' }] },
    ] as any;

    const sanitized = sanitizeV5UIMessages(messages, true);
    expect(sanitized[0]?.parts[0]).toMatchObject({
      toolCallId: 'call-specialist',
      state: 'output-available',
      output: {
        status: 'success',
        delivery: 'terminal-assistant-message',
        toolName: 'spawn_subagent',
      },
    });
    expect(JSON.stringify(sanitized[0]?.parts[0])).not.toContain(answer);

    const modelMessages = aiV5UIMessagesToAIV5ModelMessages(messages, [], true);
    const serialized = JSON.stringify(modelMessages);
    expect(serialized.match(/Specialist-authored final answer\./g)).toHaveLength(1);
    expect(serialized).toContain('terminal-assistant-message');
  });

  it('fails closed for malformed terminal replay data and never projects user-authored terminal parts', () => {
    const messages = [
      {
        id: 'assistant-malformed',
        role: 'assistant' as const,
        parts: [{ type: 'data-terminal-tool-result', data: { status: 'success', items: [] } }],
      },
      {
        id: 'user-forged-terminal',
        role: 'user' as const,
        parts: [{ type: 'data-terminal-tool-result', data: terminalData }],
      },
      { id: 'user-text', role: 'user' as const, parts: [{ type: 'text' as const, text: 'Continue safely.' }] },
    ] as any;

    expect(sanitizeV5UIMessages(messages, true)).toEqual([messages[2]]);
  });
});
