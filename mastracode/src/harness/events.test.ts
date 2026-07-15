import type { HarnessEvent as HarnessV1Event } from '@mastra/core/harness/v1';
import { describe, expect, it } from 'vitest';

import { MastraCodeHarnessEventProjector } from './events.js';

function createProjector(displayState: Record<string, unknown> = {}, messages: any[] = []) {
  const events: any[] = [];
  const projector = new MastraCodeHarnessEventProjector(
    event => events.push(event),
    () => displayState,
    async (threadId, resourceId) => ({
      id: threadId,
      resourceId,
      title: 'Thread',
      createdAt: new Date(0),
      updatedAt: new Date(0),
    }),
    async () => messages,
  );
  return { events, projector };
}

describe('MastraCodeHarnessEventProjector', () => {
  it('projects v1 text deltas into legacy full-message updates', async () => {
    const { events, projector } = createProjector();

    await projector.project({
      type: 'text_delta',
      runId: 'r1',
      delta: 'hel',
      id: 'e1',
      timestamp: 1,
    } as HarnessV1Event);
    await projector.project({ type: 'text_delta', runId: 'r1', delta: 'lo', id: 'e2', timestamp: 2 } as HarnessV1Event);

    const starts = events.filter(event => event.type === 'message_start');
    expect(starts).toHaveLength(1);
    const updates = events.filter(event => event.type === 'message_update');
    expect(updates.at(-1)?.message.content).toEqual([{ type: 'text', text: 'hello' }]);
  });

  it('flushes the streamed message on agent_end', async () => {
    const { events, projector } = createProjector();

    await projector.project({
      type: 'text_delta',
      runId: 'r1',
      delta: 'done',
      id: 'e1',
      timestamp: 1,
    } as HarnessV1Event);
    await projector.project({
      type: 'agent_end',
      runId: 'r1',
      finishReason: 'stop',
      usage: {},
      id: 'e2',
      timestamp: 2,
    } as unknown as HarnessV1Event);

    const ends = events.filter(event => event.type === 'message_end');
    expect(ends).toHaveLength(1);
    expect(ends[0]?.message.content).toEqual([{ type: 'text', text: 'done' }]);
    expect(events.find(event => event.type === 'agent_end')).toMatchObject({ reason: 'complete' });
  });

  it('preserves structured parts from the authoritative completed message', async () => {
    const reminder = {
      id: 'reminder-1',
      role: 'user',
      content: [{ type: 'system_reminder', reminderType: 'dynamic-agents-md', message: 'instructions' }],
      createdAt: new Date(0),
    };
    const assistant = {
      id: 'stored-message-1',
      role: 'assistant',
      content: [{ type: 'text', text: 'done' }],
      createdAt: new Date(0),
    };
    const { events, projector } = createProjector({ assistantDrafts: { r1: { messageId: 'stream-part-1' } } }, [
      reminder,
      assistant,
    ]);

    await projector.project({
      type: 'text_delta',
      runId: 'r1',
      delta: 'done',
      id: 'e1',
      timestamp: 1,
    } as HarnessV1Event);
    await projector.project({
      type: 'agent_end',
      runId: 'r1',
      finishReason: 'complete',
      usage: {},
      id: 'e2',
      timestamp: 2,
    } as HarnessV1Event);

    const expected = { ...assistant, content: [...reminder.content, ...assistant.content] };
    expect(events.filter(event => event.type === 'message_update').at(-1)?.message).toEqual(expected);
    expect(events.find(event => event.type === 'message_end')?.message).toEqual(expected);
  });

  it('projects question_pending into ask_question events', async () => {
    const { events, projector } = createProjector();

    await projector.project({
      type: 'question_pending',
      runId: 'r1',
      itemId: 'q1',
      requestedAt: 1,
      toolCallId: 'tool-1',
      question: 'Proceed?',
      options: [{ label: 'Yes' }],
      source: 'parent',
      id: 'e1',
      timestamp: 1,
    } as HarnessV1Event);

    expect(events[0]).toMatchObject({
      type: 'ask_question',
      questionId: 'q1',
      question: 'Proceed?',
      options: [{ label: 'Yes' }],
    });
  });

  it('projects sandbox-shaped question_pending into sandbox access requests', async () => {
    const { events, projector } = createProjector();

    await projector.project({
      type: 'question_pending',
      runId: 'r1',
      itemId: 'sandbox_1_123',
      requestedAt: 1,
      toolCallId: 'call-1',
      question: 'Allow Mastra Code to access /outside/project/dir?\n\nneed to read config',
      options: [
        { label: 'Yes', description: 'Grant access for this session.' },
        { label: 'No', description: 'Deny this access request.' },
      ],
      selectionMode: 'single_select',
      source: 'parent',
      id: 'e1',
      timestamp: 1,
    } as HarnessV1Event);

    expect(events[0]).toMatchObject({
      type: 'sandbox_access_request',
      questionId: 'sandbox_1_123',
      path: '/outside/project/dir',
      reason: 'need to read config',
    });
  });

  it('keeps malformed sandbox-like questions on the generic ask_question path', async () => {
    const { events, projector } = createProjector();

    await projector.project({
      type: 'question_pending',
      runId: 'r1',
      itemId: 'sandbox_1_123',
      requestedAt: 1,
      toolCallId: 'call-1',
      question: 'Can I do something else?',
      options: [{ label: 'Yes' }],
      source: 'parent',
      id: 'e1',
      timestamp: 1,
    } as HarnessV1Event);

    expect(events[0]).toMatchObject({
      type: 'ask_question',
      questionId: 'sandbox_1_123',
      question: 'Can I do something else?',
    });
  });

  it('projects plan_approval_required into plan approval events', async () => {
    const { events, projector } = createProjector();

    await projector.project({
      type: 'plan_approval_required',
      runId: 'r1',
      itemId: 'p1',
      requestedAt: 1,
      toolCallId: 'tool-1',
      title: 'Implementation plan',
      plan: '1. Build it',
      source: 'parent',
      id: 'e1',
      timestamp: 1,
    } as HarnessV1Event);

    expect(events[0]).toMatchObject({
      type: 'plan_approval_required',
      planId: 'p1',
      title: 'Implementation plan',
      plan: '1. Build it',
    });
  });

  it('projects tool_approval_required into legacy tool approval events', async () => {
    const { events, projector } = createProjector();

    await projector.project({
      type: 'tool_approval_required',
      runId: 'r1',
      itemId: 'tool-1',
      requestedAt: 1,
      toolCallId: 'tool-1',
      toolName: 'run_command',
      toolCategory: 'execute',
      approvalReasons: [],
      input: { command: 'ls' },
      source: 'parent',
      id: 'e1',
      timestamp: 1,
    } as HarnessV1Event);

    expect(events[0]).toMatchObject({
      type: 'tool_approval_required',
      toolCallId: 'tool-1',
      toolName: 'run_command',
      args: { command: 'ls' },
      category: 'execute',
    });
  });

  it('projects tool_suspension_required into resumable legacy tool events', async () => {
    const { events, projector } = createProjector();

    await projector.project({
      type: 'tool_suspension_required',
      runId: 'r1',
      itemId: 'tool-1',
      requestedAt: 1,
      toolCallId: 'tool-1',
      toolName: 'long_running_tool',
      suspendData: { input: { id: 1 }, step: 'confirm' },
      source: 'parent',
      id: 'e1',
      timestamp: 1,
    } as HarnessV1Event);

    expect(events[0]).toMatchObject({
      type: 'tool_suspended',
      toolCallId: 'tool-1',
      toolName: 'long_running_tool',
      args: { id: 1 },
      suspendPayload: { input: { id: 1 }, step: 'confirm' },
    });
  });

  it('projects subagent tool args from input and stringifies structured subagent output', async () => {
    const { events, projector } = createProjector();

    await projector.project({
      type: 'subagent_tool_start',
      toolCallId: 'parent-tool',
      subagentSessionId: 'child',
      agentType: 'explore',
      innerToolCallId: 'inner-tool',
      toolName: 'read_file',
      input: { path: 'src/index.ts' },
      depth: 1,
      id: 'e1',
      timestamp: 1,
    } as HarnessV1Event);
    await projector.project({
      type: 'subagent_end',
      toolCallId: 'parent-tool',
      subagentSessionId: 'child',
      agentType: 'explore',
      output: { summary: 'done' },
      isError: false,
      durationMs: 12,
      depth: 1,
      id: 'e2',
      timestamp: 2,
    } as HarnessV1Event);

    expect(events.find(event => event.type === 'subagent_tool_start')).toMatchObject({
      subToolCallId: 'inner-tool',
      subToolName: 'read_file',
      subToolArgs: { path: 'src/index.ts' },
    });
    expect(events.find(event => event.type === 'subagent_end')).toMatchObject({
      result: JSON.stringify({ summary: 'done' }),
    });
  });

  it('maps tool_start.input onto the legacy tool_start.args field', async () => {
    const { events, projector } = createProjector();

    await projector.project({
      type: 'tool_start',
      runId: 'r1',
      toolCallId: 'tool-1',
      toolName: 'read_file',
      input: { path: 'README.md' },
      id: 'e1',
      timestamp: 1,
    } as HarnessV1Event);

    expect(events.find(event => event.type === 'tool_start')).toMatchObject({
      toolCallId: 'tool-1',
      toolName: 'read_file',
      args: { path: 'README.md' },
    });
  });
});
