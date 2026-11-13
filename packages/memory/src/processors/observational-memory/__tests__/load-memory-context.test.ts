import { MessageList } from '@mastra/core/agent';
import type { MastraDBMessage } from '@mastra/core/agent';
import { InMemoryStore } from '@mastra/core/storage';
import { describe, expect, it, vi } from 'vitest';
import { Memory } from '../../../index';
import { loadMemoryContextMessages } from '../observation-turn/load-memory-context';
import type { MemoryContextProvider } from '../processor';

function message(id: string, role: MastraDBMessage['role'], text: string, time: number): MastraDBMessage {
  return {
    id,
    role,
    createdAt: new Date(time),
    threadId: 'thread',
    resourceId: 'resource',
    content: { format: 2, parts: [{ type: 'text', text }] },
  };
}

function context(messages: MastraDBMessage[]) {
  const result = {
    messages,
    systemMessage: 'System context',
    hasObservations: false,
    omRecord: null,
    continuationMessage: undefined,
    otherThreadsContext: undefined,
  };
  const memory: MemoryContextProvider = { getContext: vi.fn(async () => result), persistMessages: vi.fn() };
  return { memory, result };
}

describe('22573 history loader', () => {
  it('keeps saved plan-revision feedback in a resumed run while filtering it from the next run', async () => {
    const memory = new Memory({
      storage: new InMemoryStore(),
      options: {
        observationalMemory: { toolCallFilter: { exclude: ['submit_plan'], preserveModelOutputFor: [] } },
      },
    });
    await memory.createThread({ threadId: 'thread', resourceId: 'resource' });
    const revision = message('revision', 'assistant', 'I will revise the plan.', 2);
    revision.content.parts.push({
      type: 'tool-invocation',
      toolInvocation: {
        state: 'result',
        toolCallId: 'rejected-plan',
        toolName: 'submit_plan',
        args: { plan: 'Compare the first and second papers.' },
        result: { approved: false, revision: 'Replace the second paper with the blue-light paper; keep the first.' },
      },
    });
    const list = new MessageList({ threadId: 'thread', resourceId: 'resource' });
    list.add(message('request', 'user', 'Compare these papers.', 1), 'input');
    list.add(revision, 'response');

    // PF-8921: suspension flushes and acknowledges the save before serializing the run.
    const save = list.snapshotUnsavedMessages({ detached: true });
    await memory.persistMessages(save.messages);
    save.commit();
    const resumed = new MessageList().deserialize(structuredClone(list.serialize()));
    await loadMemoryContextMessages({ memory, messageList: resumed, threadId: 'thread', resourceId: 'resource' });

    expect(resumed.get.all.db().map(message => message.id)).toEqual(['request', 'revision']);
    const prompt = await resumed.get.all.aiV5.llmPrompt();
    expect(prompt).toMatchObject([
      { role: 'user' },
      {
        role: 'assistant',
        content: [
          { type: 'text', text: 'I will revise the plan.' },
          { type: 'tool-call', toolCallId: 'rejected-plan', toolName: 'submit_plan' },
        ],
      },
      {
        role: 'tool',
        content: [
          {
            type: 'tool-result',
            toolCallId: 'rejected-plan',
            toolName: 'submit_plan',
            output: {
              type: 'json',
              value: {
                approved: false,
                revision: 'Replace the second paper with the blue-light paper; keep the first.',
              },
            },
          },
        ],
      },
    ]);

    // A new run has history, not the previous run's response provenance.
    const nextRun = new MessageList().add(structuredClone(resumed.get.all.db()), 'memory');
    await loadMemoryContextMessages({ memory, messageList: nextRun, threadId: 'thread', resourceId: 'resource' });
    expect(await nextRun.get.all.aiV5.llmPrompt()).toMatchObject([
      { role: 'user' },
      { role: 'assistant', content: [{ type: 'text', text: 'I will revise the plan.' }] },
    ]);
    expect(await nextRun.getRunResponseMessages()).toEqual([]);
  });

  it('preserves incoming same-ID client results without duplicating messages', async () => {
    const pending = message('assistant', 'assistant', '', 2);
    pending.content.parts = [
      {
        type: 'tool-invocation',
        toolInvocation: { state: 'call', toolCallId: 'color', toolName: 'changeColor', args: { color: 'green' } },
      },
    ];
    const incoming = structuredClone(pending);
    incoming.content.parts = [
      {
        type: 'tool-invocation',
        toolInvocation: {
          state: 'result',
          toolCallId: 'color',
          toolName: 'changeColor',
          args: { color: 'green' },
          result: { applied: true },
        },
      },
    ];
    const list = new MessageList({ threadId: 'thread', resourceId: 'resource' });
    list.add(incoming, 'input');
    const { memory, result } = context([pending]);
    expect(
      await loadMemoryContextMessages({ memory, messageList: list, threadId: 'thread', resourceId: 'resource' }),
    ).toBe(result);
    expect(list.get.all.db()).toHaveLength(1);
    expect(list.get.all.db()[0]).toMatchObject({
      id: 'assistant',
      createdAt: pending.createdAt,
      content: {
        parts: [
          {
            type: 'tool-invocation',
            toolInvocation: {
              state: 'result',
              toolCallId: 'color',
              toolName: 'changeColor',
              args: { color: 'green' },
              result: { applied: true },
            },
          },
        ],
      },
    });
    expect(list.get.input.db()).toHaveLength(1);
    expect(memory.persistMessages).not.toHaveBeenCalled();
  });

  it('keeps unrelated historical messages ordered and excludes historical system messages', async () => {
    const earlier = message('earlier', 'user', 'Earlier', 1);
    const later = message('later', 'assistant', 'Later', 2);
    const incoming = message('incoming', 'user', 'Now', 3);
    const list = new MessageList({ threadId: 'thread', resourceId: 'resource' });
    list.add(incoming, 'input');
    const { memory, result } = context([message('system', 'system', 'Do not insert', 0), earlier, later]);
    expect(
      await loadMemoryContextMessages({ memory, messageList: list, threadId: 'thread', resourceId: 'resource' }),
    ).toBe(result);
    expect(list.get.all.db().map(message => message.id)).toEqual(['earlier', 'later', 'incoming']);
    expect(memory.persistMessages).not.toHaveBeenCalled();
  });

  it('keeps the stored content and metadata when the client resends a stored message', async () => {
    const incoming = message('same', 'user', 'Updated content', 1);
    incoming.content.metadata = { clientRevision: 2 };
    const stored = message('same', 'user', 'Old content', 1);
    const list = new MessageList({ threadId: 'thread', resourceId: 'resource' });
    list.add(incoming, 'input');
    const { memory } = context([stored]);
    await loadMemoryContextMessages({ memory, messageList: list, threadId: 'thread', resourceId: 'resource' });
    const messages = list.get.all.db();
    expect(messages).toHaveLength(1);
    expect(messages[0]!.content.parts).toEqual([{ type: 'text', text: 'Old content' }]);
    expect(messages[0]!.content.metadata).toBeUndefined();
  });
});
