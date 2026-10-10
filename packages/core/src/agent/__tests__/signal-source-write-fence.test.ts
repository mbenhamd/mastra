import { MockLanguageModelV2 } from '@internal/ai-sdk-v5/test';
import { describe, expect, it, vi } from 'vitest';
import { MockMemory } from '../../memory/mock';
import { ObservationalMemorySourceWriteConflictError } from '../../storage';
import { Agent } from '../agent';
import { MessageList } from '../message-list';
import { applyStateSignal } from '../state-signals';

async function setup(threadId: string, resourceId: string) {
  const memory = new MockMemory();
  await memory.createThread({ threadId, resourceId });
  const store = (await memory.storage.getStore('memory'))!;
  const record = await store.initializeObservationalMemory({ threadId, resourceId, scope: 'thread', config: {} });
  const guard = { recordId: record.id, threadId, resourceId };
  const agent = new Agent({
    id: `${threadId}-agent`,
    name: 'Signal Fence Agent',
    instructions: 'test',
    model: new MockLanguageModelV2({}),
    memory,
  });
  return { memory, store, guard, agent };
}

describe('direct signal writes honor the captured source-write guard', () => {
  it('persists an idle signal with the guard passed in stream options', async () => {
    const threadId = 'signal-fence-valid';
    const resourceId = 'signal-fence-resource';
    const { memory, store, guard, agent } = await setup(threadId, resourceId);
    const saveMessages = vi.spyOn(store, 'saveMessages');

    const result = agent.sendSignal(
      { type: 'user-message', contents: 'remember this' },
      {
        resourceId,
        threadId,
        ifIdle: { behavior: 'persist', streamOptions: { observationalMemorySourceWriteGuard: guard } },
      },
    );
    await expect(result.accepted).resolves.toMatchObject({ action: 'persist' });

    const { messages } = await memory.recall({ threadId, resourceId });
    expect(messages).toHaveLength(1);
    expect(saveMessages).toHaveBeenCalledWith(expect.objectContaining({ observationalMemorySourceWriteGuard: guard }));
  });

  it('rejects a state signal with a revoked guard without changing thread state metadata', async () => {
    const threadId = 'signal-fence-revoked';
    const resourceId = 'signal-fence-resource';
    const { memory, store, guard, agent } = await setup(threadId, resourceId);
    await store.retractObservationalMemory({ resourceId, threadId });

    await expect(
      agent.sendStateSignal(
        { id: 'browser', cacheKey: 'browser:v1', mode: 'snapshot', contents: 'stale state' },
        {
          resourceId,
          threadId,
          ifIdle: { behavior: 'persist', streamOptions: { observationalMemorySourceWriteGuard: guard } },
        },
      ),
    ).rejects.toBeInstanceOf(ObservationalMemorySourceWriteConflictError);

    const thread = await memory.getThreadById({ threadId });
    expect((thread?.metadata?.mastra as { stateSignals?: unknown } | undefined)?.stateSignals).toBeUndefined();
    const { messages } = await memory.recall({ threadId, resourceId });
    expect(messages).toHaveLength(0);
  });

  it('leaves the live transcript and stream untouched when the guarded state write is rejected', async () => {
    const threadId = 'signal-fence-live';
    const resourceId = 'signal-fence-resource';
    const { memory, store, guard } = await setup(threadId, resourceId);
    await store.retractObservationalMemory({ resourceId, threadId });
    const messageList = new MessageList({ threadId, resourceId });
    const writeSignal = vi.fn();
    const beforeAddSignal = vi.fn();

    await expect(
      applyStateSignal({
        input: { id: 'browser', cacheKey: 'browser:v1', mode: 'snapshot', contents: 'stale state' },
        memory,
        thread: (await memory.getThreadById({ threadId }))!,
        resourceId,
        threadId,
        observationalMemorySourceWriteGuard: guard,
        messageList,
        beforeAddSignal,
        writeSignal,
      }),
    ).rejects.toBeInstanceOf(ObservationalMemorySourceWriteConflictError);

    expect(messageList.get.all.db()).toHaveLength(0);
    expect(beforeAddSignal).not.toHaveBeenCalled();
    expect(writeSignal).not.toHaveBeenCalled();
  });
});
