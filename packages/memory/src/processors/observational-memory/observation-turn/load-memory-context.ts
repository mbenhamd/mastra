import type { MessageList } from '@mastra/core/agent';
import type { MemoryRunState } from '@mastra/core/memory';
import type { ObservationalMemorySourceWriteGuard } from '@mastra/core/storage';

import type { MemoryContextProvider } from '../processor';

export async function loadMemoryContextMessages({
  memory,
  messageList,
  threadId,
  resourceId,
  runState,
  observationalMemorySourceWriteGuard,
}: {
  memory: MemoryContextProvider;
  messageList: MessageList;
  threadId: string;
  resourceId?: string;
  runState?: MemoryRunState;
  observationalMemorySourceWriteGuard?: ObservationalMemorySourceWriteGuard;
}): Promise<Awaited<ReturnType<MemoryContextProvider['getContext']>>> {
  const ctx = await memory.getContext({ threadId, resourceId, runState, observationalMemorySourceWriteGuard });

  for (const msg of ctx.messages) {
    if (msg.role !== 'system') {
      messageList.add(msg, 'memory');
    }
  }

  return ctx;
}
