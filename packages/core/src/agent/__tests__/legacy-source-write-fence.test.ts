import { MockLanguageModelV1 } from '@internal/ai-sdk-v4/test';
import { describe, expect, it } from 'vitest';
import { MockMemory } from '../../memory/mock';
import { Agent } from '../agent';

describe('legacy per-step thread creation source-write fence', () => {
  it('does not recreate a thread erased during the first model step', async () => {
    const threadId = 'legacy-fence-thread';
    const resourceId = 'legacy-fence-resource';
    const memory = new MockMemory();
    const store = (await memory.storage.getStore('memory'))!;
    const record = await store.initializeObservationalMemory({ threadId, resourceId, scope: 'thread', config: {} });
    const guard = { recordId: record.id, threadId, resourceId };

    const agent = new Agent({
      id: 'legacy-fence-agent',
      name: 'Legacy Fence Agent',
      instructions: 'test',
      memory,
      model: new MockLanguageModelV1({
        doGenerate: async () => {
          // Authoritative erasure lands while the first model call is in flight.
          await store.retractObservationalMemory({ resourceId, threadId });
          await store.deleteThread({ threadId });
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            finishReason: 'stop',
            usage: { promptTokens: 1, completionTokens: 1 },
            text: 'late response',
          };
        },
      }),
    });

    await agent
      .generateLegacy('hello', {
        threadId,
        resourceId,
        savePerStep: true,
        observationalMemorySourceWriteGuard: guard,
      } as never)
      .catch(() => undefined);

    await expect(store.getThreadById({ threadId })).resolves.toBeNull();
    await expect(store.getObservationalMemory(threadId, resourceId)).resolves.toBeNull();
  });
});
