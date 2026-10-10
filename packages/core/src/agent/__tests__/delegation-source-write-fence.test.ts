import { MockLanguageModelV2 } from '@internal/ai-sdk-v5/test';
import { describe, expect, it } from 'vitest';
import { MockMemory } from '../../memory/mock';
import { RequestContext } from '../../request-context';
import { Agent } from '../agent';

describe('rejected delegation source-write fence', () => {
  it('does not recreate an erased child thread when a resumed delegation is rejected', async () => {
    const childMemory = new MockMemory();
    const store = (await childMemory.storage.getStore('memory'))!;
    const child = new Agent({
      id: 'child',
      name: 'child',
      description: 'Child agent',
      instructions: 'test',
      model: new MockLanguageModelV2({}),
      memory: childMemory,
    });
    const parentModel = new MockLanguageModelV2({});
    const parent = new Agent({
      id: 'parent',
      name: 'parent',
      instructions: 'test',
      model: parentModel,
      agents: { child },
    });

    const tools = await (parent as any).listAgentTools({
      runId: 'parent-run',
      threadId: 'parent-thread',
      resourceId: 'parent-resource',
      requestContext: new RequestContext(),
      methodType: 'generate',
      delegation: { onDelegationStart: () => ({ proceed: false, rejectionReason: 'blocked' }) },
      getModel: async () => parentModel,
    });
    const tool = tools['agent-child'];

    // The original child thread for run `suspended-run` was erased before this resume.
    const result = await tool.execute(
      { prompt: 'resume the delegated work', threadId: 'parent-thread', resourceId: 'parent-resource' },
      { toolCallId: 'call-1', messages: [], suspendedToolRunId: 'suspended-run' },
    );

    expect(result).toMatchObject({ text: expect.stringContaining('[Delegation Rejected]') });
    await expect(store.getThreadById({ threadId: 'parent-thread-suspended-run' })).resolves.toBeNull();
  });
});

/** MockMemory that behaves like a Memory instance with `sourceWriteFencing: 'required'`. */
class FencedMockMemory extends MockMemory {
  override async prepareObservationalMemorySourceWriteGuard(threadId: string, resourceId?: string) {
    const store = (await this.storage.getStore('memory'))!;
    const record =
      (await store.getObservationalMemory(threadId, resourceId!)) ??
      (await store.initializeObservationalMemory({ threadId, resourceId: resourceId!, scope: 'thread', config: {} }));
    return { recordId: record.id, threadId, resourceId: resourceId! };
  }

  override async saveMessages(args: Parameters<MockMemory['saveMessages']>[0]) {
    if (!args.observationalMemorySourceWriteGuard) {
      throw new Error('source write fencing is required but no captured record guard was provided');
    }
    return super.saveMessages(args);
  }
}

describe('delegation projection source-write fence', () => {
  it('projects a child-default-memory transcript with a fence prepared for the projection coordinates', async () => {
    const childMemory = new FencedMockMemory();
    const store = (await childMemory.storage.getStore('memory'))!;
    const child = new Agent({
      id: 'child',
      name: 'child',
      description: 'Child agent',
      instructions: 'test',
      model: new MockLanguageModelV2({
        doGenerate: async () => ({
          rawCall: { rawPrompt: null, rawSettings: {} },
          finishReason: 'stop',
          usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
          content: [{ type: 'text', text: 'child answer' }],
          warnings: [],
        }),
      }),
      memory: childMemory,
      // The child persists its own run to its default coordinates; the delegation
      // transcript is projected to separate coordinates afterwards.
      defaultOptions: { memory: { thread: 'child-own-thread', resource: 'child-own-resource' } },
    });
    const parentModel = new MockLanguageModelV2({});
    const parent = new Agent({
      id: 'parent',
      name: 'parent',
      instructions: 'test',
      model: parentModel,
      agents: { child },
    });
    const tools = await (parent as any).listAgentTools({
      runId: 'parent-run',
      threadId: 'parent-thread',
      resourceId: 'parent-resource',
      requestContext: new RequestContext(),
      methodType: 'generate',
      getModel: async () => parentModel,
    });

    const result = await tools['agent-child'].execute(
      { prompt: 'delegate', threadId: 'parent-thread', resourceId: 'parent-resource' },
      { toolCallId: 'call-1', messages: [] },
    );

    const projected = await store.listMessages({
      threadId: result.subAgentThreadId,
      resourceId: result.subAgentResourceId,
      perPage: false,
    });
    expect(projected.messages.map(message => message.role)).toEqual(expect.arrayContaining(['user', 'assistant']));
  });
});
