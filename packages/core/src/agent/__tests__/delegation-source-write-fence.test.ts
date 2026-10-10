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
