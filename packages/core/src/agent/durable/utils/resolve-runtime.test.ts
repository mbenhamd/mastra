import { describe, expect, it, vi } from 'vitest';
import { MASTRA_RESOURCE_ID_KEY, MASTRA_THREAD_ID_KEY } from '../../../request-context';
import { MessageList } from '../../message-list';
import { globalRunRegistry } from '../run-registry';
import { createDurableRuntimeRequestContext, resolveRuntimeDependencies } from './resolve-runtime';

describe('durable runtime request context', () => {
  it('rebinds inherited parent memory coordinates to the durable child run', () => {
    const context = createDurableRuntimeRequestContext({
      entries: {
        tenantId: 'tenant-1',
        MastraMemory: {
          thread: { id: 'parent-thread' },
          resourceId: 'parent-resource',
          memoryConfig: { readOnly: false },
        },
        [MASTRA_THREAD_ID_KEY]: 'parent-thread',
        [MASTRA_RESOURCE_ID_KEY]: 'parent-resource',
      },
      state: {
        memoryConfigured: true,
        threadId: 'child-thread',
        resourceId: 'child-resource',
        memoryConfig: { readOnly: true },
      },
    });

    expect(context.get('tenantId')).toBe('tenant-1');
    expect(context.get(MASTRA_THREAD_ID_KEY)).toBe('child-thread');
    expect(context.get(MASTRA_RESOURCE_ID_KEY)).toBe('child-resource');
    expect(context.get('MastraMemory')).toEqual({
      thread: { id: 'child-thread' },
      resourceId: 'child-resource',
      memoryConfig: { readOnly: true },
    });
  });

  it('uses the exact child memory context for every cold runtime dependency', async () => {
    const runId = 'cold-child-memory-context';
    globalRunRegistry.delete(runId);
    const contexts: unknown[] = [];
    const capture = (value: unknown) => {
      contexts.push(value);
      return value;
    };
    const agent = {
      getToolsForExecution: vi.fn(async ({ requestContext }) => {
        capture(requestContext);
        return {};
      }),
      getModel: vi.fn(async ({ requestContext }) => {
        capture(requestContext);
        return { provider: 'test', modelId: 'test-model', specificationVersion: 'v2' };
      }),
      getModelList: vi.fn(async requestContext => {
        capture(requestContext);
        return undefined;
      }),
      getMemory: vi.fn(async ({ requestContext }) => {
        capture(requestContext);
        return undefined;
      }),
      getWorkspace: vi.fn(async ({ requestContext }) => {
        capture(requestContext);
        return undefined;
      }),
      listInputProcessors: vi.fn(async requestContext => {
        capture(requestContext);
        return [];
      }),
      __listLLMRequestProcessors: vi.fn(async requestContext => {
        capture(requestContext);
        return [];
      }),
      listOutputProcessors: vi.fn(async requestContext => {
        capture(requestContext);
        return [];
      }),
      listErrorProcessors: vi.fn(async requestContext => {
        capture(requestContext);
        return [];
      }),
    };
    const input = {
      __workflowKind: 'durable-agent',
      runId,
      agentId: 'cold-child-agent',
      messageListState: new MessageList({
        threadId: 'child-thread',
        resourceId: 'child-resource',
      })
        .add({ role: 'user', content: 'persisted child message' }, 'input')
        .serialize(),
      toolsMetadata: [],
      modelConfig: { provider: 'test', modelId: 'test-model', specificationVersion: 'v2' },
      options: {},
      state: {
        memoryConfigured: true,
        threadId: 'child-thread',
        resourceId: 'child-resource',
        memoryConfig: { readOnly: true },
      },
      messageId: 'child-message',
      requestContextEntries: {
        tenantId: 'tenant-1',
        MastraMemory: {
          thread: { id: 'parent-thread' },
          resourceId: 'parent-resource',
          memoryConfig: { readOnly: false },
        },
        [MASTRA_THREAD_ID_KEY]: 'parent-thread',
        [MASTRA_RESOURCE_ID_KEY]: 'parent-resource',
      },
    } as any;

    try {
      await resolveRuntimeDependencies({
        runId,
        agentId: input.agentId,
        input,
        mastra: { getAgentById: () => agent } as any,
      });
    } finally {
      globalRunRegistry.delete(runId);
    }

    expect(contexts.length).toBeGreaterThan(0);
    for (const context of contexts as Array<{ get(key: string): unknown }>) {
      expect(context.get('tenantId')).toBe('tenant-1');
      expect(context.get(MASTRA_THREAD_ID_KEY)).toBe('child-thread');
      expect(context.get(MASTRA_RESOURCE_ID_KEY)).toBe('child-resource');
      expect(context.get('MastraMemory')).toEqual({
        thread: { id: 'child-thread' },
        resourceId: 'child-resource',
        memoryConfig: { readOnly: true },
      });
    }
    expect(agent.getToolsForExecution).toHaveBeenCalledWith(
      expect.objectContaining({
        processorMessages: [
          expect.objectContaining({
            role: 'user',
            content: expect.objectContaining({
              parts: [expect.objectContaining({ type: 'text', text: 'persisted child message' })],
            }),
          }),
        ],
      }),
    );
  });
});
