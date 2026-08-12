import { Agent } from '@mastra/core/agent';
import { globalRunRegistry, resolveRuntimeDependencies } from '@mastra/core/agent/durable';
import { EventEmitterPubSub } from '@mastra/core/events';
import { Mastra } from '@mastra/core/mastra';
import { RequestContext } from '@mastra/core/request-context';
import { DefaultStorage } from '@mastra/libsql';
import { Inngest } from 'inngest';
import { describe, expect, it, vi } from 'vitest';
import { createInngestAgent } from './index';

function createMockModel() {
  return {
    provider: 'test',
    modelId: 'test-model',
    specificationVersion: 'v1',
    doGenerate: vi.fn(),
    doStream: vi.fn(),
  };
}

describe('InngestAgent resume runtime rehydration', () => {
  it('rebuilds runtime dependencies after the initial stream registry is cleaned up', async () => {
    const inngest = new Inngest({ id: 'resume-runtime-rehydration' });
    const agent = new Agent({
      id: 'resume-runtime-rehydration-agent',
      name: 'Resume Runtime Rehydration Agent',
      instructions: 'Test',
      model: createMockModel() as any,
    });
    const durableAgent = createInngestAgent({ agent, inngest, durableRequestContextKeys: ['tenantId'] });
    const mastra = new Mastra({
      logger: false,
      storage: new DefaultStorage({ id: 'resume-runtime-rehydration-storage', url: ':memory:' }),
      agents: { [agent.id]: durableAgent },
    });
    (durableAgent.pubsub as any).inner = new EventEmitterPubSub();
    const sendSpy = vi.spyOn(inngest as any, 'send').mockResolvedValue({ ids: ['test-event'] });
    const requestContext = new RequestContext();
    requestContext.set('tenantId', 'tenant-1');
    const initialResult = await durableAgent.stream([{ role: 'user', content: 'hi' }], { requestContext });
    const { runId } = initialResult;

    try {
      await vi.waitFor(() => expect(globalRunRegistry.get(runId)?.workflowExecution).toBeDefined());
      await expect(globalRunRegistry.get(runId)!.workflowExecution).resolves.toBeUndefined();

      const dispatch = sendSpy.mock.calls[0]?.[0] as any;
      const workflowInput = dispatch?.data?.inputData;
      expect(workflowInput?.requestContextEntries).toEqual({ tenantId: 'tenant-1' });

      const workflowId = durableAgent.getDurableWorkflows()[0]!.id;
      const workflowsStore = await mastra.getStorage()!.getStore('workflows');
      if (!workflowsStore) throw new Error('workflow storage is unavailable');
      const runningSnapshot = await workflowsStore.loadWorkflowSnapshot({ workflowName: workflowId, runId });
      expect(runningSnapshot).toBeDefined();
      await workflowsStore.persistWorkflowSnapshot({
        workflowName: workflowId,
        runId,
        snapshot: {
          ...runningSnapshot!,
          status: 'suspended',
          context: { input: workflowInput },
          suspendedPaths: { 'agentic-loop': [0] },
          requestContext: workflowInput.requestContextEntries,
        },
      });

      initialResult.cleanup();
      expect(globalRunRegistry.has(runId)).toBe(false);

      const restoredModel = createMockModel();
      const restoredTools = {};
      const assertRestoredContext = (context: RequestContext) => {
        expect(context.get('tenantId')).toBe('tenant-1');
      };
      vi.spyOn(agent, 'getToolsForExecution').mockImplementation(async ({ requestContext: context }) => {
        expect(context).toBeDefined();
        assertRestoredContext(context!);
        return restoredTools;
      });
      vi.spyOn(agent, 'getModel').mockImplementation(async options => {
        expect(options?.requestContext).toBeDefined();
        assertRestoredContext(options!.requestContext!);
        return restoredModel as any;
      });

      const resumedResult = await durableAgent.resume(runId, { approved: true });
      try {
        const placeholder = globalRunRegistry.get(runId);
        expect(placeholder?.isPlaceholder).toBe(true);
        const placeholderAbortController = placeholder?.abortController;
        expect(placeholderAbortController).toBeInstanceOf(AbortController);

        // The durable LLM step performs this resolution on the Inngest worker.
        const resolved = await resolveRuntimeDependencies({
          mastra,
          runId,
          agentId: workflowInput.agentId,
          input: workflowInput,
        });

        expect(resolved.model).toBe(restoredModel);
        expect(resolved.tools).toBe(restoredTools);
        expect(placeholder?.isPlaceholder).toBe(false);
        expect(placeholder?.abortController).toBe(placeholderAbortController);
        await placeholder?.workflowExecution;
      } finally {
        resumedResult.cleanup();
      }
    } finally {
      initialResult.cleanup();
      globalRunRegistry.delete(runId);
      sendSpy.mockRestore();
      await mastra.shutdown();
    }
  });
});
