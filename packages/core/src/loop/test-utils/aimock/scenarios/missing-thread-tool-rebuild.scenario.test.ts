import { expect, it } from 'vitest';
import { z } from 'zod/v4';
import { MockMemory } from '../../../../memory/mock';
import { ToolSearchProcessor } from '../../../../processors/processors/tool-search';
import { RequestContext } from '../../../../request-context';
import { createTool } from '../../../../tools';
import {
  createSharedAgent,
  describeForAllEngines,
  isDurableEngineVariant,
  runLoopScenario,
  useLoopScenarioAimock,
} from '../aimock-scenario';

// Match persistent memory adapters: recalling a missing thread is an error.
// Do not replace the agent's recall helper or the processor's tool reconstruction.
class StrictMemory extends MockMemory {
  override async recall(...args: Parameters<MockMemory['recall']>) {
    if (!(await this.getThreadById({ threadId: args[0].threadId }))) {
      throw new Error(`Thread ${args[0].threadId} not found`);
    }
    return super.recall(...args);
  }
}

// #26902: tool reconstruction can run before the first thread has been created.
describeForAllEngines(
  'AIMock loop scenario: rebuild tools before the first conversation',
  engine => {
    const getMock = useLoopScenarioAimock();
    it('treats missing history as empty and completes the first turn', async () => {
      const memory = new StrictMemory();
      const weather = createTool({
        id: 'weather',
        description: 'Get weather.',
        inputSchema: z.object({ city: z.string() }),
        execute: async () => ({ temperature: 20 }),
      });
      const shared = await createSharedAgent(getMock(), {
        engine,
        memory,
        inputProcessors: [new ToolSearchProcessor({ tools: { weather }, storage: 'context' })],
      });
      const threadId = `first-thread-${engine}`;
      const resourceId = 'new-customer';
      try {
        expect(await memory.getThreadById({ threadId })).toBeNull();
        const tools = await shared.agent.getToolsForExecution({
          threadId,
          resourceId,
          requestContext: new RequestContext(),
        });
        expect(tools.weather).toBeUndefined();
        const { output, requests, chunks } = await runLoopScenario({
          engine,
          llm: getMock(),
          sharedAgent: shared,
          memory,
          threadId,
          resourceId,
          prompt: 'Hello',
          fixtures: llm => llm.onMessage('Hello', { content: 'Welcome!' }),
          collectChunks: true,
        });
        if (isDurableEngineVariant(engine)) {
          // PF-2040 (PF-6640 sync): durable tool surfaces must stay reconstructible from the
          // registered agent, so the fork's durable engines refuse an input processor that adds
          // executable tools (ToolSearchProcessor's search/load tools) before the model runs.
          // Tool reconstruction for the missing thread above still treated history as empty.
          const errors = chunks!.filter(c => c.type === 'error') as any[];
          expect(errors.map(c => String(c.payload?.error?.message ?? ''))).toEqual([
            expect.stringContaining('Durable tool surfaces must remain reconstructible from the registered agent'),
          ]);
          expect(requests).toHaveLength(0);
          return;
        }
        expect(await output.text).toBe('Welcome!');
        expect(requests).toHaveLength(1);
        expect(await memory.getThreadById({ threadId })).toMatchObject({ id: threadId });
        const { messages } = await memory.recall({ threadId, resourceId });
        expect(JSON.stringify(messages)).toContain('Welcome!');
      } finally {
        if (engine !== 'inngest') await shared.mastra.shutdown({ drainTimeout: 1000 });
      }
    });
  },
  { skip: ['fs'] },
);
