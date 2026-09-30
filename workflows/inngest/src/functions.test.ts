import { Agent } from '@mastra/core/agent';
import { Mastra } from '@mastra/core/mastra';
import { MockStore } from '@mastra/core/storage';
import { Inngest } from 'inngest';
import { describe, expect, it, vi } from 'vitest';

import { createInngestAgent } from './durable-agent';
import { collectInngestFunctions } from './functions';

function createAgent(id: string) {
  return new Agent({
    id,
    name: id,
    instructions: 'Test',
    model: {
      provider: 'test',
      modelId: 'test-model',
      specificationVersion: 'v1',
      supportsStructuredOutputs: true,
      doGenerate: vi.fn(),
      doStream: vi.fn(),
    } as any,
  });
}

describe('collectInngestFunctions()', () => {
  it('collects the original durable workflow owned by an Inngest agent', () => {
    const inngest = new Inngest({ id: 'test-app' });
    const durableAgent = createInngestAgent({ agent: createAgent('test-agent'), inngest });
    const workflow = durableAgent.getDurableWorkflows()[0];
    const originalFunctions = workflow.getFunctions();
    const mastra = new Mastra({
      storage: new MockStore(),
      agents: { testAgent: durableAgent },
    });

    const functions = collectInngestFunctions({ mastra });

    expect(functions).toEqual(originalFunctions);
    expect(workflow.__getPubsubFactory()).toBeTypeOf('function');
  });

  // Fork contract (diverges from upstream's shared loop workflow): durable loop
  // workflows are agent-scoped, so every Inngest agent contributes its own
  // functions and none is shadowed by the first agent's.
  it('collects the agent-scoped durable workflows of every Inngest agent', () => {
    const inngest = new Inngest({ id: 'test-app' });
    const firstAgent = createInngestAgent({ agent: createAgent('first-agent'), inngest });
    const secondAgent = createInngestAgent({ agent: createAgent('second-agent'), inngest });
    const firstFunctions = firstAgent.getDurableWorkflows()[0].getFunctions();
    const secondFunctions = secondAgent.getDurableWorkflows()[0].getFunctions();
    const mastra = new Mastra({
      storage: new MockStore(),
      agents: { firstAgent, secondAgent },
    });

    const functions = collectInngestFunctions({ mastra });

    expect(functions).toEqual([...firstFunctions, ...secondFunctions]);
    expect(new Set(functions.map(fn => fn.id())).size).toBe(functions.length);
  });
});
