import { stepCountIs } from '@internal/ai-sdk-v5';
import { describe, expect, it, vi } from 'vitest';
import { z } from 'zod/v4';
import { createScorer } from '../../../../evals/base';
import type { Mastra } from '../../../../mastra';
import { createTool } from '../../../../tools';
import { createSharedAgent, runLoopScenario, useLoopScenarioAimock } from '../aimock-scenario';

describe('AIMock loop scenario: mutable optional stages', () => {
  const getMock = useLoopScenarioAimock();

  it('retains an initially empty scorer configuration when a tool adds a scorer', async () => {
    let scorerCalls = 0;
    let scoredOutput: unknown;
    const scorer = createScorer({
      id: 'mutable-completion-scorer',
      name: 'Mutable Completion Scorer',
      description: 'Confirms the final model answer is complete.',
    }).generateScore(({ run }) => {
      scorerCalls++;
      scoredOutput = run.output;
      return 1;
    });
    const scorerList: Array<typeof scorer> = [];
    const isTaskComplete = { scorers: scorerList };
    const addScorer = createTool({
      id: 'add-completion-scorer',
      description: 'Adds the completion scorer while the first model turn executes.',
      inputSchema: z.object({ value: z.string() }),
      outputSchema: z.object({ value: z.string() }),
      execute: async ({ value }) => {
        scorerList.push(scorer);
        return { value };
      },
    });

    const llm = getMock();
    const shared = await createSharedAgent(llm, { tools: { addScorer: addScorer } });
    const mastra: Mastra = shared.mastra;
    const previousEvented = process.env.MASTRA_EVENTED_EXECUTION;
    vi.stubEnv('MASTRA_EVENTED_EXECUTION', 'false');

    try {
      const { output, requests } = await runLoopScenario({
        engine: 'normal',
        llm,
        sharedAgent: shared,
        prompt: 'Add the completion scorer, then answer.',
        maxSteps: 4,
        stopWhen: stepCountIs(4),
        isTaskComplete,
        fixtures: mock => {
          mock.on(
            { endpoint: 'chat', sequenceIndex: 0 },
            { toolCalls: [{ id: 'call-add-scorer', name: 'addScorer', arguments: { value: 'ready' } }] },
          );
          mock.on({ endpoint: 'chat', sequenceIndex: 1 }, { content: 'final answer' });
        },
      });

      expect(await output.text).toBe('final answer');
      expect(requests).toHaveLength(2);
      expect(scorerCalls).toBe(1);
      expect(JSON.stringify(scoredOutput)).toContain('final answer');
    } finally {
      if (previousEvented === undefined) delete process.env.MASTRA_EVENTED_EXECUTION;
      else vi.stubEnv('MASTRA_EVENTED_EXECUTION', previousEvented);
      await mastra.shutdown();
    }
  });
});
