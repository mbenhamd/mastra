import { MockLanguageModelV2 } from '@internal/ai-sdk-v5/test';
import { describe, expect, it, vi } from 'vitest';
import { Agent } from './index';

describe('Agent maxSteps validation', () => {
  it.each([0, -1, 1.5, Number.NaN, Number.POSITIVE_INFINITY])(
    'rejects invalid maxSteps %s before calling the provider',
    async maxSteps => {
      const doStream = vi.fn();
      const agent = new Agent({
        id: 'max-steps-validation',
        name: 'max-steps-validation',
        instructions: 'Answer concisely.',
        model: new MockLanguageModelV2({ doStream }),
      });

      await expect(agent.stream('hello', { maxSteps })).rejects.toThrow('maxSteps must be a positive safe integer');
      expect(doStream).not.toHaveBeenCalled();
    },
  );
});
