/**
 * Processor pipeline tests for DurableAgent
 *
 * Tests that input, output, and error processors are properly
 * wired into the durable execution path.
 */

import { describe, it, expect, vi } from 'vitest';
import { Agent } from '@mastra/core/agent';
import { createDurableAgent, globalRunRegistry } from '@mastra/core/agent/durable';
import type { DurableAgentTestContext } from '../types';
import { createTextStreamModel } from '../mock-models';

export function createProcessorPipelineTests(context: DurableAgentTestContext) {
  const { getPubSub } = context;

  describe('processor pipeline', () => {
    describe('input processors', () => {
      it('should run processInput during preparation', async () => {
        const processInputSpy = vi.fn(async ({ messages }: any) => ({ messages }));

        const mockProcessor = {
          id: 'spy-input-processor',
          processInput: processInputSpy,
        };

        const agent = new Agent({
          id: 'input-proc-agent',
          name: 'Input Proc Agent',
          instructions: 'Test',
          model: createTextStreamModel('Hello'),
          inputProcessors: [mockProcessor],
        } as any);

        const durableAgent = createDurableAgent({
          agent,
          pubsub: getPubSub(),
        });

        await durableAgent.prepare('Hello');

        expect(processInputSpy).toHaveBeenCalled();
      });
    });

    describe('output processors', () => {
      it('should carry output processors in the registry entry when configured', async () => {
        const mockOutputProcessor = {
          id: 'test-output-processor',
          processOutput: async ({ messages }: any) => ({ messages }),
        };

        const agent = new Agent({
          id: 'output-proc-agent',
          name: 'Output Proc Agent',
          instructions: 'Test',
          model: createTextStreamModel('Hello'),
          outputProcessors: [mockOutputProcessor],
        } as any);

        const durableAgent = createDurableAgent({
          agent,
          pubsub: getPubSub(),
        });

        const result = await durableAgent.prepare('Hello');

        try {
          // Output processors array is always present (may be empty if resolution filters them)
          const registryEntry = globalRunRegistry.get(result.runId);
          expect(registryEntry?.outputProcessors).toBeDefined();
          expect(Array.isArray(registryEntry?.outputProcessors)).toBe(true);
        } finally {
          result.cleanup();
        }
      });
    });

    describe('error processors', () => {
      it('should carry error processors in the registry entry', async () => {
        const mockErrorProcessor = {
          id: 'test-error-processor',
          processAPIError: async () => ({ retry: false }),
        };

        const agent = new Agent({
          id: 'error-proc-agent',
          name: 'Error Proc Agent',
          instructions: 'Test',
          model: createTextStreamModel('Hello'),
          errorProcessors: [mockErrorProcessor],
        } as any);

        const durableAgent = createDurableAgent({
          agent,
          pubsub: getPubSub(),
        });

        const result = await durableAgent.prepare('Hello');

        try {
          const registryEntry = globalRunRegistry.get(result.runId);
          // The agent adds its default error processors after the caller's own.
          expect(registryEntry?.errorProcessors).toBeDefined();
          expect(registryEntry?.errorProcessors!.map(p => p.id)).toEqual([
            'test-error-processor',
            'provider-history-compat',
            'prefill-error-handler',
            'unsupported-file-handler',
            'stream-error-retry-processor',
          ]);
        } finally {
          result.cleanup();
        }
      });

      it('should set hasErrorProcessors flag in workflow input', async () => {
        const mockErrorProcessor = {
          id: 'flag-error-processor',
          processAPIError: async () => ({ retry: false }),
        };

        const agent = new Agent({
          id: 'error-flag-agent',
          name: 'Error Flag Agent',
          instructions: 'Test',
          model: createTextStreamModel('Hello'),
          errorProcessors: [mockErrorProcessor],
        } as any);

        const durableAgent = createDurableAgent({
          agent,
          pubsub: getPubSub(),
        });

        const result = await durableAgent.prepare('Hello');

        expect(result.workflowInput.options.hasErrorProcessors).toBe(true);
      });
    });

    describe('combined processors', () => {
      it('should handle both input and output processors', async () => {
        const mockInputProcessor = {
          id: 'combined-input',
          processInput: async ({ messages }: any) => ({ messages }),
        };
        const mockOutputProcessor = {
          id: 'combined-output',
          processOutput: async ({ messages }: any) => ({ messages }),
        };

        const agent = new Agent({
          id: 'combined-proc-agent',
          name: 'Combined Proc Agent',
          instructions: 'Test',
          model: createTextStreamModel('Result'),
          inputProcessors: [mockInputProcessor],
          outputProcessors: [mockOutputProcessor],
        } as any);

        const durableAgent = createDurableAgent({
          agent,
          pubsub: getPubSub(),
        });

        const result = await durableAgent.prepare('Hello');

        try {
          const registryEntry = globalRunRegistry.get(result.runId);
          expect(registryEntry?.inputProcessors).toBeDefined();
          expect(registryEntry?.outputProcessors).toBeDefined();
          expect(registryEntry?.processorStates).toBeDefined();
        } finally {
          result.cleanup();
        }
      });
    });
  });
}
