import { MockLanguageModelV1 } from '@internal/ai-sdk-v4/test';
import { describe, expect, it, vi } from 'vitest';
import { Agent } from '../agent';
import type { InputProcessorOrWorkflow, OutputProcessorOrWorkflow, ErrorProcessorOrWorkflow } from '../processors';
import { ProcessorStepSchema } from '../processors/step-schema';
import { createWorkflow } from '../workflows';
import { Mastra } from './index';

/**
 * Tests for processor workflow registration in Mastra.addAgent.
 *
 * When an agent is added to Mastra, lone processors stay directly registered;
 * only processor chains and explicit workflows are registered as workflows.
 */
describe('Processor Workflow Registration', () => {
  // Helper to wait for async workflow registration
  const waitForWorkflowRegistration = () => new Promise(resolve => setTimeout(resolve, 50));

  const createMockModel = () =>
    new MockLanguageModelV1({
      doGenerate: async () => ({
        rawCall: { rawPrompt: null, rawSettings: {} },
        finishReason: 'stop',
        usage: { promptTokens: 10, completionTokens: 20 },
        text: 'Test response',
      }),
    });

  describe('Static processor registration', () => {
    it('should register a lone static input processor without a synthetic workflow', async () => {
      const inputProcessor: InputProcessorOrWorkflow = {
        id: 'test-input-processor',
        processInput: async ({ messages }) => messages,
      };

      const agent = new Agent({
        id: 'test-agent',
        name: 'Test Agent',
        instructions: 'Test',
        model: createMockModel(),
        inputProcessors: [inputProcessor],
      });

      const mastra = new Mastra({
        logger: false,
        agents: { testAgent: agent },
      });

      // Wait for async registration
      await waitForWorkflowRegistration();

      expect(mastra.listProcessors()?.['test-input-processor']).toBe(inputProcessor);
      expect(() => mastra.getWorkflow('test-agent-input-processor')).toThrow();
    });

    it('should register a lone static output processor without a synthetic workflow', async () => {
      const outputProcessor: OutputProcessorOrWorkflow = {
        id: 'test-output-processor',
        processOutputResult: async ({ messages }) => messages,
      };

      const agent = new Agent({
        id: 'test-agent-output',
        name: 'Test Agent',
        instructions: 'Test',
        model: createMockModel(),
        outputProcessors: [outputProcessor],
      });

      const mastra = new Mastra({
        logger: false,
        agents: { testAgent: agent },
      });

      // Wait for async registration
      await waitForWorkflowRegistration();

      expect(mastra.listProcessors()?.['test-output-processor']).toBe(outputProcessor);
      expect(() => mastra.getWorkflow('test-agent-output-output-processor')).toThrow();
    });

    it('should directly register lone input/output processors without registering error processors', async () => {
      const inputProcessor: InputProcessorOrWorkflow = {
        id: 'test-input',
        processInput: async ({ messages }) => messages,
      };

      const outputProcessor: OutputProcessorOrWorkflow = {
        id: 'test-output',
        processOutputResult: async ({ messages }) => messages,
      };

      const errorProcessor: ErrorProcessorOrWorkflow = {
        id: 'test-error',
        processAPIError: async () => ({ retry: false }),
      };

      const agent = new Agent({
        id: 'test-agent-both',
        name: 'Test Agent',
        instructions: 'Test',
        model: createMockModel(),
        inputProcessors: [inputProcessor],
        outputProcessors: [outputProcessor],
        errorProcessors: [errorProcessor],
      });

      const mastra = new Mastra({
        logger: false,
        agents: { testAgent: agent },
      });

      // Wait for async registration
      await waitForWorkflowRegistration();

      expect(mastra.listProcessors()?.['test-input']).toBe(inputProcessor);
      expect(mastra.listProcessors()?.['test-output']).toBe(outputProcessor);
      expect(mastra.listProcessors()?.['test-error']).toBeUndefined();
      expect(() => mastra.getWorkflow('test-agent-both-input-processor')).toThrow();
      expect(() => mastra.getWorkflow('test-agent-both-output-processor')).toThrow();
      expect(() => mastra.getWorkflow('test-agent-both-error-processor')).toThrow();
    });

    it('should not register workflows when agent has no processors', async () => {
      const agent = new Agent({
        id: 'test-agent-no-processors',
        name: 'Test Agent',
        instructions: 'Test',
        model: createMockModel(),
      });

      const mastra = new Mastra({
        logger: false,
        agents: { testAgent: agent },
      });

      // Wait for async registration
      await waitForWorkflowRegistration();

      // Should not have registered any processor workflows
      expect(() => mastra.getWorkflow('test-agent-no-processors-input-processor')).toThrow();
      expect(() => mastra.getWorkflow('test-agent-no-processors-output-processor')).toThrow();
    });
  });

  describe('Function-based processor registration', () => {
    it('should directly register a lone function-based input processor', async () => {
      const inputProcessor: InputProcessorOrWorkflow = {
        id: 'dynamic-input-processor',
        processInput: async ({ messages }) => messages,
      };

      const processorFn = vi.fn().mockReturnValue([inputProcessor]);

      const agent = new Agent({
        id: 'test-agent-fn',
        name: 'Test Agent',
        instructions: 'Test',
        model: createMockModel(),
        inputProcessors: processorFn as unknown as () => InputProcessorOrWorkflow[],
      });

      const mastra = new Mastra({
        logger: false,
        agents: { testAgent: agent },
      });

      // Wait for async registration
      await waitForWorkflowRegistration();

      // The function should have been called
      expect(processorFn).toHaveBeenCalled();

      expect(mastra.listProcessors()?.['dynamic-input-processor']).toBe(inputProcessor);
      expect(() => mastra.getWorkflow('test-agent-fn-input-processor')).toThrow();
    });

    it('should directly register a lone function-based output processor', async () => {
      const outputProcessor: OutputProcessorOrWorkflow = {
        id: 'dynamic-output-processor',
        processOutputResult: async ({ messages }) => messages,
      };

      const processorFn = vi.fn().mockReturnValue([outputProcessor]);

      const agent = new Agent({
        id: 'test-agent-output-fn',
        name: 'Test Agent',
        instructions: 'Test',
        model: createMockModel(),
        outputProcessors: processorFn as unknown as () => OutputProcessorOrWorkflow[],
      });

      const mastra = new Mastra({
        logger: false,
        agents: { testAgent: agent },
      });

      // Wait for async registration
      await waitForWorkflowRegistration();

      // The function should have been called
      expect(processorFn).toHaveBeenCalled();

      expect(mastra.listProcessors()?.['dynamic-output-processor']).toBe(outputProcessor);
      expect(() => mastra.getWorkflow('test-agent-output-fn-output-processor')).toThrow();
    });

    it('keeps repeated dynamic processor introspection side-effect free', async () => {
      let resolution = 0;
      const processorFn = vi.fn(() => {
        resolution += 1;
        return [
          {
            id: `dynamic-introspection-${resolution}`,
            processInput: async ({ messages }: any) => messages,
          },
        ];
      });
      const agent = new Agent({
        id: 'dynamic-introspection-agent',
        name: 'Dynamic Introspection Agent',
        instructions: 'Test',
        model: createMockModel(),
        inputProcessors: processorFn as unknown as () => InputProcessorOrWorkflow[],
      });
      const mastra = new Mastra({ logger: false, agents: { dynamic: agent } });
      await waitForWorkflowRegistration();
      const registeredProcessorIds = Object.keys(mastra.listProcessors() ?? {});
      const registeredConfigurationIds = [...mastra.listProcessorConfigurations().keys()];

      await agent.getConfiguredProcessorWorkflows();
      await agent.getConfiguredProcessorWorkflows();

      expect(processorFn).toHaveBeenCalledTimes(3);
      expect(Object.keys(mastra.listProcessors() ?? {})).toEqual(registeredProcessorIds);
      expect([...mastra.listProcessorConfigurations().keys()]).toEqual(registeredConfigurationIds);
      expect(registeredProcessorIds).toContain('dynamic-introspection-1');
      expect(registeredProcessorIds).not.toContain('dynamic-introspection-2');
      expect(registeredProcessorIds).not.toContain('dynamic-introspection-3');
    });
  });

  describe('Workflow processor registration', () => {
    it('should register a single workflow directly when passed as inputProcessor', async () => {
      const processorWorkflow = createWorkflow({
        id: 'custom-input-workflow',
        inputSchema: ProcessorStepSchema,
        outputSchema: ProcessorStepSchema,
      })
        .then({
          id: 'custom-step',
          inputSchema: ProcessorStepSchema,
          outputSchema: ProcessorStepSchema,
          execute: async ({ inputData }) => inputData,
        })
        .commit();

      const agent = new Agent({
        id: 'test-agent-workflow',
        name: 'Test Agent',
        instructions: 'Test',
        model: createMockModel(),
        inputProcessors: [processorWorkflow],
      });

      const mastra = new Mastra({
        logger: false,
        agents: { testAgent: agent },
      });

      // Wait for async registration
      await waitForWorkflowRegistration();

      // Should have registered the custom workflow directly (not wrapped)
      const workflow = mastra.getWorkflow('custom-input-workflow');
      expect(workflow).toBeDefined();
      expect(workflow.id).toBe('custom-input-workflow');
    });
  });

  describe('Multiple processors chaining', () => {
    it('should chain multiple processors into a single workflow', async () => {
      const processor1: InputProcessorOrWorkflow = {
        id: 'processor-1',
        processInput: async ({ messages }) => messages,
      };

      const processor2: InputProcessorOrWorkflow = {
        id: 'processor-2',
        processInput: async ({ messages }) => messages,
      };

      const processor3: InputProcessorOrWorkflow = {
        id: 'processor-3',
        processInput: async ({ messages }) => messages,
      };

      const agent = new Agent({
        id: 'test-agent-chain',
        name: 'Test Agent',
        instructions: 'Test',
        model: createMockModel(),
        inputProcessors: [processor1, processor2, processor3],
      });

      const mastra = new Mastra({
        logger: false,
        agents: { testAgent: agent },
      });

      // Wait for async registration
      await waitForWorkflowRegistration();

      // Should have registered a single combined workflow
      const workflow = mastra.getWorkflow('test-agent-chain-input-processor');
      expect(workflow).toBeDefined();

      // The workflow should contain all three processors as steps
      // We can verify this by checking the serializedStepFlow
      expect(workflow.serializedStepFlow).toBeDefined();
      expect(workflow.serializedStepFlow.length).toBe(3);
    });
  });

  describe('Error handling', () => {
    it('should handle errors gracefully when processor function throws', async () => {
      const errorFn = vi.fn().mockRejectedValue(new Error('Processor function error'));

      const agent = new Agent({
        id: 'test-agent-error',
        name: 'Test Agent',
        instructions: 'Test',
        model: createMockModel(),
        inputProcessors: errorFn as unknown as () => Promise<InputProcessorOrWorkflow[]>,
      });

      // Should not throw during construction
      const mastra = new Mastra({
        logger: false,
        agents: { testAgent: agent },
      });

      // Wait for async registration (which should fail silently)
      await waitForWorkflowRegistration();

      // Agent should still be registered
      expect(mastra.getAgent('testAgent')).toBe(agent);

      // But no workflow should be registered
      expect(() => mastra.getWorkflow('test-agent-error-input-processor')).toThrow();
    });
  });

  describe('Adding agents after construction', () => {
    it('should directly register a lone processor when an agent is added via addAgent', async () => {
      const mastra = new Mastra({
        logger: false,
      });

      const inputProcessor: InputProcessorOrWorkflow = {
        id: 'late-added-processor',
        processInput: async ({ messages }) => messages,
      };

      const agent = new Agent({
        id: 'late-added-agent',
        name: 'Late Added Agent',
        instructions: 'Test',
        model: createMockModel(),
        inputProcessors: [inputProcessor],
      });

      mastra.addAgent(agent);

      // Wait for async registration
      await waitForWorkflowRegistration();

      expect(mastra.listProcessors()?.['late-added-processor']).toBe(inputProcessor);
      expect(() => mastra.getWorkflow('late-added-agent-input-processor')).toThrow();
    });
  });
});
