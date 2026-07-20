import { describe, expect, it } from 'vitest';
import {
  outputProcessorsAllowTerminalToolResult,
  outputProcessorsOwnTerminalPersistence,
} from '../../loop/shared/terminal-tool-result';
import type { OutputProcessor, OutputProcessorOrWorkflow, ProcessorWorkflow } from '../../processors';
import { Agent } from '../agent';

function createAgent() {
  return new Agent({
    id: 'terminal-processor-policy-agent',
    name: 'terminal-processor-policy-agent',
    instructions: 'test',
    model: 'openai/gpt-5',
  });
}

function outputProcessor(id: string, passThrough: boolean, persistenceOwner = false): OutputProcessor {
  return {
    id,
    ...(passThrough ? { terminalToolResultPolicy: 'pass-through' as const } : {}),
    ...(persistenceOwner ? { terminalToolResultPersistence: 'owner' as const } : {}),
    processOutputResult: async ({ messageList }) => messageList,
  };
}

function combine(agent: Agent, processors: OutputProcessorOrWorkflow[]): OutputProcessorOrWorkflow[] {
  return (
    agent as unknown as {
      combineProcessorsIntoWorkflow: (
        processors: OutputProcessorOrWorkflow[],
        workflowId: string,
      ) => OutputProcessorOrWorkflow[];
    }
  ).combineProcessorsIntoWorkflow(processors, 'terminal-output-processors');
}

function prebuiltWorkflow(policy?: 'pass-through'): ProcessorWorkflow {
  return {
    id: 'prebuilt-output-processor',
    inputSchema: {},
    outputSchema: {},
    execute: async () => undefined,
    ...(policy ? { terminalToolResultPolicy: policy } : {}),
  } as unknown as ProcessorWorkflow;
}

describe('terminal tool-result output processor policy', () => {
  it('preserves pass-through when every combined processor explicitly opts in', () => {
    const combined = combine(createAgent(), [outputProcessor('first', true), outputProcessor('second', true)]);

    expect(combined).toHaveLength(1);
    expect(outputProcessorsAllowTerminalToolResult(combined)).toBe(true);
    expect((combined[0] as ProcessorWorkflow).terminalToolResultPolicy).toBe('pass-through');
  });

  it('fails closed when one processor in the combined workflow does not opt in', () => {
    const combined = combine(createAgent(), [outputProcessor('first', true), outputProcessor('second', false)]);

    expect(combined).toHaveLength(1);
    expect(outputProcessorsAllowTerminalToolResult(combined)).toBe(false);
    expect((combined[0] as ProcessorWorkflow).terminalToolResultPolicy).toBeUndefined();
  });

  it('honors the explicit policy on a single prebuilt processor workflow', () => {
    const allowed = combine(createAgent(), [prebuiltWorkflow('pass-through')]);
    const blocked = combine(createAgent(), [prebuiltWorkflow()]);

    expect(allowed[0]).toHaveProperty('type', 'processor');
    expect(outputProcessorsAllowTerminalToolResult(allowed)).toBe(true);
    expect(outputProcessorsAllowTerminalToolResult(blocked)).toBe(false);
  });

  it('accepts one final persistence owner and preserves it on the combined workflow', () => {
    const combined = combine(createAgent(), [
      outputProcessor('indexer', true),
      outputProcessor('message-history', true, true),
    ]);

    expect(outputProcessorsAllowTerminalToolResult(combined)).toBe(true);
    expect(outputProcessorsOwnTerminalPersistence(combined)).toBe(true);
    expect((combined[0] as ProcessorWorkflow).terminalToolResultPersistence).toBe('owner');
  });

  it('fails closed when persistence is not unique and final', () => {
    const ownerBeforeFallibleProcessor = combine(createAgent(), [
      outputProcessor('message-history', true, true),
      outputProcessor('later', true),
    ]);
    const twoOwners = combine(createAgent(), [
      outputProcessor('first-owner', true, true),
      outputProcessor('second-owner', true, true),
    ]);

    expect(outputProcessorsAllowTerminalToolResult(ownerBeforeFallibleProcessor)).toBe(false);
    expect(outputProcessorsOwnTerminalPersistence(ownerBeforeFallibleProcessor)).toBe(false);
    expect(outputProcessorsAllowTerminalToolResult(twoOwners)).toBe(false);
    expect(outputProcessorsOwnTerminalPersistence(twoOwners)).toBe(false);
  });
});
