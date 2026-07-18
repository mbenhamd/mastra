import { describe, expect, it } from 'vitest';
import {
  getProcessorWorkflowPhases,
  processorWorkflowHasPhaseRestrictions,
  processorWorkflowSupportsPhase,
  setProcessorWorkflowPhases,
} from './is-processor-workflow';
import type { ProcessorWorkflow } from './index';

function processorWorkflowStub(): ProcessorWorkflow {
  return {
    id: 'processor-workflow',
    inputSchema: {},
    outputSchema: {},
    execute: () => undefined,
  } as unknown as ProcessorWorkflow;
}

describe('processor workflow phase capabilities', () => {
  it('keeps unannotated workflows compatible with every phase', () => {
    const workflow = processorWorkflowStub();

    expect(getProcessorWorkflowPhases(workflow)).toEqual([
      'input',
      'inputStep',
      'outputStream',
      'outputResult',
      'outputStep',
    ]);
    expect(processorWorkflowHasPhaseRestrictions(workflow)).toBe(false);
    expect(processorWorkflowSupportsPhase(workflow, 'outputStream')).toBe(true);
  });

  it('exposes a supported annotation for custom and nested processor workflows', () => {
    const workflow = processorWorkflowStub();

    expect(setProcessorWorkflowPhases(workflow, ['outputResult', 'outputResult'])).toBe(workflow);
    expect(getProcessorWorkflowPhases(workflow)).toEqual(['outputResult']);
    expect(processorWorkflowHasPhaseRestrictions(workflow)).toBe(true);
    expect(processorWorkflowSupportsPhase(workflow, 'outputResult')).toBe(true);
    expect(processorWorkflowSupportsPhase(workflow, 'outputStream')).toBe(false);
  });

  it('does not treat an explicit all-phase annotation as restrictive', () => {
    const workflow = processorWorkflowStub();

    setProcessorWorkflowPhases(workflow, ['input', 'inputStep', 'outputStream', 'outputResult', 'outputStep']);

    expect(processorWorkflowHasPhaseRestrictions(workflow)).toBe(false);
  });
});
