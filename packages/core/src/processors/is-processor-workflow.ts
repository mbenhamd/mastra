import type { Processor, ProcessorWorkflow, ProcessorWorkflowPhase } from './index';

const allProcessorWorkflowPhases = [
  'input',
  'inputStep',
  'outputStream',
  'outputResult',
  'outputStep',
] as const satisfies readonly ProcessorWorkflowPhase[];

/**
 * Type guard to check if an object is a Workflow that can be used as a processor.
 *
 * Extracted to its own module so that `runner.ts` (and by extension
 * `stream/base/output.ts`) can use it without loading the full processors
 * barrel — which re-exports every built-in processor, many of which import
 * from the agent barrel and create ESM init-time cycles.
 */
export function isProcessorWorkflow(obj: unknown): obj is ProcessorWorkflow {
  return (
    obj !== null &&
    typeof obj === 'object' &&
    'id' in obj &&
    typeof (obj as Record<string, unknown>).id === 'string' &&
    'inputSchema' in obj &&
    'outputSchema' in obj &&
    'execute' in obj &&
    typeof (obj as Record<string, unknown>).execute === 'function' &&
    !('processInput' in obj) &&
    !('processInputStep' in obj) &&
    !('processOutputStream' in obj) &&
    !('processOutputResult' in obj) &&
    !('processOutputStep' in obj) &&
    !('processLLMRequest' in obj) &&
    !('processAPIError' in obj)
  );
}

/** Return the phases a processor or explicitly annotated processor workflow can consume. */
export function getProcessorWorkflowPhases(
  processorOrWorkflow: Processor | ProcessorWorkflow,
): readonly ProcessorWorkflowPhase[] {
  if (isProcessorWorkflow(processorOrWorkflow)) {
    return processorOrWorkflow.__processorPhases ?? allProcessorWorkflowPhases;
  }

  const phases: ProcessorWorkflowPhase[] = [];
  if (processorOrWorkflow.processInput) phases.push('input');
  if (processorOrWorkflow.processInputStep || processorOrWorkflow.computeStateSignal) phases.push('inputStep');
  if (processorOrWorkflow.processOutputStream) phases.push('outputStream');
  if (processorOrWorkflow.processOutputResult) phases.push('outputResult');
  if (processorOrWorkflow.processOutputStep) phases.push('outputStep');
  return phases;
}

/** Preserve arbitrary workflow compatibility while honoring explicit combined-workflow capabilities. */
export function processorWorkflowSupportsPhase(workflow: ProcessorWorkflow, phase: ProcessorWorkflowPhase): boolean {
  return workflow.__processorPhases?.includes(phase) ?? true;
}
