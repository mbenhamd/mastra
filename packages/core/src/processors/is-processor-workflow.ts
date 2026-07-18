import type { Processor, ProcessorWorkflow, ProcessorWorkflowPhase } from './index';

const allProcessorWorkflowPhases = [
  'input',
  'inputStep',
  'outputStream',
  'outputResult',
  'outputStep',
] as const satisfies readonly ProcessorWorkflowPhase[];

const declaredProcessorWorkflowPhases = new WeakMap<object, readonly ProcessorWorkflowPhase[]>();

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
    return declaredProcessorWorkflowPhases.get(processorOrWorkflow) ?? allProcessorWorkflowPhases;
  }

  const phases: ProcessorWorkflowPhase[] = [];
  if (processorOrWorkflow.processInput) phases.push('input');
  if (processorOrWorkflow.processInputStep || processorOrWorkflow.computeStateSignal) phases.push('inputStep');
  if (processorOrWorkflow.processOutputStream) phases.push('outputStream');
  if (processorOrWorkflow.processOutputResult) phases.push('outputResult');
  if (processorOrWorkflow.processOutputStep) phases.push('outputStep');
  return phases;
}

/**
 * Declare exactly which processor phases a workflow consumes.
 *
 * Unannotated workflows remain compatible with every phase. Annotating a
 * workflow lets the Agent avoid admitting it for phases that cannot affect its
 * output. The same workflow instance is returned for convenient composition.
 */
export function setProcessorWorkflowPhases<TWorkflow extends ProcessorWorkflow>(
  workflow: TWorkflow,
  phases: readonly ProcessorWorkflowPhase[],
): TWorkflow {
  declaredProcessorWorkflowPhases.set(workflow, Object.freeze([...new Set(phases)]));
  return workflow;
}

/** @internal Preserve phase capabilities when a workflow is cloned. */
export function copyProcessorWorkflowPhases<TWorkflow extends object>(source: object, target: TWorkflow): TWorkflow {
  const declaredPhases = declaredProcessorWorkflowPhases.get(source);
  if (declaredPhases) {
    declaredProcessorWorkflowPhases.set(target, declaredPhases);
  }
  return target;
}

/** Preserve unannotated workflow compatibility while honoring declared capabilities. */
export function processorWorkflowSupportsPhase(workflow: ProcessorWorkflow, phase: ProcessorWorkflowPhase): boolean {
  return declaredProcessorWorkflowPhases.get(workflow)?.includes(phase) ?? true;
}

/** @internal Whether an annotation excludes at least one processor phase. */
export function processorWorkflowHasPhaseRestrictions(workflow: ProcessorWorkflow): boolean {
  const declaredPhases = declaredProcessorWorkflowPhases.get(workflow);
  return Boolean(declaredPhases && allProcessorWorkflowPhases.some(phase => !declaredPhases.includes(phase)));
}

/** Evented workflows and wrappers containing them require durable parent execution. */
export function processorWorkflowRequiresDurableExecution(workflow: ProcessorWorkflow): boolean {
  const visited = new Set<object>();

  const visit = (candidate: unknown): boolean => {
    if (!candidate || typeof candidate !== 'object' || visited.has(candidate)) {
      return false;
    }
    visited.add(candidate);

    const record = candidate as { engineType?: unknown; steps?: unknown };
    if (record.engineType === 'evented') {
      return true;
    }
    if (!record.steps || typeof record.steps !== 'object') {
      return false;
    }
    return Object.values(record.steps).some(visit);
  };

  return visit(workflow);
}
