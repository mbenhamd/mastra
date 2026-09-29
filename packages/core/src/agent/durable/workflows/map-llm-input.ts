import type { DurableAgenticWorkflowInput } from '../types';

/** Accumulated step records threaded to processor hooks (#24293 parity with the main loop). */
export type DurableAccumulatedStep = Record<string, unknown>;

export function mapDurableIterationToLLMInput(
  state: DurableAgenticWorkflowInput & {
    iterationCount?: number;
    accumulatedSteps?: DurableAccumulatedStep[];
  },
) {
  return {
    runId: state.runId,
    runtimeBindingId: state.runtimeBindingId,
    agentId: state.agentId,
    agentName: state.agentName,
    versions: state.versions,
    hasProcessors: state.hasProcessors,
    runtimeBindings: state.runtimeBindings,
    runtimeResolution: state.runtimeResolution,
    messageListState: state.messageListState,
    toolsMetadata: state.toolsMetadata,
    modelConfig: state.modelConfig,
    modelList: state.modelList,
    options: state.options,
    responseRecovery: state.responseRecovery,
    state: state.state,
    messageId: state.messageId,
    stepIndex: state.iterationCount ?? state.stepIndex,
    // Processor hooks receive the running step list (#24293) — the
    // llm-execution step reads this for stepNumber/steps parity with the
    // main loop.
    accumulatedSteps: state.accumulatedSteps,
    agentSpanData: state.agentSpanData,
    modelSpanData: state.modelSpanData,
    requestContextEntries: state.requestContextEntries,
    requiredRequestContextCapabilities: state.requiredRequestContextCapabilities,
  };
}
