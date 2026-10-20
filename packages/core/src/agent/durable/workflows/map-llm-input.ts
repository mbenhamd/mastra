import type { DurableAgenticWorkflowInput } from '../types';

/** Accumulated step records threaded to processor hooks (#24293 parity with the main loop). */
export type DurableAccumulatedStep = Record<string, unknown>;

export function mapDurableIterationToLLMInput(
  // Upstream #26221 moved the transcript into workflow state, so iteration
  // state may carry no messageListState; the llm step reads it from state.
  state: Omit<DurableAgenticWorkflowInput, 'messageListState'> & {
    messageListState?: DurableAgenticWorkflowInput['messageListState'];
    iterationCount?: number;
    accumulatedSteps?: DurableAccumulatedStep[];
    initialUntaggedSystemMessages?: unknown[];
    lastStepResult?: { signalPreempted?: boolean };
    completionStartTime?: Date;
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
    initialUntaggedSystemMessages: state.initialUntaggedSystemMessages,
    toolsMetadata: state.toolsMetadata,
    modelConfig: state.modelConfig,
    modelList: state.modelList,
    options: state.options,
    responseRecovery: state.responseRecovery,
    recoveryToolCeiling: state.recoveryToolCeiling,
    state: state.state,
    messageId: state.messageId,
    stepIndex: state.iterationCount ?? state.stepIndex,
    signalPreempted: state.lastStepResult?.signalPreempted,
    // Processor hooks receive the running step list (#24293) — the
    // llm-execution step reads this for stepNumber/steps parity with the
    // main loop.
    accumulatedSteps: state.accumulatedSteps,
    agentSpanData: state.agentSpanData,
    modelSpanData: state.modelSpanData,
    completionStartTime: state.completionStartTime,
    requestContextEntries: state.requestContextEntries,
    requiredRequestContextCapabilities: state.requiredRequestContextCapabilities,
  };
}
