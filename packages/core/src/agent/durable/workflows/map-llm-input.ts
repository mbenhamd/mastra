import type { DurableAgenticWorkflowInput } from '../types';

export function mapDurableIterationToLLMInput(
  state: DurableAgenticWorkflowInput & { iterationCount?: number },
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
    state: state.state,
    messageId: state.messageId,
    stepIndex: state.iterationCount ?? state.stepIndex,
    agentSpanData: state.agentSpanData,
    modelSpanData: state.modelSpanData,
  };
}
