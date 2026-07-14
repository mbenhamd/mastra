import type { DurableAgenticWorkflowInput } from '../types';

export function mapDurableIterationToLLMInput(state: DurableAgenticWorkflowInput) {
  return {
    runId: state.runId,
    agentId: state.agentId,
    agentName: state.agentName,
    runtimeResolution: state.runtimeResolution,
    messageListState: state.messageListState,
    toolsMetadata: state.toolsMetadata,
    modelConfig: state.modelConfig,
    modelList: state.modelList,
    options: state.options,
    state: state.state,
    messageId: state.messageId,
  };
}
