import type { Mastra } from '@mastra/core/mastra';
import type { InngestFunction } from 'inngest';
import { isInngestAgent } from './durable-agent/create-inngest-agent';
import { InngestWorkflow } from './workflow';

export function collectInngestFunctions({
  mastra,
  functions: userFunctions = [],
}: {
  mastra: Mastra;
  functions?: InngestFunction.Like[];
}) {
  const workflows = [
    ...Object.values(mastra.listWorkflows()),
    // Durable backing workflows are hidden from the public workflow listing,
    // but their Inngest functions must still be served with the owning agents.
    // Workflow IDs are agent-scoped (createInngestDurableAgenticWorkflowIds),
    // so every inngest agent's functions must be served, not just one
    // representative agent. The Map below dedupes shared function IDs.
    ...Object.values(mastra.listAgents()).flatMap(agent => (isInngestAgent(agent) ? agent.getDurableWorkflows() : [])),
  ];
  const workflowFunctions = new Map<string, InngestFunction.Like>();

  for (const workflow of workflows) {
    if (!(workflow instanceof InngestWorkflow)) continue;

    workflow.__registerMastra(mastra);
    for (const fn of workflow.getFunctions()) {
      const functionId = fn.id();
      if (!workflowFunctions.has(functionId)) {
        workflowFunctions.set(functionId, fn);
      }
    }
  }

  return [...workflowFunctions.values(), ...userFunctions];
}
