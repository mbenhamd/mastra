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
    ...Object.values(mastra.listAgents()).flatMap(agent => (isInngestAgent(agent) ? agent.getDurableWorkflows() : [])),
  ];
  const workflowFunctions = Array.from(
    new Set(
      workflows.flatMap(workflow => {
        if (workflow instanceof InngestWorkflow) {
          workflow.__registerMastra(mastra);
          return workflow.getFunctions();
        }
        return [];
      }),
    ),
  );

  return [...workflowFunctions, ...userFunctions];
}
