import { describe, expect, it } from 'vitest';
import { z } from 'zod/v4';
import { createWorkflow } from '../create';
import { createStep } from '../workflow';

describe('workflow terminal graph fingerprint builder compatibility', () => {
  it('accepts condition identities emitted by native branch and loop builders', async () => {
    const { createWorkflowTerminalGraphFingerprint } = await import('./graph-fingerprint');
    const schema = z.object({ value: z.number() });
    const branchStep = createStep({
      id: 'native-branch',
      inputSchema: schema,
      outputSchema: schema,
      execute: async ({ inputData }) => inputData,
    });
    const loopStep = createStep({
      id: 'native-loop',
      inputSchema: schema,
      outputSchema: schema,
      execute: async ({ inputData }) => inputData,
    });
    const branchWorkflow = createWorkflow({ id: 'native-branch-workflow', inputSchema: schema, outputSchema: schema })
      .branch([[async () => true, branchStep]])
      .commit();
    const dowhileWorkflow = createWorkflow({ id: 'native-dowhile-workflow', inputSchema: schema, outputSchema: schema })
      .dowhile(loopStep, async () => false)
      .commit();
    const dountilWorkflow = createWorkflow({ id: 'native-dountil-workflow', inputSchema: schema, outputSchema: schema })
      .dountil(loopStep, async () => true)
      .commit();

    for (const workflow of [branchWorkflow, dowhileWorkflow, dountilWorkflow]) {
      const liveFingerprint = createWorkflowTerminalGraphFingerprint(workflow.serializedStepFlow);
      const persistedGraph = JSON.parse(JSON.stringify(workflow.serializedStepFlow));
      expect(createWorkflowTerminalGraphFingerprint(persistedGraph)).toBe(liveFingerprint);
    }
  });
});
