import { describe, expect, it, vi } from 'vitest';
import { z } from 'zod/v4';
import { createStep, createWorkflow } from '..';
import { EventEmitterPubSub } from '../../../events/event-emitter';
import type { Event } from '../../../events/types';
import { Mastra } from '../../../mastra';
import { MockStore } from '../../../storage/mock';
import { createEmptyWorkflowSnapshot } from '../../../storage/workflow-snapshot';
import { isQueuedForeachIteration } from './loop';

describe('foreach stale partial writes', () => {
  it.each(['workflow.start', 'workflow.step.end'] as const)(
    '%s does not restore a queued sibling after it has been claimed',
    async type => {
      const child = createWorkflow({ id: 'child', inputSchema: z.number(), outputSchema: z.number() })
        .then(
          createStep({
            id: 'identity',
            inputSchema: z.number(),
            outputSchema: z.number(),
            execute: async ({ inputData }) => inputData,
          }),
        )
        .commit();
      const parent = createWorkflow({
        id: 'parent',
        inputSchema: z.array(z.number()),
        outputSchema: z.array(z.number()),
      })
        .foreach(child, { concurrency: 2 })
        .commit();
      const storage = new MockStore();
      const pubsub = new EventEmitterPubSub();
      const mastra = new Mastra({ logger: false, storage, pubsub, workflows: { parent, child } });
      const store = await storage.getStore('workflows');
      const snapshot = createEmptyWorkflowSnapshot('parent-run');
      snapshot.status = 'running';
      // PF-4387 (PF-6640 sync): the fork drops a workflow event without an execution
      // generation and fences writes to the generation that owns the run.
      snapshot.executionGeneration = 'parent-generation';
      // PF-4390: durable nested admission binds the child to its parent's graph coordinate.
      snapshot.serializedStepGraph = parent.serializedStepGraph;
      snapshot.context.child = {
        status: 'running',
        payload: [1, 2],
        output: [null, { __mastra_foreach_queued__: true, nestedRunId: 'child-1' }],
        metadata: { foreachNestedRunIds: { 0: 'child-0', 1: 'child-1' } },
      };
      await store!.persistWorkflowSnapshot({ workflowName: 'parent', runId: 'parent-run', snapshot });
      const load = store!.loadWorkflowSnapshot.bind(store);
      const writes = vi.spyOn(store!, 'updateWorkflowResults');
      let raced = false;
      let parentReads = 0;
      vi.spyOn(store!, 'loadWorkflowSnapshot').mockImplementation(async args => {
        const stale = structuredClone(await load(args));
        if (args.workflowName === 'parent') parentReads++;
        if (args.workflowName === 'parent' && parentReads === (type === 'workflow.start' ? 1 : 2) && !raced) {
          raced = true;
          await store!.updateWorkflowResults({
            workflowName: 'parent',
            runId: 'parent-run',
            stepId: 'child',
            result: { ...snapshot.context.child, output: [null, { __mastra_pending__: true }] },
            requestContext: {},
          });
        }
        return stale;
      });
      try {
        const result = await mastra.handleWorkflowEvent({
          type,
          runId: type === 'workflow.start' ? 'child-0' : 'parent-run',
          data: {
            workflowId: type === 'workflow.start' ? 'child' : 'parent',
            executionGeneration: type === 'workflow.start' ? 'child-generation' : 'parent-generation',
            runId: type === 'workflow.start' ? 'child-0' : 'parent-run',
            executionPath: type === 'workflow.start' ? [0] : [0, 0],
            stepResults: {},
            activeStepsPath: {},
            requestContext: {},
            prevResult: { status: 'success', output: 1 },
            ...(type === 'workflow.start'
              ? {
                  parentWorkflow: {
                    workflowId: 'parent',
                    runId: 'parent-run',
                    executionGeneration: 'parent-generation',
                    // Fork terminal recovery records the parent step a nested run serves.
                    stepId: 'child',
                    executionPath: [0, 0],
                    stepGraph: parent.stepGraph,
                    input: { output: 1 },
                  },
                }
              : {}),
          },
        } as Event);
        expect(result).toEqual({ ok: true });
        expect(raced).toBe(true);
        for (const [write] of writes.mock.calls) {
          if (write.workflowName === 'parent') {
            expect(write.result.output?.some(isQueuedForeachIteration)).not.toBe(true);
          }
        }
        const persisted = await load({ workflowName: 'parent', runId: 'parent-run' });
        expect(persisted?.context.child.output).toEqual(type === 'workflow.start' ? [null, null] : [1, null]);
        expect(persisted?.context.child.metadata.foreachNestedRunIds).toEqual({ 0: 'child-0', 1: 'child-1' });
      } finally {
        vi.restoreAllMocks();
        await mastra.shutdown();
      }
    },
  );
});
