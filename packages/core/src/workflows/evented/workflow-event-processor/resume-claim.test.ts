import { describe, expect, it, vi } from 'vitest';
import { z } from 'zod/v4';
import { EventEmitterPubSub } from '../../../events/event-emitter';
import { Mastra } from '../../../mastra';
import { MockStore } from '../../../storage/mock';
import { createEmptyWorkflowSnapshot } from '../../../storage/workflow-snapshot';
import { createWorkflow } from '../../evented';
import { createStep } from '../../index';
import type { ProcessorArgs } from './index';
import { WorkflowEventProcessor } from './index';

type Competitor = 'canceled' | 'success' | 'new-generation' | 'increased-resume-attempt';

class ExposedProcessor extends WorkflowEventProcessor {
  start(args: ProcessorArgs) {
    return this.processWorkflowStart(args);
  }
}

function makeWorkflow(id: string, persistRunning: boolean) {
  const step = createStep({
    id: 'resume-claim-step',
    inputSchema: z.object({}),
    outputSchema: z.object({}),
    execute: async () => ({}),
  });
  return createWorkflow({
    id,
    inputSchema: z.object({}),
    outputSchema: z.object({}),
    options: {
      shouldPersistSnapshot: ({ workflowStatus }) => persistRunning || workflowStatus !== 'running',
    },
  })
    .then(step)
    .commit();
}

function competitorOptions(kind: Competitor, generation: string) {
  if (kind === 'new-generation') {
    return { status: 'running' as const, executionGeneration: `${generation}-replacement`, lifecycleResumeAttempt: 1 };
  }
  if (kind === 'increased-resume-attempt') {
    return { status: 'running' as const, executionGeneration: generation, lifecycleResumeAttempt: 2 };
  }
  return { status: kind, executionGeneration: generation, lifecycleResumeAttempt: 1 };
}

describe.each([true, false] as const)('evented resume claim with running persistence %s', persistRunning => {
  describe.each(['canceled', 'success', 'new-generation', 'increased-resume-attempt'] as const)(
    'competing %s transition',
    competitor => {
      it('keeps the competing winner before initialization and dispatch', async () => {
        const storage = new MockStore();
        const pubsub = new EventEmitterPubSub();
        const workflow = makeWorkflow(`resume-claim-${persistRunning}-${competitor}`, persistRunning);
        const mastra = new Mastra({ logger: false, storage, pubsub, workflows: { [workflow.id]: workflow } });
        const runId = `resume-claim-${persistRunning}-${competitor}-run`;
        const generation = `resume-generation-${persistRunning}-${competitor}`;
        const workflowsStore = (await storage.getStore('workflows'))!;
        await workflowsStore.persistWorkflowSnapshot({
          workflowName: workflow.id,
          runId,
          snapshot: {
            ...createEmptyWorkflowSnapshot(runId),
            status: 'running',
            executionGeneration: generation,
            lifecycleResumeAttempt: 1,
            serializedStepGraph: workflow.serializedStepGraph,
            suspendedPaths: { 'resume-claim-step': [0] },
            context: { 'resume-claim-step': { status: 'suspended', payload: {}, startedAt: 0, suspendedAt: 1 } },
          },
        });
        const input = {
          // ProcessorArgs defaults to the default engine although this shared
          // processor receives evented workflow definitions at runtime.
          workflow: workflow as ProcessorArgs['workflow'],
          workflowId: workflow.id,
          runId,
          executionGeneration: generation,
          lifecycleResumeAttempt: 1,
          lifecycleStepStates: {},
          lifecycleStartKind: 'resume' as const,
          executionPath: [0],
          stepResults: {},
          activeStepsPath: {},
          resumeSteps: ['resume-claim-step'],
          prevResult: { status: 'success' as const, output: {}, payload: {}, startedAt: 0, endedAt: 1 },
          requestContext: {},
          resumeData: {},
          state: {},
        } satisfies ProcessorArgs;
        const originalUpdate = workflowsStore.updateWorkflowState.bind(workflowsStore);
        let raced = false;
        const update = vi.spyOn(workflowsStore, 'updateWorkflowState').mockImplementation(async args => {
          if (!raced && args.opts.expectedStatus === 'running') {
            raced = true;
            await originalUpdate({ ...args, opts: competitorOptions(competitor, generation) });
          }
          return originalUpdate(args);
        });
        const persist = vi.spyOn(workflowsStore, 'persistWorkflowSnapshot');
        const publish = vi.spyOn(pubsub, 'publish');
        const processor = new ExposedProcessor({ mastra, topicCleanupDelayMs: 0 });

        try {
          await processor.start(input);
          const final = await workflowsStore.loadWorkflowSnapshot({ workflowName: workflow.id, runId });
          expect(raced).toBe(true);
          expect(final).toMatchObject({
            status:
              competitor === 'new-generation' || competitor === 'increased-resume-attempt' ? 'running' : competitor,
            executionGeneration: competitor === 'new-generation' ? `${generation}-replacement` : generation,
            lifecycleResumeAttempt: competitor === 'increased-resume-attempt' ? 2 : 1,
          });
          expect(persist).not.toHaveBeenCalled();
          expect(publish.mock.calls.some(([, event]) => event.type === 'workflow.step.run')).toBe(false);
          expect(
            publish.mock.calls.some(
              ([, event]) => event.type === 'workflow.lifecycle' && event.data?.event?.type === 'workflow.resumed',
            ),
          ).toBe(false);
        } finally {
          publish.mockRestore();
          persist.mockRestore();
          update.mockRestore();
          await mastra.shutdown();
        }
      });
    },
  );
});
