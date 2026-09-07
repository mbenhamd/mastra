import { describe, expect, it, vi } from 'vitest';
import { z } from 'zod/v4';
import { EventEmitterPubSub } from '../../../events/event-emitter';
import { Mastra } from '../../../mastra';
import { MockStore } from '../../../storage/mock';
import { createWorkflow } from '../../evented';
import { createStep } from '../../index';
import { getOrCreateWorkflowStepLifecycleState } from '../../lifecycle-events';
import type { ProcessorArgs } from './index';
import { WorkflowEventProcessor } from './index';

type TerminalStatus = 'success' | 'failed' | 'canceled';
type Competitor = TerminalStatus | 'new-generation' | 'increased-resume-attempt';

class ExposedProcessor extends WorkflowEventProcessor {
  finish(args: ProcessorArgs, status: TerminalStatus) {
    return this.endWorkflow(args, status);
  }
  fail(args: ProcessorArgs) {
    return this.processWorkflowFail(args);
  }
  cancel(args: ProcessorArgs) {
    return this.processWorkflowCancel(args);
  }
}

function makeWorkflow(id: string) {
  const step = createStep({
    id: 'conditional-terminal-step',
    inputSchema: z.object({}),
    outputSchema: z.object({}),
    execute: async () => ({}),
  });
  return createWorkflow({ id, inputSchema: z.object({}), outputSchema: z.object({}) })
    .then(step)
    .commit();
}

function makeArgs(
  workflow: ReturnType<typeof makeWorkflow>,
  runId: string,
  generation: string,
  status: TerminalStatus,
) {
  const processorWorkflow = workflow as ProcessorArgs['workflow'];
  return {
    workflow: processorWorkflow,
    workflowId: workflow.id,
    runId,
    executionGeneration: generation,
    lifecycleResumeAttempt: 0,
    lifecycleStepStates: {},
    executionPath: [],
    stepResults: {},
    activeStepsPath: {},
    resumeSteps: [],
    // Processor cancellation messages carry a run-level canceled status,
    // although the shared StepResult type does not include that discriminator.
    prevResult: {
      status,
      payload: {},
      output: {},
      startedAt: 0,
      endedAt: 1,
      ...(status === 'failed' ? { error: { name: 'Error', message: 'failed' } } : {}),
    } as ProcessorArgs['prevResult'],
    requestContext: {},
  } satisfies ProcessorArgs;
}

function competitorOptions(kind: Competitor, generation: string) {
  if (kind === 'new-generation')
    return { status: 'pending' as const, executionGeneration: `${generation}-replacement` };
  if (kind === 'increased-resume-attempt')
    return { status: 'pending' as const, executionGeneration: generation, lifecycleResumeAttempt: 1 };
  return { status: kind, executionGeneration: generation, lifecycleResumeAttempt: 0 };
}

async function setup(status: TerminalStatus) {
  const storage = new MockStore();
  const pubsub = new EventEmitterPubSub();
  const workflow = makeWorkflow(`conditional-terminal-${status}-${Math.random().toString(36).slice(2)}`);
  const mastra = new Mastra({ logger: false, storage, pubsub, workflows: { [workflow.id]: workflow } });
  const runId = `run-${status}`;
  await workflow.createRun({ runId });
  const workflows = (await storage.getStore('workflows'))!;
  const pending = (await workflows.loadWorkflowSnapshot({ workflowName: workflow.id, runId }))!;
  const generation = pending.executionGeneration!;
  await workflows.updateWorkflowState({
    workflowName: workflow.id,
    runId,
    opts: { status: 'running', executionGeneration: generation, lifecycleResumeAttempt: 0 },
  });
  return { pubsub, workflow, mastra, runId, generation, workflows };
}

describe.each(['success', 'failed', 'canceled'] as const)('evented terminal writer: %s', writer => {
  describe.each(['success', 'failed', 'new-generation', 'increased-resume-attempt'] as const)(
    'competing %s transition',
    competitor => {
      it('keeps the raced snapshot and emits no terminal effects', async () => {
        const fixture = await setup(writer);
        const input = makeArgs(fixture.workflow, fixture.runId, fixture.generation, writer);
        if (writer === 'canceled') {
          const { state } = getOrCreateWorkflowStepLifecycleState({
            workflowId: fixture.workflow.id,
            runId: fixture.runId,
            executionGeneration: fixture.generation,
            stepId: 'conditional-terminal-step',
            executionPath: [0],
            states: input.lifecycleStepStates,
          });
          state.stepAttempt = 1;
          input.activeStepsPath = { 'conditional-terminal-step': [0] };
        }
        const published: unknown[] = [];
        const publish = vi.spyOn(fixture.pubsub, 'publish').mockImplementation(async (_topic, event) => {
          published.push(event);
        });
        const originalUpdate = fixture.workflows.updateWorkflowState.bind(fixture.workflows);
        let raced = false;
        const update = vi.spyOn(fixture.workflows, 'updateWorkflowState').mockImplementation(async args => {
          if (!raced) {
            raced = true;
            await originalUpdate({ ...args, opts: competitorOptions(competitor, fixture.generation) });
          }
          return originalUpdate(args);
        });
        const processor = new ExposedProcessor({ mastra: fixture.mastra, topicCleanupDelayMs: 0 });
        try {
          if (writer === 'failed') await processor.fail(input);
          else if (writer === 'canceled') await processor.cancel(input);
          else await processor.finish(input, writer);
          const final = await fixture.workflows.loadWorkflowSnapshot({
            workflowName: fixture.workflow.id,
            runId: fixture.runId,
          });
          expect(raced).toBe(true);
          expect(final).toMatchObject({
            status:
              competitor === 'new-generation' || competitor === 'increased-resume-attempt' ? 'pending' : competitor,
            executionGeneration:
              competitor === 'new-generation' ? `${fixture.generation}-replacement` : fixture.generation,
            ...(competitor === 'increased-resume-attempt' ? { lifecycleResumeAttempt: 1 } : {}),
          });
          expect(published).toHaveLength(0);
          expect(
            await fixture.workflows.loadWorkflowSnapshot({ workflowName: fixture.workflow.id, runId: fixture.runId }),
          ).toEqual(final);
        } finally {
          update.mockRestore();
          publish.mockRestore();
          await fixture.mastra.shutdown();
        }
      });
    },
  );
});

describe.each(['success', 'failed', 'canceled'] as const)('duplicate evented terminal writer: %s', status => {
  it('publishes the terminal sequence once', async () => {
    const fixture = await setup(status);
    const input = makeArgs(fixture.workflow, fixture.runId, fixture.generation, status);
    if (status === 'canceled') {
      const { state } = getOrCreateWorkflowStepLifecycleState({
        workflowId: fixture.workflow.id,
        runId: fixture.runId,
        executionGeneration: fixture.generation,
        stepId: 'conditional-terminal-step',
        executionPath: [0],
        states: input.lifecycleStepStates,
      });
      state.stepAttempt = 1;
      input.activeStepsPath = { 'conditional-terminal-step': [0] };
      await fixture.workflows.updateWorkflowState({
        workflowName: fixture.workflow.id,
        runId: fixture.runId,
        opts: { activeStepsPath: input.activeStepsPath, lifecycleStepStates: input.lifecycleStepStates },
      });
    }
    const published: Array<{ type?: string; data?: { event?: { type?: string } } }> = [];
    const publish = vi.spyOn(fixture.pubsub, 'publish').mockImplementation(async (_topic, event) => {
      published.push(event);
    });
    const processor = new ExposedProcessor({ mastra: fixture.mastra, topicCleanupDelayMs: 0 });
    try {
      if (status === 'failed') {
        await processor.fail(input);
      } else if (status === 'canceled') {
        await processor.cancel(input);
      } else {
        await processor.finish(input, status);
      }
      const afterFirst = published.map(event => JSON.stringify(event));
      if (status === 'failed') await processor.fail(input);
      else if (status === 'success') await processor.finish(input, status);
      else await processor.cancel(input);
      expect(published.map(event => JSON.stringify(event))).toEqual(afterFirst);
      const lifecycleTypes = published
        .filter(event => event.type === 'workflow.lifecycle')
        .map(event => event.data?.event?.type);
      expect(lifecycleTypes).toEqual(
        status === 'success'
          ? ['workflow.finished']
          : status === 'failed'
            ? ['workflow.failed', 'workflow.finished']
            : ['step.canceled', 'step.finished', 'workflow.canceled', 'workflow.finished'],
      );
    } finally {
      publish.mockRestore();
      await fixture.mastra.shutdown();
    }
  });
});

it('rejects a terminal write against a generationless persisted run', async () => {
  const fixture = await setup('success');
  const input = makeArgs(fixture.workflow, fixture.runId, fixture.generation, 'success');
  const persisted = (await fixture.workflows.loadWorkflowSnapshot({
    workflowName: fixture.workflow.id,
    runId: fixture.runId,
  }))!;
  await fixture.workflows.persistWorkflowSnapshot({
    workflowName: fixture.workflow.id,
    runId: fixture.runId,
    snapshot: { ...persisted, executionGeneration: undefined },
  });
  const processor = new ExposedProcessor({ mastra: fixture.mastra, topicCleanupDelayMs: 0 });
  try {
    await processor.finish(input, 'success');
    await expect(
      fixture.workflows.loadWorkflowSnapshot({ workflowName: fixture.workflow.id, runId: fixture.runId }),
    ).resolves.toMatchObject({ status: 'running', executionGeneration: undefined });
  } finally {
    await fixture.mastra.shutdown();
  }
});
