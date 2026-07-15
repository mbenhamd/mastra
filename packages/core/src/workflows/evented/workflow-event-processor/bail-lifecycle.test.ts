import { describe, expect, it, vi } from 'vitest';
import { EventEmitterPubSub } from '../../../events/event-emitter';
import { Mastra } from '../../../mastra';
import { emitStepResultEvents } from '../../handlers/step';
import { WorkflowEventProcessor } from '.';

function lifecycleTypes(events: any[]) {
  return events
    .filter(event => event.type === 'workflow.lifecycle')
    .map(event => ({ type: event.data.event.type, status: event.data.event.status }));
}

describe('workflow bail lifecycle projection', () => {
  it('closes a default-engine bailed step as successful lifecycle completion', async () => {
    const pubsub = new EventEmitterPubSub();
    const published: any[] = [];
    vi.spyOn(pubsub, 'publish').mockImplementation(async (_topic, event) => {
      published.push(event);
    });

    await emitStepResultEvents({
      stepId: 'bailed-step',
      stepCallId: 'bailed-call',
      stepAttempt: 1,
      workflowId: 'workflow',
      executionGeneration: 'default-bail-generation',
      execResults: { status: 'bailed', output: { early: true } } as any,
      pubsub,
      runId: 'default-bail-run',
    });

    expect(lifecycleTypes(published)).toEqual([
      { type: 'step.completed', status: undefined },
      { type: 'step.finished', status: 'success' },
    ]);
    expect(published.find(event => event.data?.event?.type === 'step.completed')?.data.event.output).toEqual({
      early: true,
    });
  });

  it('closes an evented-engine bailed step before successful workflow completion', async () => {
    const pubsub = new EventEmitterPubSub();
    const mastra = new Mastra({ logger: false, pubsub, workflows: {} as any });
    const published: any[] = [];
    vi.spyOn(pubsub, 'publish').mockImplementation(async (_topic, event) => {
      published.push(event);
    });

    class ExposedProcessor extends WorkflowEventProcessor {
      runStep(args: any) {
        return this.processWorkflowStepRun(args);
      }
    }

    const processor = new ExposedProcessor({
      mastra,
      topicCleanupDelayMs: 0,
      stepExecutionStrategy: {
        executeStep: vi.fn(async () => ({ status: 'bailed', output: { early: true } })),
      } as any,
    });
    await processor.runStep({
      workflow: {
        id: 'workflow',
        stepGraph: [{ type: 'step', step: { id: 'bailed-step' } }],
        retryConfig: { attempts: 0 },
        options: { validateInputs: false },
      },
      workflowId: 'workflow',
      runId: 'evented-bail-run',
      executionGeneration: 'evented-bail-generation',
      lifecycleResumeAttempt: 0,
      lifecycleStepStates: {},
      executionPath: [0],
      stepResults: {},
      activeStepsPath: {},
      resumeSteps: [],
      prevResult: { status: 'success', output: {} },
      requestContext: {},
    });

    expect(lifecycleTypes(published)).toEqual([
      { type: 'step.started', status: undefined },
      { type: 'step.completed', status: undefined },
      { type: 'step.finished', status: 'success' },
      { type: 'workflow.finished', status: 'success' },
    ]);
    await mastra.shutdown();
  });
});
