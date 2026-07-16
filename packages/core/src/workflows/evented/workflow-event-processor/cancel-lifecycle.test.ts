import { describe, expect, it, vi } from 'vitest';
import { EventEmitterPubSub } from '../../../events/event-emitter';
import { Mastra } from '../../../mastra';
import { MockStore } from '../../../storage/mock';
import { createEmptyWorkflowSnapshot } from '../../../storage/workflow-snapshot';
import { getOrCreateWorkflowStepLifecycleState } from '../../lifecycle-events';
import { WorkflowEventProcessor } from '.';

class ExposedProcessor extends WorkflowEventProcessor {
  cancel(args: any) {
    return this.processWorkflowCancel(args);
  }
}

describe('evented cancellation lifecycle projection', () => {
  it('closes a durably active step before finishing the workflow on another worker', async () => {
    const pubsub = new EventEmitterPubSub();
    const storage = new MockStore();
    const mastra = new Mastra({ logger: false, storage, pubsub, workflows: {} as any });
    const executionGeneration = 'distributed-cancel-generation';
    const lifecycleStepStates = {};
    const { state: activeIdentity } = getOrCreateWorkflowStepLifecycleState({
      workflowId: 'workflow',
      runId: 'run',
      executionGeneration,
      stepId: 'active-step',
      executionPath: [0],
      states: lifecycleStepStates,
    });
    activeIdentity.stepAttempt = 1;
    const workflows = await storage.getStore('workflows');
    await workflows.persistWorkflowSnapshot({
      workflowName: 'workflow',
      runId: 'run',
      snapshot: {
        ...createEmptyWorkflowSnapshot('run'),
        status: 'running',
        executionGeneration,
        lifecycleResumeAttempt: 0,
        lifecycleStepStates,
        activeStepsPath: { 'active-step': [0] },
        context: { 'active-step': { status: 'running' } } as any,
      },
    });
    const published: any[] = [];
    vi.spyOn(pubsub, 'publish').mockImplementation(async (_topic, event) => {
      published.push(event);
    });
    const processor = new ExposedProcessor({ mastra, topicCleanupDelayMs: 0 });

    await processor.cancel({
      workflow: {
        id: 'workflow',
        stepGraph: [{ type: 'step', step: { id: 'active-step' } }],
        options: {},
      },
      workflowId: 'workflow',
      runId: 'run',
      executionGeneration,
      lifecycleResumeAttempt: 0,
      lifecycleStepStates,
      executionPath: [],
      stepResults: {},
      activeStepsPath: {},
      resumeSteps: [],
      prevResult: { status: 'canceled' },
      requestContext: {},
    });

    const lifecycleTypes = published
      .filter(event => event.type === 'workflow.lifecycle')
      .map(event => event.data.event.type);
    expect(lifecycleTypes).toEqual(['step.canceled', 'step.finished', 'workflow.canceled', 'workflow.finished']);
    await expect(workflows.loadWorkflowSnapshot({ workflowName: 'workflow', runId: 'run' })).resolves.toMatchObject({
      status: 'canceled',
    });

    await mastra.shutdown();
  });

  it('closes a persisted active attempt after the workflow registration is removed', async () => {
    const pubsub = new EventEmitterPubSub();
    const storage = new MockStore();
    const mastra = new Mastra({ logger: false, storage, pubsub, workflows: {} as any });
    const executionGeneration = 'removed-workflow-cancel-generation';
    const lifecycleStepStates = {};
    const { state: activeIdentity } = getOrCreateWorkflowStepLifecycleState({
      workflowId: 'removed-workflow',
      runId: 'removed-run',
      executionGeneration,
      stepId: 'removed-active-step',
      executionPath: [0],
      states: lifecycleStepStates,
    });
    activeIdentity.stepAttempt = 1;
    const workflows = await storage.getStore('workflows');
    await workflows.persistWorkflowSnapshot({
      workflowName: 'removed-workflow',
      runId: 'removed-run',
      snapshot: {
        ...createEmptyWorkflowSnapshot('removed-run'),
        status: 'running',
        executionGeneration,
        lifecycleResumeAttempt: 0,
        lifecycleStepStates,
        activeStepsPath: { 'removed-active-step': [0] },
        context: { 'removed-active-step': { status: 'running' } } as any,
      },
    });
    const published: any[] = [];
    vi.spyOn(pubsub, 'publish').mockImplementation(async (_topic, event) => {
      published.push(event);
    });

    await expect(
      mastra.handleWorkflowEvent({
        type: 'workflow.cancel',
        runId: 'removed-run',
        data: {
          workflowId: 'removed-workflow',
          runId: 'removed-run',
          executionGeneration,
          lifecycleResumeAttempt: 0,
          lifecycleStepStates,
          executionPath: [],
          stepResults: {},
          activeStepsPath: {},
          resumeSteps: [],
          prevResult: { status: 'canceled' },
          requestContext: {},
        },
      } as any),
    ).resolves.toEqual({ ok: true });

    const lifecycleTypes = published
      .filter(event => event.type === 'workflow.lifecycle')
      .map(event => event.data.event.type);
    expect(lifecycleTypes).toEqual(['step.canceled', 'step.finished', 'workflow.canceled', 'workflow.finished']);
    expect(published.find(event => event.data?.event?.type === 'step.canceled')?.data.event).toMatchObject({
      stepId: 'removed-active-step',
      stepCallId: activeIdentity.stepCallId,
      stepAttempt: 1,
    });
    await expect(
      workflows.loadWorkflowSnapshot({ workflowName: 'removed-workflow', runId: 'removed-run' }),
    ).resolves.toMatchObject({ status: 'canceled' });

    await mastra.shutdown();
  });
});
