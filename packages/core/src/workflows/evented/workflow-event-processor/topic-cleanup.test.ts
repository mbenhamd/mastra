import { describe, expect, it, vi } from 'vitest';
import { InMemoryServerCache } from '../../../cache/inmemory';
import { CachingPubSub } from '../../../events/caching-pubsub';
import { EventEmitterPubSub } from '../../../events/event-emitter';
import type { PubSub } from '../../../events/pubsub';
import { Mastra } from '../../../mastra';
import { MockStore } from '../../../storage/mock';
import { getWorkflowLifecycleTopic } from '../../lifecycle-events';
import { WorkflowEventProcessor } from './index';

class TestWorkflowEventProcessor extends WorkflowEventProcessor {
  callProcessWorkflowEnd(args: any) {
    return this.processWorkflowEnd(args);
  }
  callProcessWorkflowFail(args: any) {
    return this.processWorkflowFail(args);
  }
  callProcessWorkflowStart(args: any) {
    return this.processWorkflowStart(args);
  }
}

async function persistRunStatus(mastra: Mastra, status: string, executionGeneration?: string) {
  const workflowsStore = await mastra.getStorage()!.getStore('workflows');
  await workflowsStore!.persistWorkflowSnapshot({
    workflowName: 'wf',
    runId: 'run-1',
    snapshot: {
      status,
      context: {},
      activePaths: [],
      timestamp: Date.now(),
      value: {},
      runId: 'run-1',
      executionGeneration,
    } as any,
  });
}

function setup(topicCleanupDelayMs?: number, pubsub: PubSub = new EventEmitterPubSub()) {
  const mastra = new Mastra({
    logger: false,
    storage: new MockStore(),
    workflows: {} as any,
    pubsub,
  });
  const processor = new TestWorkflowEventProcessor({ mastra, topicCleanupDelayMs });
  const clearTopicSpy = vi.spyOn(pubsub, 'clearTopic');
  return { mastra, processor, clearTopicSpy };
}

const baseArgs = {
  workflowId: 'wf',
  runId: 'run-1',
  executionGeneration: 'test-execution-generation',
  lifecycleResumeAttempt: 0,
  lifecycleStepStates: {},
  executionPath: [],
  resumeSteps: [],
  stepResults: {},
  activeStepsPath: {},
  requestContext: {},
  prevResult: { status: 'success' },
};

describe('WorkflowEventProcessor per-run topic cleanup', () => {
  it('clears the workflow.events.v2 topic after a terminal workflow.end', async () => {
    const { mastra, processor, clearTopicSpy } = setup(10);

    await processor.callProcessWorkflowEnd({ ...baseArgs });

    // Deletion is delayed so watchers can drain the terminal event first.
    expect(clearTopicSpy).not.toHaveBeenCalled();
    await vi.waitFor(() => {
      expect(clearTopicSpy).toHaveBeenCalledWith('workflow.events.v2.run-1');
      expect(clearTopicSpy).toHaveBeenCalledWith(
        getWorkflowLifecycleTopic({
          workflowId: baseArgs.workflowId,
          runId: baseArgs.runId,
          executionGeneration: baseArgs.executionGeneration,
        }),
      );
    });

    await mastra.shutdown();
  });

  it('clears the workflow.events.v2 topic after workflow.fail', async () => {
    const { mastra, processor, clearTopicSpy } = setup(10);

    await processor.callProcessWorkflowFail({
      ...baseArgs,
      prevResult: { status: 'failed', error: 'boom' },
    });

    await vi.waitFor(() => {
      expect(clearTopicSpy).toHaveBeenCalledWith('workflow.events.v2.run-1');
    });

    await mastra.shutdown();
  });

  it('does not clear the topic for a per-step (paused) workflow.end', async () => {
    const { mastra, processor, clearTopicSpy } = setup(10);

    await processor.callProcessWorkflowEnd({ ...baseArgs, perStep: true });

    // The run is only paused: it keeps writing to its topic when the next
    // step executes, so cleanup must not be scheduled.
    await new Promise(resolve => setTimeout(resolve, 50));
    expect(clearTopicSpy).not.toHaveBeenCalled();

    await mastra.shutdown();
  });

  it('clears the topic for a canceled per-step workflow.end', async () => {
    const { mastra, processor, clearTopicSpy } = setup(10);

    await processor.callProcessWorkflowEnd({
      ...baseArgs,
      perStep: true,
      prevResult: { status: 'canceled' },
    });

    await vi.waitFor(() => {
      expect(clearTopicSpy).toHaveBeenCalledWith('workflow.events.v2.run-1');
    });

    await mastra.shutdown();
  });

  it('disables topic cleanup when topicCleanupDelayMs is 0', async () => {
    const { mastra, processor, clearTopicSpy } = setup(0);

    await processor.callProcessWorkflowEnd({ ...baseArgs });

    await new Promise(resolve => setTimeout(resolve, 50));
    expect(clearTopicSpy).not.toHaveBeenCalled();

    await mastra.shutdown();
  });

  it('does not clear exact replay history before its declared retention horizon', async () => {
    vi.useFakeTimers();
    try {
      const pubsub = new CachingPubSub(new EventEmitterPubSub(), new InMemoryServerCache(), {
        indexedReplay: { retentionMs: 100, maxEvents: 100 },
      });
      const { mastra, processor, clearTopicSpy } = setup(10, pubsub);

      await processor.callProcessWorkflowEnd({ ...baseArgs });
      await vi.advanceTimersByTimeAsync(99);
      expect(clearTopicSpy).not.toHaveBeenCalled();

      await vi.advanceTimersByTimeAsync(1);
      expect(clearTopicSpy).toHaveBeenCalledWith('workflow.events.v2.run-1');
      await mastra.shutdown();
    } finally {
      vi.useRealTimers();
    }
  });

  it('skips deletion when the run is active again at fire time (cross-process restart)', async () => {
    const { mastra, processor, clearTopicSpy } = setup(10);

    await processor.callProcessWorkflowEnd({ ...baseArgs });
    // Simulate a timeTravel/restart picked up by a different worker process:
    // the local timer is still pending, but shared storage says the run is
    // executing again.
    await persistRunStatus(mastra, 'running');

    await new Promise(resolve => setTimeout(resolve, 60));
    expect(clearTopicSpy).not.toHaveBeenCalled();

    await mastra.shutdown();
  });

  it('does not let an old generation clear a newer generation shared topic', async () => {
    vi.useFakeTimers();
    try {
      const { mastra, processor, clearTopicSpy } = setup(10);

      await processor.callProcessWorkflowEnd({ ...baseArgs });
      await persistRunStatus(mastra, 'success', 'newer-execution-generation');
      await vi.advanceTimersByTimeAsync(10);

      expect(clearTopicSpy).not.toHaveBeenCalledWith('workflow.events.v2.run-1');
      expect(clearTopicSpy).toHaveBeenCalledWith(
        getWorkflowLifecycleTopic({
          workflowId: baseArgs.workflowId,
          runId: baseArgs.runId,
          executionGeneration: baseArgs.executionGeneration,
        }),
      );

      await mastra.shutdown();
    } finally {
      vi.useRealTimers();
    }
  });

  it('retains independent cleanup timers when workflows reuse a run id', async () => {
    vi.useFakeTimers();
    try {
      const { mastra, processor, clearTopicSpy } = setup(10);
      const first = { ...baseArgs, workflowId: 'workflow-a', executionGeneration: 'generation-a' };
      const second = { ...baseArgs, workflowId: 'workflow-b', executionGeneration: 'generation-b' };

      await processor.callProcessWorkflowEnd(first);
      await processor.callProcessWorkflowEnd(second);
      await vi.advanceTimersByTimeAsync(10);

      expect(clearTopicSpy).toHaveBeenCalledWith(getWorkflowLifecycleTopic(first));
      expect(clearTopicSpy).toHaveBeenCalledWith(getWorkflowLifecycleTopic(second));

      await mastra.shutdown();
    } finally {
      vi.useRealTimers();
    }
  });

  it('proceeds with deletion when the persisted status is terminal', async () => {
    const { mastra, processor, clearTopicSpy } = setup(10);

    await persistRunStatus(mastra, 'failed');
    await processor.callProcessWorkflowFail({
      ...baseArgs,
      prevResult: { status: 'failed', error: 'boom' },
    });

    await vi.waitFor(() => {
      expect(clearTopicSpy).toHaveBeenCalledWith('workflow.events.v2.run-1');
    });

    await mastra.shutdown();
  });

  it('cancels a pending cleanup when the run restarts in-process', async () => {
    vi.useFakeTimers();
    try {
      const { mastra, processor, clearTopicSpy } = setup(30);

      await processor.callProcessWorkflowEnd({ ...baseArgs });

      // A timeTravel/restart re-enters through processWorkflowStart with the
      // same runId. The minimal workflow here may make later phases of start
      // throw — the cancellation happens first and is what's under test.
      await processor
        .callProcessWorkflowStart({
          ...baseArgs,
          workflow: { id: 'wf', options: {}, stepGraph: [] },
        })
        .catch(() => {});

      // Force the persisted status terminal so the fire-time status guard would
      // NOT protect the topic. Only the in-process timer cancellation can
      // prevent deletion here — this isolates the layer under test.
      await persistRunStatus(mastra, 'success');

      await vi.advanceTimersByTimeAsync(80);
      expect(clearTopicSpy).not.toHaveBeenCalled();

      await mastra.shutdown();
    } finally {
      vi.useRealTimers();
    }
  });
});
