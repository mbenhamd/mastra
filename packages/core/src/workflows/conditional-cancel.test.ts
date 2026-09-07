import { describe, expect, it, vi } from 'vitest';
import { z } from 'zod/v4';
import { InMemoryServerCache } from '../cache/inmemory';
import { CachingPubSub } from '../events/caching-pubsub';
import { EventEmitterPubSub } from '../events/event-emitter';
import { Mastra } from '../mastra';
import { MockStore } from '../storage/mock';
import { createWorkflow as createEventedWorkflow } from './evented';
import type { WorkflowRunState } from './types';
import { createStep, createWorkflow } from './index';

type Competitor = 'success' | 'failed' | 'new-generation' | 'increased-resume-attempt';

it('cancels a pending evented run after reserving its identity without storage', async () => {
  const step = createStep({
    id: 'step',
    inputSchema: z.object({}),
    outputSchema: z.object({}),
    execute: async () => ({}),
  });
  const workflow = createEventedWorkflow({
    id: 'cancel-without-storage',
    inputSchema: z.object({}),
    outputSchema: z.object({}),
  })
    .then(step)
    .commit();
  const mastra = new Mastra({
    logger: false,
    pubsub: new EventEmitterPubSub(),
    workflows: { [workflow.id]: workflow },
  });
  try {
    const run = await workflow.createRun({ runId: 'cancel-without-storage-run' });
    await run.cancel();
    expect(run.abortController.signal.aborted).toBe(true);
    expect(run.workflowRunStatus).toBe('canceled');
  } finally {
    await mastra.shutdown();
  }
});

it('aborts locally when both evented cancellation dispatch and its storage fallback fail', async () => {
  const storage = new MockStore();
  const pubsub = new EventEmitterPubSub();
  const step = createStep({
    id: 'step',
    inputSchema: z.object({}),
    outputSchema: z.object({}),
    execute: async () => ({}),
  });
  const workflow = createEventedWorkflow({
    id: 'cancel-storage-error',
    inputSchema: z.object({}),
    outputSchema: z.object({}),
  })
    .then(step)
    .commit();
  const mastra = new Mastra({ logger: false, storage, pubsub, workflows: { [workflow.id]: workflow } });
  const run = await workflow.createRun({ runId: 'cancel-storage-error-run' });
  const workflows = (await storage.getStore('workflows'))!;
  const publish = vi.spyOn(pubsub, 'publish').mockRejectedValue(new Error('transport unavailable'));
  const update = vi.spyOn(workflows, 'updateWorkflowState').mockRejectedValue(new Error('storage unavailable'));
  try {
    await expect(run.cancel()).rejects.toThrow('storage unavailable');
    expect(run.abortController.signal.aborted).toBe(true);
    await expect(
      workflows.loadWorkflowSnapshot({ workflowName: workflow.id, runId: run.runId }),
    ).resolves.toMatchObject({ status: 'pending' });
    expect(publish.mock.calls.filter(([, event]) => event.type === 'workflow.lifecycle')).toHaveLength(0);
  } finally {
    publish.mockRestore();
    update.mockRestore();
    await mastra.shutdown();
  }
});

function makeWorkflow(id: string) {
  const step = createStep({
    id: 'cancel-race-step',
    inputSchema: z.object({}),
    outputSchema: z.object({}),
    execute: async () => ({}),
  });
  return createWorkflow({ id, inputSchema: z.object({}), outputSchema: z.object({}) })
    .then(step)
    .commit();
}

function competitorOptions(kind: Competitor, snapshot: WorkflowRunState) {
  switch (kind) {
    case 'success':
      return { status: 'success' as const };
    case 'failed':
      return { status: 'failed' as const };
    case 'new-generation':
      return {
        status: 'pending' as const,
        executionGeneration: `${snapshot.executionGeneration}-replacement`,
      };
    case 'increased-resume-attempt':
      return {
        status: 'pending' as const,
        lifecycleResumeAttempt: (snapshot.lifecycleResumeAttempt ?? 0) + 1,
      };
  }
}

describe.each(['success', 'failed', 'new-generation', 'increased-resume-attempt'] as const)(
  'conditional cancellation CAS against a competing %s transition',
  competitor => {
    it('keeps the competitor and emits no local cancellation', async () => {
      const storage = new MockStore();
      const pubsub = new CachingPubSub(new EventEmitterPubSub(), new InMemoryServerCache(), {
        indexedReplay: { retentionMs: 60_000, maxEvents: 100 },
      });
      const workflow = makeWorkflow(`conditional-cancel-${competitor}`);
      const mastra = new Mastra({ logger: false, storage, pubsub, workflows: { [workflow.id]: workflow } });
      const runId = `conditional-cancel-${competitor}-run`;
      const run = await workflow.createRun({ runId });
      const identity = await run.getLifecycleExecutionIdentity();
      const workflowsStore = (await storage.getStore('workflows'))!;
      const initial = (await workflowsStore.loadWorkflowSnapshot({ workflowName: workflow.id, runId }))!;
      const originalUpdate = workflowsStore.updateWorkflowState.bind(workflowsStore);
      let competitorCommitted = false;
      const update = vi.spyOn(workflowsStore, 'updateWorkflowState').mockImplementation(async args => {
        if (!competitorCommitted && args.workflowName === workflow.id && args.runId === runId) {
          competitorCommitted = true;
          await originalUpdate({ ...args, opts: competitorOptions(competitor, initial) });
        }
        return originalUpdate(args);
      });

      try {
        await run.cancel();

        const final = await workflowsStore.loadWorkflowSnapshot({ workflowName: workflow.id, runId });
        expect(competitorCommitted).toBe(true);
        expect(final).toMatchObject({
          status: competitor === 'success' ? 'success' : competitor === 'failed' ? 'failed' : 'pending',
          executionGeneration:
            competitor === 'new-generation'
              ? `${initial.executionGeneration}-replacement`
              : initial.executionGeneration,
          ...(competitor === 'increased-resume-attempt'
            ? { lifecycleResumeAttempt: (initial.lifecycleResumeAttempt ?? 0) + 1 }
            : {}),
        });
        expect(run.abortController.signal.aborted).toBe(false);
        expect(run.workflowRunStatus).not.toBe('canceled');
        expect(await pubsub.getHistory(identity.topic)).toEqual([]);
        // The competing transition uses the bound real store method, while the
        // spy observes only the cancellation CAS attempt.
        expect(update).toHaveBeenCalledTimes(1);
      } finally {
        update.mockRestore();
        await mastra.shutdown();
      }
    });
  },
);

it('emits pending cancellation lifecycle exactly once when cancel is duplicated', async () => {
  const storage = new MockStore();
  const pubsub = new CachingPubSub(new EventEmitterPubSub(), new InMemoryServerCache(), {
    indexedReplay: { retentionMs: 60_000, maxEvents: 100 },
  });
  const workflow = makeWorkflow('conditional-cancel-duplicate');
  const mastra = new Mastra({ logger: false, storage, pubsub, workflows: { [workflow.id]: workflow } });
  try {
    const run = await workflow.createRun({ runId: 'conditional-cancel-duplicate-run' });
    const identity = await run.getLifecycleExecutionIdentity();
    await run.cancel();
    await run.cancel();

    const events = (await pubsub.getHistory(identity.topic)).map(item => item.data.event.type);
    expect(events).toEqual(['workflow.canceled', 'workflow.finished']);
  } finally {
    await mastra.shutdown();
  }
});

describe.each(['success', 'new-generation', 'increased-resume-attempt'] as const)(
  'evented cancellation fallback against a competing %s transition',
  competitor => {
    it('does not let failed workflow.cancel dispatch overwrite the competitor', async () => {
      const storage = new MockStore();
      const pubsub = new CachingPubSub(new EventEmitterPubSub(), new InMemoryServerCache(), {
        indexedReplay: { retentionMs: 60_000, maxEvents: 100 },
      });
      const step = createStep({
        id: 'evented-cancel-race-step',
        inputSchema: z.object({}),
        outputSchema: z.object({}),
        execute: async () => ({}),
      });
      const workflow = createEventedWorkflow({
        id: `evented-conditional-cancel-${competitor}`,
        inputSchema: z.object({}),
        outputSchema: z.object({}),
      })
        .then(step)
        .commit();
      const mastra = new Mastra({ logger: false, storage, pubsub, workflows: { [workflow.id]: workflow } });
      const runId = `evented-conditional-cancel-${competitor}-run`;
      const run = await workflow.createRun({ runId });
      const identity = await run.getLifecycleExecutionIdentity();
      const workflowsStore = (await storage.getStore('workflows'))!;
      const initial = (await workflowsStore.loadWorkflowSnapshot({ workflowName: workflow.id, runId }))!;
      const originalUpdate = workflowsStore.updateWorkflowState.bind(workflowsStore);
      const update = vi.spyOn(workflowsStore, 'updateWorkflowState').mockImplementation(async args => {
        if (args.opts.status === 'canceled') {
          await originalUpdate({ ...args, opts: competitorOptions(competitor, initial) });
        }
        return originalUpdate(args);
      });
      const originalPublish = pubsub.publish.bind(pubsub);
      const publish = vi.spyOn(pubsub, 'publish').mockImplementation(async (topic, event, options) => {
        if (topic === 'workflows' && event.type === 'workflow.cancel') {
          throw new Error('workflow.cancel transport unavailable');
        }
        return originalPublish(topic, event, options);
      });

      try {
        await run.cancel();
        const final = await workflowsStore.loadWorkflowSnapshot({ workflowName: workflow.id, runId });
        expect(final).toMatchObject({
          status: competitor === 'success' ? 'success' : 'pending',
          executionGeneration:
            competitor === 'new-generation'
              ? `${initial.executionGeneration}-replacement`
              : initial.executionGeneration,
          ...(competitor === 'increased-resume-attempt'
            ? { lifecycleResumeAttempt: (initial.lifecycleResumeAttempt ?? 0) + 1 }
            : {}),
        });
        expect(run.abortController.signal.aborted).toBe(false);
        expect(run.workflowRunStatus).not.toBe('canceled');
        expect(await pubsub.getHistory(identity.topic)).toEqual([]);
        expect(publish).toHaveBeenCalledWith(
          'workflows',
          expect.objectContaining({ type: 'workflow.cancel' }),
          undefined,
        );
      } finally {
        publish.mockRestore();
        update.mockRestore();
        await mastra.shutdown();
      }
    });
  },
);
