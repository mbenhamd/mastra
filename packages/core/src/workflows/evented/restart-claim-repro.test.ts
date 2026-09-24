import { describe, expect, it, vi } from 'vitest';
import { z } from 'zod/v4';
import { EventEmitterPubSub } from '../../events/event-emitter';
import { Mastra } from '../../mastra';
import { MockStore } from '../../storage/mock';
import { createStep, createWorkflow } from '.';

describe('evented restart claim cancel repro', () => {
  it('cancel recorded durably mid-claim (no in-flight event)', async () => {
    let workExecutions = 0;
    const workStep = createStep({
      id: 'work',
      inputSchema: z.object({ item: z.string() }),
      outputSchema: z.object({ done: z.boolean() }),
      suspendSchema: z.object({ reason: z.string() }),
      execute: async ({ suspend }) => {
        workExecutions++;
        if (workExecutions === 1) {
          await suspend({ reason: 'pause once' });
          return { done: false };
        }
        return { done: true };
      },
    });
    const workflow = createWorkflow({
      id: 'evented-restart-cancel-wf',
      inputSchema: z.object({ item: z.string() }),
      outputSchema: z.object({ done: z.boolean() }),
      steps: [workStep],
      options: { validateInputs: false },
    })
      .then(workStep)
      .commit();

    const storage = new MockStore();
    const pubsub = new EventEmitterPubSub();
    const published: any[] = [];
    const originalPublish = pubsub.publish.bind(pubsub);
    vi.spyOn(pubsub, 'publish').mockImplementation(async (topic, event, opts) => {
      published.push({ topic, type: (event as any).type, gen: (event as any).data?.executionGeneration });
      return originalPublish(topic, event, opts);
    });
    const mastra = new Mastra({
      storage,
      pubsub,
      workflows: { 'evented-restart-cancel-wf': workflow },
      logger: false,
    });
    await mastra.startWorkers();

    const run = await workflow.createRun();
    const started = await run.start({ inputData: { item: 'widget' } });
    expect(started.status).toBe('suspended');

    const workflowsStore = await storage.getStore('workflows');
    const suspended = await workflowsStore.loadWorkflowSnapshot({
      workflowName: 'evented-restart-cancel-wf',
      runId: run.runId,
    });
    await workflowsStore.persistWorkflowSnapshot({
      workflowName: 'evented-restart-cancel-wf',
      runId: run.runId,
      snapshot: {
        ...suspended!,
        status: 'running',
        suspendedPaths: {},
        activePaths: [0],
        activeStepsPath: { work: [0] },
      },
    });

    // Simulate a cancellation recorded durably by another process while the
    // claim is in flight: after the claim CAS installs generation A, flip the
    // row to canceled against that same generation (no local event in flight).
    const originalUpdate = workflowsStore.updateWorkflowState.bind(workflowsStore);
    let cancelWritten = false;
    vi.spyOn(workflowsStore, 'updateWorkflowState').mockImplementation(async args => {
      const updated = await originalUpdate(args);
      if (
        !cancelWritten &&
        updated &&
        args.opts.status === 'running' &&
        typeof args.opts.executionGeneration === 'string'
      ) {
        cancelWritten = true;
        await originalUpdate({
          workflowName: args.workflowName,
          runId: args.runId,
          opts: {
            status: 'canceled',
            expectedStatus: 'running',
            expectedExecutionGeneration: args.opts.executionGeneration,
            expectedLifecycleResumeAttempt: args.opts.lifecycleResumeAttempt ?? 0,
          },
        });
      }
      return updated;
    });

    let hangTimeout: ReturnType<typeof setTimeout> | undefined;
    const result = await Promise.race([
      run.restart(),
      new Promise((_, reject) => {
        hangTimeout = setTimeout(() => reject(new Error('TIMEOUT: restart hung')), 10000);
      }),
    ]);
    clearTimeout(hangTimeout);

    // The durably admitted cancellation resolves the restart as canceled
    // instead of hanging the awaiting execute.
    expect((result as { status: string }).status).toBe('canceled');

    // The workflow.cancel dispatched for the adopted (already-aborted)
    // generation must surface a workflows-finish workflow.end republished
    // from the already-terminal row. Processing is asynchronous, so poll for
    // the publish instead of asserting synchronously.
    const deadline = Date.now() + 5000;
    let republished = false;
    while (Date.now() < deadline && !republished) {
      republished = published.some(
        p => p.topic === 'workflows-finish' && p.type === 'workflow.end' && p.gen !== undefined,
      );
      if (!republished) await new Promise(resolve => setTimeout(resolve, 10));
    }
    expect(republished).toBe(true);
    await mastra.shutdown();
  }, 30000);

  it('cancel admitted mid-claim resolves as canceled — not paused — under perStep', async () => {
    let workExecutions = 0;
    const workStep = createStep({
      id: 'work',
      inputSchema: z.object({ item: z.string() }),
      outputSchema: z.object({ done: z.boolean() }),
      suspendSchema: z.object({ reason: z.string() }),
      execute: async ({ suspend }) => {
        workExecutions++;
        if (workExecutions === 1) {
          await suspend({ reason: 'pause once' });
          return { done: false };
        }
        return { done: true };
      },
    });
    const workflow = createWorkflow({
      id: 'evented-timetravel-perstep-cancel-wf',
      inputSchema: z.object({ item: z.string() }),
      outputSchema: z.object({ done: z.boolean() }),
      steps: [workStep],
      options: { validateInputs: false },
    })
      .then(workStep)
      .commit();

    const storage = new MockStore();
    const pubsub = new EventEmitterPubSub();
    const mastra = new Mastra({
      storage,
      pubsub,
      workflows: { 'evented-timetravel-perstep-cancel-wf': workflow },
      logger: false,
    });
    await mastra.startWorkers();

    const run = await workflow.createRun();
    const started = await run.start({ inputData: { item: 'widget' } });
    expect(started.status).toBe('suspended');

    const workflowsStore = await storage.getStore('workflows');

    // Record a durable cancellation while the timeTravel claim is in flight:
    // after the claim CAS installs the generation, flip the row to canceled
    // against that same generation so adoption observes an aborted controller.
    const originalUpdate = workflowsStore.updateWorkflowState.bind(workflowsStore);
    let cancelWritten = false;
    vi.spyOn(workflowsStore, 'updateWorkflowState').mockImplementation(async args => {
      const updated = await originalUpdate(args);
      if (
        !cancelWritten &&
        updated &&
        args.opts.status === 'running' &&
        typeof args.opts.executionGeneration === 'string'
      ) {
        cancelWritten = true;
        await originalUpdate({
          workflowName: args.workflowName,
          runId: args.runId,
          opts: {
            status: 'canceled',
            expectedStatus: 'running',
            expectedExecutionGeneration: args.opts.executionGeneration,
            expectedLifecycleResumeAttempt: args.opts.lifecycleResumeAttempt ?? 0,
          },
        });
      }
      return updated;
    });

    let hangTimeout: ReturnType<typeof setTimeout> | undefined;
    const result = await Promise.race([
      run.timeTravel({ step: 'work', perStep: true }),
      new Promise((_, reject) => {
        hangTimeout = setTimeout(() => reject(new Error('TIMEOUT: timeTravel hung')), 10000);
      }),
    ]);
    clearTimeout(hangTimeout);

    // perStep must not reclassify the durable canceled outcome as paused:
    // storage records canceled, so the caller must see canceled.
    expect((result as { status: string }).status).toBe('canceled');
    await mastra.shutdown();
  }, 30000);
});
