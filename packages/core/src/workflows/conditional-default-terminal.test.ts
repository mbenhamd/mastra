import { describe, expect, it, vi } from 'vitest';
import { z } from 'zod/v4';
import { EventEmitterPubSub } from '../events/event-emitter';
import { Mastra } from '../mastra';
import { MockStore } from '../storage/mock';
import type { PersistWorkflowStepUpdateInput } from '../storage/types';
import { createWorkflow } from './create';
import { createStep } from './workflow';

type Competitor = 'canceled' | 'success' | 'new-generation' | 'increased-resume-attempt';

function deferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void;
  const promise = new Promise<T>(resolvePromise => {
    resolve = resolvePromise;
  });
  return { promise, resolve };
}

function makeWorkflow(id: string, options: { fail?: boolean; onFinish?: (result: any) => Promise<void> } = {}) {
  const step = createStep({
    id: 'terminal-step',
    inputSchema: z.object({}),
    outputSchema: z.object({}),
    execute: async () => {
      if (options.fail) throw new Error('local stale failure');
      return {};
    },
  });

  return createWorkflow({
    id,
    inputSchema: z.object({}),
    outputSchema: z.object({}),
    options,
  })
    .then(step)
    .commit();
}

describe.each(['canceled', 'success', 'new-generation', 'increased-resume-attempt'] as const)(
  'default engine terminal persistence fence against a remote %s winner',
  competitor => {
    it('retains the remote terminal transition and emits no stale workflow.finished event', async () => {
      const storage = new MockStore();
      const pubsub = new EventEmitterPubSub();
      const workflowId = `conditional-default-terminal-${competitor}`;
      const runId = `${workflowId}-run`;
      const workflow = makeWorkflow(workflowId);
      const mastra = new Mastra({ logger: false, storage, pubsub, workflows: { [workflowId]: workflow } });
      const run = await workflow.createRun({ runId });
      const workflowsStore = (await storage.getStore('workflows'))!;
      const originalPersist = workflowsStore.persistWorkflowStepUpdate.bind(workflowsStore);
      const persistCalls: PersistWorkflowStepUpdateInput[] = [];
      let injected = false;

      const persist = vi.spyOn(workflowsStore, 'persistWorkflowStepUpdate').mockImplementation(async args => {
        persistCalls.push(args);
        if (!injected && ['canceled', 'success'].includes(args.snapshot.status)) {
          injected = true;
          const replacementGeneration = `${args.snapshot.executionGeneration}-remote`;
          await workflowsStore.updateWorkflowState({
            workflowName: workflowId,
            runId,
            opts:
              competitor === 'new-generation'
                ? {
                    status: 'pending',
                    executionGeneration: replacementGeneration,
                    expectedExecutionGeneration: args.snapshot.executionGeneration,
                  }
                : competitor === 'increased-resume-attempt'
                  ? {
                      status: 'pending',
                      lifecycleResumeAttempt: (args.snapshot.lifecycleResumeAttempt ?? 0) + 1,
                      expectedExecutionGeneration: args.snapshot.executionGeneration,
                    }
                  : {
                      status: competitor,
                      ...(competitor === 'success' ? { result: { remote: true } } : {}),
                      expectedExecutionGeneration: args.snapshot.executionGeneration,
                    },
          });
        }
        return originalPersist(args);
      });
      const publish = vi.spyOn(pubsub, 'publish');

      try {
        const result = await run.start({ inputData: {} });
        const final = await workflowsStore.loadWorkflowSnapshot({ workflowName: workflowId, runId });
        const lifecycleEvents = publish.mock.calls
          .filter(([topic]) => String(topic).startsWith('workflow.lifecycle.v1.'))
          .map(([, event]) => (event as any).data?.event)
          .filter(Boolean);

        expect(injected).toBe(true);
        expect(persist).toHaveBeenCalled();
        expect(persistCalls.at(-1)?.snapshot.status).toBe('success');
        expect(final).toMatchObject(
          competitor === 'new-generation'
            ? { status: 'pending', executionGeneration: expect.stringContaining('-remote') }
            : competitor === 'increased-resume-attempt'
              ? { status: 'pending', lifecycleResumeAttempt: 1 }
              : { status: competitor },
        );
        expect(result.status).toBe(
          competitor === 'new-generation' || competitor === 'increased-resume-attempt' ? 'canceled' : competitor,
        );
        if (competitor === 'success') {
          if (result.status === 'success') expect(result.result).toEqual({ remote: true });
        }
        expect(lifecycleEvents.filter(event => event.type === 'workflow.finished')).toEqual([]);
      } finally {
        persist.mockRestore();
        await mastra.shutdown();
      }
    });
  },
);

it('returns the remote failure error when a terminal failure write loses the fence', async () => {
  const storage = new MockStore();
  const pubsub = new EventEmitterPubSub();
  const workflowId = 'conditional-default-terminal-failed';
  const runId = `${workflowId}-run`;
  const workflow = makeWorkflow(workflowId, { fail: true });
  const mastra = new Mastra({ logger: false, storage, pubsub, workflows: { [workflowId]: workflow } });
  const run = await workflow.createRun({ runId });
  const workflowsStore = (await storage.getStore('workflows'))!;
  const originalPersist = workflowsStore.persistWorkflowStepUpdate.bind(workflowsStore);
  let injected = false;
  const persist = vi.spyOn(workflowsStore, 'persistWorkflowStepUpdate').mockImplementation(async args => {
    if (!injected && args.snapshot.status === 'failed') {
      injected = true;
      await workflowsStore.updateWorkflowState({
        workflowName: workflowId,
        runId,
        opts: {
          status: 'failed',
          error: { name: 'RemoteWinner', message: 'remote failure won' },
          expectedExecutionGeneration: args.snapshot.executionGeneration,
        },
      });
    }
    return originalPersist(args);
  });
  const publish = vi.spyOn(pubsub, 'publish');

  try {
    const result = await run.start({ inputData: {} });
    const final = await workflowsStore.loadWorkflowSnapshot({ workflowName: workflowId, runId });
    expect(injected).toBe(true);
    expect(final).toMatchObject({ status: 'failed', error: { name: 'RemoteWinner', message: 'remote failure won' } });
    expect(result.status).toBe('failed');
    if (result.status === 'failed') {
      expect(result.error).toMatchObject({ name: 'RemoteWinner', message: 'remote failure won' });
    }
    expect(
      publish.mock.calls
        .filter(([topic]) => String(topic).startsWith('workflow.lifecycle.v1.'))
        .map(([, event]) => (event as any).data?.event)
        .filter((event: any) => event?.type === 'workflow.finished'),
    ).toEqual([]);
  } finally {
    persist.mockRestore();
    await mastra.shutdown();
  }
});

it('does not let cancellation replace a stored success while onFinish is awaiting', async () => {
  const storage = new MockStore();
  const pubsub = new EventEmitterPubSub();
  const entered = deferred<void>();
  const release = deferred<void>();
  const workflowId = 'conditional-default-terminal-on-finish';
  const runId = `${workflowId}-run`;
  const workflow = makeWorkflow(workflowId, {
    onFinish: async result => {
      expect(result.status).toBe('success');
      entered.resolve();
      await release.promise;
    },
  });
  const mastra = new Mastra({ logger: false, storage, pubsub, workflows: { [workflowId]: workflow } });
  const run = await workflow.createRun({ runId });
  const workflowsStore = (await storage.getStore('workflows'))!;

  try {
    const started = run.start({ inputData: {} });
    await entered.promise;
    await expect(workflowsStore.loadWorkflowSnapshot({ workflowName: workflowId, runId })).resolves.toMatchObject({
      status: 'success',
    });
    await run.cancel();
    await expect(workflowsStore.loadWorkflowSnapshot({ workflowName: workflowId, runId })).resolves.toMatchObject({
      status: 'success',
    });
    release.resolve();
    await expect(started).resolves.toMatchObject({ status: 'success', result: {} });
  } finally {
    release.resolve();
    await mastra.shutdown();
  }
});

it('returns and retains the remote terminal final state after a fenced local success', async () => {
  const storage = new MockStore();
  const pubsub = new EventEmitterPubSub();
  const workflowId = 'conditional-default-terminal-final-state';
  const runId = `${workflowId}-run`;
  const workflow = makeWorkflow(workflowId);
  const mastra = new Mastra({ logger: false, storage, pubsub, workflows: { [workflowId]: workflow } });
  const run = await workflow.createRun({ runId });
  const workflowsStore = (await storage.getStore('workflows'))!;
  const originalPersist = workflowsStore.persistWorkflowStepUpdate.bind(workflowsStore);
  let injected = false;
  const persist = vi.spyOn(workflowsStore, 'persistWorkflowStepUpdate').mockImplementation(async args => {
    if (!injected && args.snapshot.status === 'success') {
      injected = true;
      await workflowsStore.updateWorkflowState({
        workflowName: workflowId,
        runId,
        opts: {
          status: 'success',
          result: { remote: 'result' },
          finalState: { remote: true },
          expectedExecutionGeneration: args.snapshot.executionGeneration,
        },
      });
    }
    return originalPersist(args);
  });

  try {
    const result = await run.start({
      inputData: {},
      initialState: { local: true },
      outputOptions: { includeState: true },
    });
    const final = await workflowsStore.loadWorkflowSnapshot({ workflowName: workflowId, runId });
    expect(injected).toBe(true);
    expect(result.status).toBe('success');
    if (result.status === 'success') {
      expect(result.result).toEqual({ remote: 'result' });
      expect(result.state).toEqual({ remote: true });
    }
    expect(final).toMatchObject({
      status: 'success',
      result: { remote: 'result' },
      value: { remote: true },
      context: { __state: { remote: true } },
    });
    expect(final?.value).not.toEqual({ local: true });
  } finally {
    persist.mockRestore();
    await mastra.shutdown();
  }
});

it('does not treat a local abort as terminal ownership when a remote cancel wins the fence', async () => {
  const storage = new MockStore();
  const pubsub = new EventEmitterPubSub();
  const onFinish = vi.fn(async () => {});
  const workflowId = 'conditional-default-terminal-local-abort';
  const runId = `${workflowId}-run`;
  const workflow = makeWorkflow(workflowId, { onFinish });
  const mastra = new Mastra({ logger: false, storage, pubsub, workflows: { [workflowId]: workflow } });
  const run = await workflow.createRun({ runId });
  const workflowsStore = (await storage.getStore('workflows'))!;
  const originalPersist = workflowsStore.persistWorkflowStepUpdate.bind(workflowsStore);
  let injected = false;
  const persist = vi.spyOn(workflowsStore, 'persistWorkflowStepUpdate').mockImplementation(async args => {
    if (!injected && args.snapshot.status === 'success') {
      injected = true;
      run.abortController.abort();
      await workflowsStore.updateWorkflowState({
        workflowName: workflowId,
        runId,
        opts: { status: 'canceled', expectedExecutionGeneration: args.snapshot.executionGeneration },
      });
    }
    return originalPersist(args);
  });
  const publish = vi.spyOn(pubsub, 'publish');

  try {
    const result = await run.start({ inputData: {} });
    const final = await workflowsStore.loadWorkflowSnapshot({ workflowName: workflowId, runId });
    expect(injected).toBe(true);
    expect(run.abortController.signal.aborted).toBe(true);
    expect(result.status).toBe('canceled');
    expect(final).toMatchObject({ status: 'canceled' });
    expect(onFinish).not.toHaveBeenCalled();
    expect(
      publish.mock.calls
        .filter(([topic]) => String(topic).startsWith('workflow.lifecycle.v1.'))
        .map(([, event]) => (event as any).data?.event)
        .filter((event: any) => event?.type === 'workflow.finished'),
    ).toEqual([]);
  } finally {
    persist.mockRestore();
    await mastra.shutdown();
  }
});

it('publishes one local cancellation lifecycle sequence when Run.cancel wins an active durable run', async () => {
  const storage = new MockStore();
  const pubsub = new EventEmitterPubSub();
  const entered = deferred<void>();
  const onFinish = vi.fn(async (result: { status: string }) => {
    expect(result.status).toBe('canceled');
  });
  const workflowId = 'conditional-default-terminal-active-cancel';
  const runId = `${workflowId}-run`;
  const step = createStep({
    id: 'blocking-step',
    inputSchema: z.object({}),
    outputSchema: z.object({}),
    execute: async ({ abortSignal }) => {
      entered.resolve();
      await new Promise<void>(resolve => {
        if (abortSignal.aborted) return resolve();
        abortSignal.addEventListener('abort', () => resolve(), { once: true });
      });
      return {};
    },
  });
  const workflow = createWorkflow({
    id: workflowId,
    inputSchema: z.object({}),
    outputSchema: z.object({}),
    options: { onFinish },
  })
    .then(step)
    .commit();
  const mastra = new Mastra({ logger: false, storage, pubsub, workflows: { [workflowId]: workflow } });
  const run = await workflow.createRun({ runId });
  const publish = vi.spyOn(pubsub, 'publish');

  try {
    const started = run.start({ inputData: {} });
    await entered.promise;
    await run.cancel();
    await expect(started).resolves.toMatchObject({ status: 'canceled' });
    const lifecycleEvents = publish.mock.calls
      .filter(([topic]) => String(topic).startsWith('workflow.lifecycle.v1.'))
      .map(([, event]) => (event as any).data?.event)
      .filter(Boolean);
    expect(onFinish).toHaveBeenCalledOnce();
    expect(lifecycleEvents.map(event => event.type)).toEqual([
      'workflow.started',
      'step.started',
      'workflow.canceled',
      'workflow.finished',
    ]);
    expect(lifecycleEvents.filter(event => event.type === 'workflow.canceled')).toHaveLength(1);
    expect(lifecycleEvents.filter(event => event.type === 'workflow.finished')).toHaveLength(1);
    expect(lifecycleEvents.at(-1)).toMatchObject({ type: 'workflow.finished', status: 'canceled' });
  } finally {
    await mastra.shutdown();
  }
});

it('waits for the durable cancellation acknowledgement after the active step finishes', async () => {
  const storage = new MockStore();
  const pubsub = new EventEmitterPubSub();
  const entered = deferred<void>();
  const releaseStep = deferred<void>();
  const stepFinished = deferred<void>();
  const admissionCalled = deferred<void>();
  const releaseAcknowledgement = deferred<void>();
  const onFinish = vi.fn(async (result: { status: string }) => {
    expect(result.status).toBe('canceled');
  });
  const workflowId = 'conditional-default-terminal-delayed-cancel-ack';
  const runId = `${workflowId}-run`;
  const step = createStep({
    id: 'ack-blocking-step',
    inputSchema: z.object({}),
    outputSchema: z.object({}),
    execute: async () => {
      entered.resolve();
      await releaseStep.promise;
      stepFinished.resolve();
      return {};
    },
  });
  const workflow = createWorkflow({
    id: workflowId,
    inputSchema: z.object({}),
    outputSchema: z.object({}),
    options: { onFinish },
  })
    .then(step)
    .commit();
  const mastra = new Mastra({ logger: false, storage, pubsub, workflows: { [workflowId]: workflow } });
  const run = await workflow.createRun({ runId });
  const workflowsStore = (await storage.getStore('workflows'))!;
  const originalAdmission = (run as any).isCancellationAdmitted.bind(run);
  (run as any).isCancellationAdmitted = (...args: any[]) => {
    const admission = originalAdmission(...args);
    admissionCalled.resolve();
    return admission;
  };
  const originalUpdate = workflowsStore.updateWorkflowState.bind(workflowsStore);
  const update = vi.spyOn(workflowsStore, 'updateWorkflowState').mockImplementation(async args => {
    const committed = await originalUpdate(args);
    if (args.opts.status === 'canceled' && committed) {
      await releaseAcknowledgement.promise;
    }
    return committed;
  });
  const publish = vi.spyOn(pubsub, 'publish');

  try {
    const started = run.start({ inputData: {} });
    await entered.promise;
    const cancel = run.cancel();
    await vi.waitFor(async () => {
      await expect(workflowsStore.loadWorkflowSnapshot({ workflowName: workflowId, runId })).resolves.toMatchObject({
        status: 'canceled',
      });
    });
    releaseStep.resolve();
    await stepFinished.promise;
    await admissionCalled.promise;
    expect(onFinish).not.toHaveBeenCalled();
    expect(
      publish.mock.calls
        .filter(([topic]) => String(topic).startsWith('workflow.lifecycle.v1.'))
        .map(([, event]) => (event as any).data?.event)
        .filter((event: any) => event?.type === 'workflow.finished'),
    ).toEqual([]);
    releaseAcknowledgement.resolve();
    await expect(cancel).resolves.toBeUndefined();
    await expect(started).resolves.toMatchObject({ status: 'canceled' });
    expect(onFinish).toHaveBeenCalledOnce();
    const lifecycleEvents = publish.mock.calls
      .filter(([topic]) => String(topic).startsWith('workflow.lifecycle.v1.'))
      .map(([, event]) => (event as any).data?.event)
      .filter(Boolean);
    expect(lifecycleEvents.filter(event => event.type === 'workflow.finished')).toHaveLength(1);
  } finally {
    releaseStep.resolve();
    releaseAcknowledgement.resolve();
    update.mockRestore();
    await mastra.shutdown();
  }
});

it('keeps overlapping durable cancellations joined to the active cancellation admission', async () => {
  const storage = new MockStore();
  const pubsub = new EventEmitterPubSub();
  const entered = deferred<void>();
  const releaseStep = deferred<void>();
  const stepFinished = deferred<void>();
  const bothSnapshotsRead = deferred<void>();
  const releaseSnapshots = deferred<void>();
  const admissionCalled = deferred<void>();
  const releaseAcknowledgement = deferred<void>();
  const onFinish = vi.fn(async (result: { status: string }) => {
    expect(result.status).toBe('canceled');
  });
  const workflowId = 'conditional-default-terminal-overlapping-cancel';
  const runId = `${workflowId}-run`;
  const step = createStep({
    id: 'overlap-blocking-step',
    inputSchema: z.object({}),
    outputSchema: z.object({}),
    execute: async () => {
      entered.resolve();
      await releaseStep.promise;
      stepFinished.resolve();
      return {};
    },
  });
  const workflow = createWorkflow({
    id: workflowId,
    inputSchema: z.object({}),
    outputSchema: z.object({}),
    options: { onFinish },
  })
    .then(step)
    .commit();
  const mastra = new Mastra({ logger: false, storage, pubsub, workflows: { [workflowId]: workflow } });
  const run = await workflow.createRun({ runId });
  const workflowsStore = (await storage.getStore('workflows'))!;
  const originalAdmission = (run as any).isCancellationAdmitted.bind(run);
  (run as any).isCancellationAdmitted = (...args: any[]) => {
    const admission = originalAdmission(...args);
    admissionCalled.resolve();
    return admission;
  };
  const originalLoad = workflowsStore.loadWorkflowSnapshot.bind(workflowsStore);
  let snapshotsRead = 0;
  let cancellationsStarted = false;
  const load = vi.spyOn(workflowsStore, 'loadWorkflowSnapshot').mockImplementation(async args => {
    const snapshot = await originalLoad(args);
    if (cancellationsStarted && snapshot?.status === 'running' && snapshotsRead < 2) {
      snapshotsRead++;
      if (snapshotsRead === 2) bothSnapshotsRead.resolve();
      await releaseSnapshots.promise;
    }
    return snapshot;
  });
  const originalUpdate = workflowsStore.updateWorkflowState.bind(workflowsStore);
  const releaseAcknowledgements = vi.spyOn(workflowsStore, 'updateWorkflowState').mockImplementation(async args => {
    const committed = await originalUpdate(args);
    if (args.opts.status === 'canceled' && committed) await releaseAcknowledgement.promise;
    return committed;
  });

  try {
    const started = run.start({ inputData: {} });
    await entered.promise;
    cancellationsStarted = true;
    const cancelOne = run.cancel();
    const cancelTwo = run.cancel();
    await bothSnapshotsRead.promise;
    releaseSnapshots.resolve();
    releaseStep.resolve();
    await stepFinished.promise;
    await admissionCalled.promise;
    expect(onFinish).not.toHaveBeenCalled();
    releaseAcknowledgement.resolve();
    await Promise.all([cancelOne, cancelTwo]);
    await expect(started).resolves.toMatchObject({ status: 'canceled' });
    expect(onFinish).toHaveBeenCalledOnce();
  } finally {
    releaseSnapshots.resolve();
    releaseStep.resolve();
    releaseAcknowledgement.resolve();
    releaseAcknowledgements.mockRestore();
    load.mockRestore();
    await mastra.shutdown();
  }
});

it('retains a remote tripwire as top-level terminal data without a synthetic step result', async () => {
  const storage = new MockStore();
  const pubsub = new EventEmitterPubSub();
  const workflowId = 'conditional-default-terminal-tripwire';
  const runId = `${workflowId}-run`;
  const workflow = makeWorkflow(workflowId);
  const mastra = new Mastra({ logger: false, storage, pubsub, workflows: { [workflowId]: workflow } });
  const run = await workflow.createRun({ runId });
  const workflowsStore = (await storage.getStore('workflows'))!;
  const originalPersist = workflowsStore.persistWorkflowStepUpdate.bind(workflowsStore);
  let injected = false;
  const persist = vi.spyOn(workflowsStore, 'persistWorkflowStepUpdate').mockImplementation(async args => {
    if (!injected && args.snapshot.status === 'success') {
      injected = true;
      await workflowsStore.updateWorkflowState({
        workflowName: workflowId,
        runId,
        opts: {
          status: 'tripwire',
          error: { name: 'RemoteTripwire', message: 'remote policy winner' },
          tripwire: { reason: 'remote policy winner', retry: false },
          expectedExecutionGeneration: args.snapshot.executionGeneration,
        } as any,
      });
    }
    return originalPersist(args);
  });

  try {
    const result = await run.start({ inputData: {} });
    const final = await workflowsStore.loadWorkflowSnapshot({ workflowName: workflowId, runId });
    expect(injected).toBe(true);
    expect(final).toMatchObject({
      status: 'tripwire',
      error: { name: 'RemoteTripwire', message: 'remote policy winner' },
      tripwire: { reason: 'remote policy winner', retry: false },
    });
    expect(final?.context['terminal-step']).not.toHaveProperty('tripwire');
    expect(result.status).toBe('tripwire');
    if (result.status === 'tripwire') {
      expect(result.tripwire).toEqual({ reason: 'remote policy winner', retry: false });
    }
  } finally {
    persist.mockRestore();
    await mastra.shutdown();
  }
});
