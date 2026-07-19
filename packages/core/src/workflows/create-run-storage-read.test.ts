import { describe, expect, it, vi } from 'vitest';
import { z } from 'zod/v4';
import { EventEmitterPubSub } from '../events/event-emitter';
import { Mastra } from '../mastra';
import { MockStore } from '../storage/mock';
import { PROCESSOR_EXECUTION_SYMBOL, PUBSUB_SYMBOL, TRANSIENT_EXECUTION_SYMBOL } from './constants';
import { createWorkflow as createEventedWorkflow } from './evented';
import type { WorkflowRunState } from './types';
import { createStep, createWorkflow } from './index';

const ioSchema = z.object({ value: z.string() });

function defineCreateRunStorageReadTests(
  engine: string,
  workflowFactory: (config: Record<string, unknown>) => ReturnType<typeof createWorkflow>,
) {
  const buildWorkflow = (id: string, shouldPersistSnapshot: boolean) =>
    workflowFactory({
      id,
      inputSchema: ioSchema,
      outputSchema: ioSchema,
      options: { shouldPersistSnapshot: () => shouldPersistSnapshot },
    })
      .then(
        createStep({
          id: 'passthrough',
          inputSchema: ioSchema,
          outputSchema: ioSchema,
          execute: async ({ inputData }) => inputData,
        }),
      )
      .commit();

  describe(`${engine} createRun storage existence read`, () => {
    it('skips a guaranteed-miss read for a generated non-persisting run', async () => {
      const storage = new MockStore();
      const workflow = buildWorkflow(`${engine}-transient-generated`, false);
      new Mastra({ logger: false, storage, workflows: { [workflow.id]: workflow } });
      const workflowsStore = (await storage.getStore('workflows'))!;
      const read = vi.spyOn(workflowsStore, 'getWorkflowRunById');
      const persist = vi.spyOn(workflowsStore, 'persistWorkflowSnapshot');

      await workflow.createRun();

      expect(read).not.toHaveBeenCalled();
      expect(persist).not.toHaveBeenCalled();
    });

    it('still reads storage for a persisting workflow', async () => {
      const storage = new MockStore();
      const workflow = buildWorkflow(`${engine}-persisting-generated`, true);
      new Mastra({ logger: false, storage, workflows: { [workflow.id]: workflow } });
      const workflowsStore = (await storage.getStore('workflows'))!;
      const read = vi.spyOn(workflowsStore, 'getWorkflowRunById');

      await workflow.createRun();

      expect(read).toHaveBeenCalledTimes(1);
    });

    it('still reads storage when a non-persisting workflow receives an explicit runId', async () => {
      const storage = new MockStore();
      const workflow = buildWorkflow(`${engine}-transient-explicit`, false);
      new Mastra({ logger: false, storage, workflows: { [workflow.id]: workflow } });
      const workflowsStore = (await storage.getStore('workflows'))!;
      const read = vi.spyOn(workflowsStore, 'getWorkflowRunById');

      await workflow.createRun({ runId: 'caller-owned-run-id' });

      expect(read).toHaveBeenCalledTimes(1);
    });
  });
}

defineCreateRunStorageReadTests('default', createWorkflow);
defineCreateRunStorageReadTests('evented', createEventedWorkflow as never);

function buildExplicitlyTransientWorkflow(id: string) {
  return createWorkflow({
    id,
    inputSchema: ioSchema,
    outputSchema: ioSchema,
    options: { executionMode: 'transient' },
  })
    .then(
      createStep({
        id: 'passthrough',
        inputSchema: ioSchema,
        outputSchema: ioSchema,
        execute: async ({ inputData }) => inputData,
      }),
    )
    .commit();
}

function createApprovalStep(id: string) {
  return createStep({
    id,
    inputSchema: ioSchema,
    outputSchema: ioSchema,
    suspendSchema: z.object({ waiting: z.boolean() }),
    resumeSchema: z.object({ approved: z.boolean() }),
    execute: async ({ inputData, resumeData, suspend }) => {
      if (!resumeData?.approved) {
        await suspend({ waiting: true });
      }
      return inputData;
    },
  });
}

describe('explicitly transient workflow lifecycle reads', () => {
  it('keeps transient processor lifecycle events off the registered Mastra pubsub', async () => {
    const workflow = buildExplicitlyTransientWorkflow('transient-processor-local-pubsub');
    const storage = new MockStore();
    const configuredPubsub = new EventEmitterPubSub();
    new Mastra({
      logger: false,
      pubsub: configuredPubsub,
      storage,
      workflows: { [workflow.id]: workflow },
    });
    const publish = vi.spyOn(configuredPubsub, 'publish');
    const persistStepUpdate = vi.spyOn((workflow as any).executionEngine, 'persistStepUpdate');

    const run = await workflow.createRun({ [PROCESSOR_EXECUTION_SYMBOL]: true });
    await run.start({ inputData: { value: 'local' } });

    expect(run.transientExecution).toBe(true);
    expect(publish).not.toHaveBeenCalled();
    expect(persistStepUpdate).not.toHaveBeenCalled();
  });

  it('rejects an evented child inherited by a transient parent before durable admission', async () => {
    const step = createStep({
      id: 'evented-child-step',
      inputSchema: ioSchema,
      outputSchema: ioSchema,
      execute: async ({ inputData }) => inputData,
    });
    const workflow = createEventedWorkflow({
      id: 'evented-child-of-transient-parent',
      inputSchema: ioSchema,
      outputSchema: ioSchema,
    })
      .then(step)
      .commit();
    const storage = new MockStore();
    new Mastra({ logger: false, storage, workflows: { [workflow.id]: workflow } });
    const workflowsStore = (await storage.getStore('workflows'))!;
    const lookup = vi.spyOn(workflowsStore, 'getWorkflowRunById');
    const persist = vi.spyOn(workflowsStore, 'persistWorkflowSnapshot');

    await expect(workflow.createRun({ [TRANSIENT_EXECUTION_SYMBOL]: true })).rejects.toThrow(
      'Evented workflows cannot run inside transient workflows',
    );
    expect(lookup).not.toHaveBeenCalled();
    expect(persist).not.toHaveBeenCalled();
  });

  it('skips lifecycle reads for a built-in generated run ID', async () => {
    const workflow = buildExplicitlyTransientWorkflow('explicit-transient-generated');
    const storage = new MockStore();
    new Mastra({ logger: false, storage, workflows: { [workflow.id]: workflow } });
    const workflowsStore = (await storage.getStore('workflows'))!;
    const read = vi.spyOn(workflowsStore, 'loadWorkflowSnapshot');
    const persist = vi.spyOn(workflowsStore, 'persistWorkflowSnapshot');

    const run = await workflow.createRun();
    await run.start({ inputData: { value: 'generated' } });

    expect(run.transientExecution).toBe(true);
    expect(read).not.toHaveBeenCalled();
    expect(persist).not.toHaveBeenCalled();
  });

  it('preserves a completed transient run status when cancel is called later', async () => {
    const workflow = buildExplicitlyTransientWorkflow('explicit-transient-cancel-after-success');
    const storage = new MockStore();
    new Mastra({ logger: false, storage, workflows: { [workflow.id]: workflow } });
    const workflowsStore = (await storage.getStore('workflows'))!;
    const read = vi.spyOn(workflowsStore, 'loadWorkflowSnapshot');
    const update = vi.spyOn(workflowsStore, 'updateWorkflowState');

    const run = await workflow.createRun();
    const lifecycleEvents: string[] = [];
    const unwatch = run.watch(event => lifecycleEvents.push(event.type));
    await expect(run.start({ inputData: { value: 'complete' } })).resolves.toMatchObject({ status: 'success' });
    const eventsAfterCompletion = [...lifecycleEvents];
    await run.cancel();
    unwatch();

    expect(run.workflowRunStatus).toBe('success');
    expect(lifecycleEvents).toEqual(eventsAfterCompletion);
    expect(read).not.toHaveBeenCalled();
    expect(update).not.toHaveBeenCalled();
  });

  it('preserves a transient cancellation that arrives during terminal finalization', async () => {
    const workflow = buildExplicitlyTransientWorkflow('explicit-transient-late-cancel');
    const storage = new MockStore();
    new Mastra({ logger: false, storage, workflows: { [workflow.id]: workflow } });
    const workflowsStore = (await storage.getStore('workflows'))!;
    const read = vi.spyOn(workflowsStore, 'loadWorkflowSnapshot');
    const update = vi.spyOn(workflowsStore, 'updateWorkflowState');
    const persist = vi.spyOn(workflowsStore, 'persistWorkflowSnapshot');

    let markTerminalFormatted!: () => void;
    const terminalFormatted = new Promise<void>(resolve => {
      markTerminalFormatted = resolve;
    });
    let releaseFinalization!: () => void;
    const finalizationGate = new Promise<void>(resolve => {
      releaseFinalization = resolve;
    });
    const executionEngine = (workflow as any).executionEngine;
    const fmtReturnValue = executionEngine.fmtReturnValue.bind(executionEngine);
    vi.spyOn(executionEngine, 'fmtReturnValue').mockImplementation(async (...args: any[]) => {
      const result = await fmtReturnValue(...args);
      if (result.status === 'success') {
        markTerminalFormatted();
        await finalizationGate;
      }
      return result;
    });

    const run = await workflow.createRun();
    const resultPromise = run.start({ inputData: { value: 'cancel-during-finalization' } });
    await terminalFormatted;
    await run.cancel();
    releaseFinalization();

    await expect(resultPromise).resolves.toMatchObject({ status: 'canceled', result: undefined });
    expect(run.workflowRunStatus).toBe('canceled');
    expect(run.abortController.signal.aborted).toBe(true);
    expect(read).not.toHaveBeenCalled();
    expect(update).not.toHaveBeenCalled();
    expect(persist).not.toHaveBeenCalled();
  });

  it('preserves a transient cancellation that arrives during terminal lifecycle callbacks', async () => {
    const workflow = buildExplicitlyTransientWorkflow('explicit-transient-callback-cancel');
    new Mastra({ logger: false, workflows: { [workflow.id]: workflow } });
    const executionEngine = (workflow as any).executionEngine;
    const invokeLifecycleCallbacks = executionEngine.invokeLifecycleCallbacks.bind(executionEngine);
    let markCallbackStarted!: () => void;
    const callbackStarted = new Promise<void>(resolve => {
      markCallbackStarted = resolve;
    });
    let releaseCallback!: () => void;
    const callbackGate = new Promise<void>(resolve => {
      releaseCallback = resolve;
    });
    vi.spyOn(executionEngine, 'invokeLifecycleCallbacks').mockImplementation(async (...args: any[]) => {
      markCallbackStarted();
      await callbackGate;
      return invokeLifecycleCallbacks(...args);
    });

    const run = await workflow.createRun();
    const resultPromise = run.start({ inputData: { value: 'cancel-during-callback' } });
    await callbackStarted;
    await run.cancel();
    releaseCallback();

    await expect(resultPromise).resolves.toMatchObject({ status: 'canceled', result: undefined });
    expect(run.workflowRunStatus).toBe('canceled');
    expect(run.abortController.signal.aborted).toBe(true);
  });

  it('preserves a transient cancellation that arrives during failed lifecycle callbacks', async () => {
    const failingStep = createStep({
      id: 'failed-callback-step',
      inputSchema: ioSchema,
      outputSchema: ioSchema,
      execute: async () => {
        throw new Error('expected failure');
      },
    });
    const workflow = createWorkflow({
      id: 'explicit-transient-failed-callback-cancel',
      inputSchema: ioSchema,
      outputSchema: ioSchema,
      steps: [failingStep],
      options: { executionMode: 'transient' },
    })
      .then(failingStep)
      .commit();
    new Mastra({ logger: false, workflows: { [workflow.id]: workflow } });
    const executionEngine = (workflow as any).executionEngine;
    const invokeLifecycleCallbacks = executionEngine.invokeLifecycleCallbacks.bind(executionEngine);
    let markCallbackStarted!: () => void;
    const callbackStarted = new Promise<void>(resolve => {
      markCallbackStarted = resolve;
    });
    let releaseCallback!: () => void;
    const callbackGate = new Promise<void>(resolve => {
      releaseCallback = resolve;
    });
    vi.spyOn(executionEngine, 'invokeLifecycleCallbacks').mockImplementation(async (...args: any[]) => {
      markCallbackStarted();
      await callbackGate;
      return invokeLifecycleCallbacks(...args);
    });

    const run = await workflow.createRun();
    const resultPromise = run.start({ inputData: { value: 'cancel-failed-callback' } });
    await callbackStarted;
    await run.cancel();
    releaseCallback();

    await expect(resultPromise).resolves.toMatchObject({ status: 'canceled', result: undefined, error: undefined });
    expect(run.workflowRunStatus).toBe('canceled');
    expect(run.abortController.signal.aborted).toBe(true);
  });

  it('admits only one concurrent start for the same transient Run instance', async () => {
    let executionCount = 0;
    let markExecutionStarted!: () => void;
    const executionStarted = new Promise<void>(resolve => {
      markExecutionStarted = resolve;
    });
    let releaseExecution!: () => void;
    const executionGate = new Promise<void>(resolve => {
      releaseExecution = resolve;
    });
    const guardedStep = createStep({
      id: 'single-admission-step',
      inputSchema: ioSchema,
      outputSchema: ioSchema,
      execute: async ({ inputData }) => {
        executionCount += 1;
        markExecutionStarted();
        await executionGate;
        return inputData;
      },
    });
    const workflow = createWorkflow({
      id: 'explicit-transient-concurrent-start',
      inputSchema: ioSchema,
      outputSchema: ioSchema,
      steps: [guardedStep],
      options: { executionMode: 'transient' },
    })
      .then(guardedStep)
      .commit();
    new Mastra({ logger: false, workflows: { [workflow.id]: workflow } });
    const run = await workflow.createRun();

    const winningResult = run.start({ inputData: { value: 'first' } });
    await executionStarted;
    await expect(run.start({ inputData: { value: 'second' } })).rejects.toThrow('admission is stale');

    // The rejected start shares the same Run object, so it must not retire the
    // cache entry still owned by the active execution.
    expect(workflow.runs.get(run.runId)).toBe(run);

    releaseExecution();
    await expect(winningResult).resolves.toMatchObject({
      status: 'success',
      result: { value: 'first' },
    });

    expect(executionCount).toBe(1);
    expect(workflow.runs.has(run.runId)).toBe(false);
  });

  it('does not cancel after terminal lifecycle publication has committed its status', async () => {
    const workflow = buildExplicitlyTransientWorkflow('explicit-transient-terminal-publication-commit');
    const configuredPubsub = new EventEmitterPubSub();
    const publish = configuredPubsub.publish.bind(configuredPubsub);
    let markFinishedPublicationStarted!: () => void;
    const finishedPublicationStarted = new Promise<void>(resolve => {
      markFinishedPublicationStarted = resolve;
    });
    let releaseFinishedPublication!: () => void;
    const finishedPublicationGate = new Promise<void>(resolve => {
      releaseFinishedPublication = resolve;
    });
    vi.spyOn(configuredPubsub, 'publish').mockImplementation(async (topic, event, options) => {
      if (event.data?.event?.type === 'workflow.finished') {
        markFinishedPublicationStarted();
        await finishedPublicationGate;
      }
      return publish(topic, event, options);
    });
    new Mastra({
      logger: false,
      pubsub: configuredPubsub,
      workflows: { [workflow.id]: workflow },
    });

    const run = await workflow.createRun();
    const resultPromise = run.start({ inputData: { value: 'terminal-publication' } });
    await finishedPublicationStarted;
    await run.cancel();
    releaseFinishedPublication();

    await expect(resultPromise).resolves.toMatchObject({
      status: 'success',
      result: { value: 'terminal-publication' },
    });
    expect(run.workflowRunStatus).toBe('success');
    expect(run.abortController.signal.aborted).toBe(false);
  });

  it('rejects reusing an in-memory run ID with a different execution mode', async () => {
    const workflow = buildExplicitlyTransientWorkflow('explicit-transient-mode-reuse');
    const storage = new MockStore();
    new Mastra({ logger: false, storage, workflows: { [workflow.id]: workflow } });
    const workflowsStore = (await storage.getStore('workflows'))!;
    const lookup = vi.spyOn(workflowsStore, 'getWorkflowRunById');

    const transientRun = await workflow.createRun();
    await expect(workflow.createRun({ runId: transientRun.runId })).rejects.toThrow(
      `Workflow run ${workflow.id}/${transientRun.runId} cannot change execution mode`,
    );

    const durableRun = await workflow.createRun({ runId: 'durable-mode-reuse' });
    await expect(workflow.createRun({ runId: durableRun.runId, [TRANSIENT_EXECUTION_SYMBOL]: true })).rejects.toThrow(
      `Workflow run ${workflow.id}/${durableRun.runId} cannot change execution mode`,
    );
    expect(lookup).toHaveBeenCalledTimes(1);
  });

  it('propagates transient execution through parallel child contexts', async () => {
    const first = createStep({
      id: 'parallel-first',
      inputSchema: ioSchema,
      outputSchema: ioSchema,
      execute: async ({ inputData }) => inputData,
    });
    const second = createStep({
      id: 'parallel-second',
      inputSchema: ioSchema,
      outputSchema: ioSchema,
      execute: async ({ inputData }) => inputData,
    });
    const workflow = createWorkflow({
      id: 'explicit-transient-parallel',
      inputSchema: ioSchema,
      outputSchema: z.any(),
      options: { executionMode: 'transient' },
    })
      .parallel([first, second])
      .commit();
    const storage = new MockStore();
    new Mastra({ logger: false, storage, workflows: { [workflow.id]: workflow } });
    const workflowsStore = (await storage.getStore('workflows'))!;
    const read = vi.spyOn(workflowsStore, 'loadWorkflowSnapshot');

    const run = await workflow.createRun();
    await run.start({ inputData: { value: 'parallel' } });

    expect(run.transientExecution).toBe(true);
    expect(read).not.toHaveBeenCalled();
  });

  it('cancels a transient run without creating durable state or allowing a later start', async () => {
    const workflow = buildExplicitlyTransientWorkflow('explicit-transient-cancel');
    const storage = new MockStore();
    new Mastra({ logger: false, storage, workflows: { [workflow.id]: workflow } });
    const workflowsStore = (await storage.getStore('workflows'))!;
    const read = vi.spyOn(workflowsStore, 'loadWorkflowSnapshot');
    const update = vi.spyOn(workflowsStore, 'updateWorkflowState');
    const persist = vi.spyOn(workflowsStore, 'persistWorkflowSnapshot');

    const run = await workflow.createRun();
    await run.cancel();
    const replacement = await workflow.createRun({
      runId: run.runId,
      [TRANSIENT_EXECUTION_SYMBOL]: true,
    });

    expect(run.workflowRunStatus).toBe('canceled');
    expect(replacement).not.toBe(run);
    expect(replacement.transientExecution).toBe(true);
    await expect(run.start({ inputData: { value: 'must-not-run' } })).rejects.toThrow(
      'lifecycle execution admission is stale',
    );
    const cachedReplacement = await workflow.createRun({
      runId: run.runId,
      [TRANSIENT_EXECUTION_SYMBOL]: true,
    });
    expect(run.workflowRunStatus).toBe('canceled');
    expect(cachedReplacement).toBe(replacement);
    expect(read).not.toHaveBeenCalled();
    expect(update).not.toHaveBeenCalled();
    expect(persist).not.toHaveBeenCalled();
    await replacement.cancel();
  });

  it('fails a transient suspension attempt without creating durable state', async () => {
    const approval = createApprovalStep('transient-approval');
    const workflow = createWorkflow({
      id: 'explicit-transient-suspend',
      inputSchema: ioSchema,
      outputSchema: ioSchema,
      steps: [approval],
      options: { executionMode: 'transient' },
    })
      .then(approval)
      .commit();
    const storage = new MockStore();
    new Mastra({ logger: false, storage, workflows: { [workflow.id]: workflow } });
    const workflowsStore = (await storage.getStore('workflows'))!;
    const read = vi.spyOn(workflowsStore, 'loadWorkflowSnapshot');
    const persist = vi.spyOn(workflowsStore, 'persistWorkflowSnapshot');

    const run = await workflow.createRun();
    const result = await run.start({ inputData: { value: 'suspend' } });

    expect(result).toMatchObject({
      status: 'failed',
      error: expect.objectContaining({ message: 'Transient workflow runs cannot suspend' }),
    });
    expect(read).not.toHaveBeenCalled();
    expect(persist).not.toHaveBeenCalled();
  });

  it('propagates transient execution into nested workflows without storage cleanup calls', async () => {
    const childStep = createStep({
      id: 'nested-transient-suspend-step',
      inputSchema: ioSchema,
      outputSchema: ioSchema,
      execute: async ({ inputData, suspend }) => {
        await suspend();
        return inputData;
      },
    });
    const child = createWorkflow({
      id: 'nested-transient-child',
      inputSchema: ioSchema,
      outputSchema: ioSchema,
      steps: [childStep],
    })
      .then(childStep)
      .commit();
    const parent = createWorkflow({
      id: 'nested-transient-parent',
      inputSchema: ioSchema,
      outputSchema: ioSchema,
      steps: [child],
      options: { executionMode: 'transient' },
    })
      .then(child)
      .commit();
    const storage = new MockStore();
    new Mastra({ logger: false, storage, workflows: { [parent.id]: parent } });
    const workflowsStore = (await storage.getStore('workflows'))!;
    const read = vi.spyOn(workflowsStore, 'loadWorkflowSnapshot');
    const lookup = vi.spyOn(workflowsStore, 'getWorkflowRunById');
    const persist = vi.spyOn(workflowsStore, 'persistWorkflowSnapshot');
    const remove = vi.spyOn(workflowsStore, 'deleteWorkflowRunById');

    const run = await parent.createRun();
    const result = await run.start({ inputData: { value: 'nested-suspend' } });

    expect(result).toMatchObject({
      status: 'failed',
      error: expect.objectContaining({ message: 'Transient workflow runs cannot suspend' }),
    });
    expect(read).not.toHaveBeenCalled();
    expect(lookup).not.toHaveBeenCalled();
    expect(persist).not.toHaveBeenCalled();
    expect(remove).not.toHaveBeenCalled();
  });

  it('keeps process-local lifecycle suppression through nested processor workflows', async () => {
    const childStep = createStep({
      id: 'nested-suppressed-child-step',
      inputSchema: ioSchema,
      outputSchema: ioSchema,
      execute: async ({ inputData }) => inputData,
    });
    const child = createWorkflow({
      id: 'nested-suppressed-child',
      inputSchema: ioSchema,
      outputSchema: ioSchema,
      steps: [childStep],
    })
      .then(childStep)
      .commit();
    const parent = createWorkflow({
      id: 'nested-suppressed-parent',
      inputSchema: ioSchema,
      outputSchema: ioSchema,
      steps: [child],
      type: 'processor',
    })
      .then(child)
      .commit();
    new Mastra({ logger: false, workflows: { [parent.id]: parent } });
    const eventEmitterPublish = vi.spyOn(EventEmitterPubSub.prototype, 'publish');
    const parentDurableOperation = vi.spyOn((parent as any).executionEngine, 'wrapDurableOperation');
    const childDurableOperation = vi.spyOn((child as any).executionEngine, 'wrapDurableOperation');
    const parentStepStart = vi.spyOn((parent as any).executionEngine, 'onStepExecutionStart');
    const childStepStart = vi.spyOn((child as any).executionEngine, 'onStepExecutionStart');
    const parentDisposition = vi.spyOn((parent as any).executionEngine, 'getAuthoritativeExecutionDisposition');
    const childDisposition = vi.spyOn((child as any).executionEngine, 'getAuthoritativeExecutionDisposition');

    try {
      const run = await parent.createRun({ [PROCESSOR_EXECUTION_SYMBOL]: true });
      await expect(run.start({ inputData: { value: 'nested' } })).resolves.toMatchObject({ status: 'success' });

      expect(eventEmitterPublish).not.toHaveBeenCalled();
      expect(
        [...parentDurableOperation.mock.calls, ...childDurableOperation.mock.calls].filter(([operationId]) =>
          String(operationId).endsWith('.emit_result'),
        ),
      ).toEqual([]);
      expect(parentStepStart).not.toHaveBeenCalled();
      expect(childStepStart).not.toHaveBeenCalled();
      expect(parentDisposition).not.toHaveBeenCalled();
      expect(childDisposition).not.toHaveBeenCalled();
    } finally {
      eventEmitterPublish.mockRestore();
    }
  });

  it('skips the platform nested-result wrapper when processor lifecycle events are suppressed', async () => {
    const childStep = createStep({
      id: 'platform-nested-suppressed-child-step',
      inputSchema: ioSchema,
      outputSchema: ioSchema,
      execute: async ({ inputData }) => inputData,
    });
    const child = createWorkflow({
      id: 'platform-nested-suppressed-child',
      inputSchema: ioSchema,
      outputSchema: ioSchema,
      steps: [childStep],
    })
      .then(childStep)
      .commit();
    const parent = createWorkflow({
      id: 'platform-nested-suppressed-parent',
      inputSchema: ioSchema,
      outputSchema: ioSchema,
      steps: [child],
      type: 'processor',
    })
      .then(child)
      .commit();
    new Mastra({ logger: false, workflows: { [parent.id]: parent } });
    const engine = (parent as any).executionEngine;
    vi.spyOn(engine, 'isNestedWorkflowStep').mockReturnValue(true);
    const nestedInvocation = vi.spyOn(engine, 'executeWorkflowStep').mockResolvedValue({
      status: 'success',
      output: { value: 'platform-result' },
      endedAt: Date.now(),
    });
    const durableOperation = vi.spyOn(engine, 'wrapDurableOperation');

    const run = await parent.createRun({ [PROCESSOR_EXECUTION_SYMBOL]: true });
    await expect(run.start({ inputData: { value: 'nested' } })).resolves.toMatchObject({
      status: 'success',
      result: { value: 'platform-result' },
    });

    expect(nestedInvocation).toHaveBeenCalledOnce();
    expect(durableOperation.mock.calls.filter(([operationId]) => String(operationId).endsWith('.emit_result'))).toEqual(
      [],
    );
  });

  it('keeps transient foreach processor runs off legacy lifecycle emit paths', async () => {
    const worker = createStep({
      id: 'transient-foreach-worker',
      inputSchema: z.string(),
      outputSchema: z.string(),
      execute: async ({ inputData }) => inputData.toUpperCase(),
    });
    const workflow = createWorkflow({
      id: 'transient-foreach-processor',
      inputSchema: z.array(z.string()),
      outputSchema: z.array(z.string()),
      steps: [worker],
      options: { executionMode: 'transient' },
    })
      .foreach(worker, { concurrency: 2 })
      .commit();
    new Mastra({ logger: false, workflows: { [workflow.id]: workflow } });
    const durableOperation = vi.spyOn((workflow as any).executionEngine, 'wrapDurableOperation');

    const run = await workflow.createRun({ [PROCESSOR_EXECUTION_SYMBOL]: true });
    await expect(run.start({ inputData: ['first', 'second', 'third'] })).resolves.toMatchObject({
      status: 'success',
      result: ['FIRST', 'SECOND', 'THIRD'],
    });

    expect(
      durableOperation.mock.calls.filter(([operationId]) =>
        ['.running_ev', '.emit_result'].some(suffix => String(operationId).endsWith(suffix)),
      ),
    ).toEqual([]);
  });

  it('isolates concurrent nested workflow runs inside a transient foreach', async () => {
    const childRunIds: string[] = [];
    let markFirstChildStarted!: () => void;
    const firstChildStarted = new Promise<void>(resolve => {
      markFirstChildStarted = resolve;
    });
    let releaseChildren!: () => void;
    const childrenGate = new Promise<void>(resolve => {
      releaseChildren = resolve;
    });
    const childStep = createStep({
      id: 'nested-transient-foreach-child-step',
      inputSchema: z.string(),
      outputSchema: z.string(),
      execute: async ({ inputData, runId }) => {
        childRunIds.push(runId);
        markFirstChildStarted();
        await childrenGate;
        return inputData.toUpperCase();
      },
    });
    const child = createWorkflow({
      id: 'nested-transient-foreach-child',
      inputSchema: z.string(),
      outputSchema: z.string(),
      steps: [childStep],
    })
      .then(childStep)
      .commit();
    const parent = createWorkflow({
      id: 'nested-transient-foreach-parent',
      inputSchema: z.array(z.string()),
      outputSchema: z.array(z.string()),
      steps: [child],
      options: { executionMode: 'transient' },
    })
      .foreach(child, { concurrency: 2 })
      .commit();
    new Mastra({ logger: false, workflows: { [parent.id]: parent } });

    const run = await parent.createRun();
    const resultPromise = run.start({ inputData: ['first', 'second'] });
    await firstChildStarted;
    // Give the sibling worker a turn while the first nested run is still active.
    await new Promise(resolve => setTimeout(resolve, 0));
    releaseChildren();

    await expect(resultPromise).resolves.toMatchObject({
      status: 'success',
      result: ['FIRST', 'SECOND'],
    });
    expect(childRunIds).toHaveLength(2);
    expect(new Set(childRunIds).size).toBe(2);
    expect(child.runs.size).toBe(0);
  });

  it('honors a transient child nested under a durable parent', async () => {
    const childStep = createStep({
      id: 'nested-own-transient-step',
      inputSchema: ioSchema,
      outputSchema: ioSchema,
      execute: async ({ inputData }) => inputData,
    });
    const child = createWorkflow({
      id: 'nested-own-transient-child',
      inputSchema: ioSchema,
      outputSchema: ioSchema,
      steps: [childStep],
      options: { executionMode: 'transient' },
    })
      .then(childStep)
      .commit();
    const parent = createWorkflow({
      id: 'durable-parent-with-transient-child',
      inputSchema: ioSchema,
      outputSchema: ioSchema,
      steps: [child],
    })
      .then(child)
      .commit();
    const storage = new MockStore();
    new Mastra({ logger: false, storage, workflows: { [parent.id]: parent } });
    const workflowsStore = (await storage.getStore('workflows'))!;
    const lookup = vi.spyOn(workflowsStore, 'getWorkflowRunById');
    const persist = vi.spyOn(workflowsStore, 'persistWorkflowSnapshot');
    const remove = vi.spyOn(workflowsStore, 'deleteWorkflowRunById');

    const run = await parent.createRun();
    await expect(run.start({ inputData: { value: 'nested' } })).resolves.toMatchObject({ status: 'success' });

    expect(lookup.mock.calls.filter(([input]) => input.workflowName === child.id)).toEqual([]);
    expect(persist.mock.calls.filter(([input]) => input.workflowName === child.id)).toEqual([]);
    expect(remove.mock.calls.filter(([input]) => input.workflowName === child.id)).toEqual([]);
  });

  it('rejects per-step execution before a transient run can become unrecoverably paused', async () => {
    const workflow = buildExplicitlyTransientWorkflow('explicit-transient-per-step');
    const storage = new MockStore();
    new Mastra({ logger: false, storage, workflows: { [workflow.id]: workflow } });
    const workflowsStore = (await storage.getStore('workflows'))!;
    const read = vi.spyOn(workflowsStore, 'loadWorkflowSnapshot');
    const lookup = vi.spyOn(workflowsStore, 'getWorkflowRunById');
    const persist = vi.spyOn(workflowsStore, 'persistWorkflowSnapshot');

    const run = await workflow.createRun();
    expect(workflow.runs.size).toBe(1);

    await expect(run.start({ inputData: { value: 'per-step' }, perStep: true })).rejects.toThrow(
      'Transient workflow runs cannot use per-step execution',
    );
    expect(run.workflowRunStatus).toBe('pending');
    expect(workflow.runs.size).toBe(0);
    expect(read).not.toHaveBeenCalled();
    expect(lookup).not.toHaveBeenCalled();
    expect(persist).not.toHaveBeenCalled();
  });

  it('retires a transient run when workflow input validation rejects', async () => {
    const workflow = createWorkflow({
      id: 'explicit-transient-invalid-input',
      inputSchema: ioSchema,
      outputSchema: ioSchema,
      options: { executionMode: 'transient', validateInputs: true },
    })
      .then(
        createStep({
          id: 'validated-passthrough',
          inputSchema: ioSchema,
          outputSchema: ioSchema,
          execute: async ({ inputData }) => inputData,
        }),
      )
      .commit();
    const run = await workflow.createRun();

    await expect(run.start({ inputData: { value: 1 } as never })).rejects.toThrow('Invalid input data');

    expect(workflow.runs.size).toBe(0);
  });

  it('rejects durable replay operations without reading storage', async () => {
    const workflow = buildExplicitlyTransientWorkflow('explicit-transient-replay');
    const storage = new MockStore();
    new Mastra({ logger: false, storage, workflows: { [workflow.id]: workflow } });
    const workflowsStore = (await storage.getStore('workflows'))!;
    const read = vi.spyOn(workflowsStore, 'loadWorkflowSnapshot');

    const run = await workflow.createRun();

    await expect(run.resume({ resumeData: { value: 'resume' } })).rejects.toThrow(
      'Transient workflow runs cannot resume',
    );
    await expect(run.restart()).rejects.toThrow('Transient workflow runs cannot restart');
    await expect(run.timeTravel({ step: 'passthrough', inputData: { value: 'travel' } })).rejects.toThrow(
      'Transient workflow runs cannot time travel',
    );
    expect(read).not.toHaveBeenCalled();
  });

  it('retains lifecycle reads for an explicit run ID', async () => {
    const workflow = buildExplicitlyTransientWorkflow('explicit-transient-caller-id');
    const storage = new MockStore();
    new Mastra({ logger: false, storage, workflows: { [workflow.id]: workflow } });
    const workflowsStore = (await storage.getStore('workflows'))!;
    const read = vi.spyOn(workflowsStore, 'loadWorkflowSnapshot');
    const persist = vi.spyOn(workflowsStore, 'persistWorkflowSnapshot');

    const run = await workflow.createRun({ runId: 'caller-owned-run-id' });
    await run.start({ inputData: { value: 'explicit' } });

    expect(run.transientExecution).toBe(false);
    expect(read).toHaveBeenCalled();
    expect(persist).toHaveBeenCalledTimes(1);
  });

  it('keeps an explicit run ID durable when execute is called without a transient marker', async () => {
    const workflow = buildExplicitlyTransientWorkflow('explicit-transient-direct-execute');
    const storage = new MockStore();
    const mastra = new Mastra({ logger: false, storage, workflows: { [workflow.id]: workflow } });
    const workflowsStore = (await storage.getStore('workflows'))!;
    const lookup = vi.spyOn(workflowsStore, 'getWorkflowRunById');
    const persist = vi.spyOn(workflowsStore, 'persistWorkflowSnapshot');

    await expect(
      workflow.execute({
        runId: 'direct-server-run-id',
        inputData: { value: 'server' },
        state: {},
        setState: vi.fn(async () => undefined),
        suspend: vi.fn(),
        [PUBSUB_SYMBOL]: new EventEmitterPubSub(),
        mastra,
        abort: vi.fn(),
        abortSignal: new AbortController().signal,
        engine: 'default',
        bail: vi.fn(),
      }),
    ).resolves.toEqual({ value: 'server' });

    expect(lookup).toHaveBeenCalledWith({ runId: 'direct-server-run-id', workflowName: workflow.id });
    expect(persist).toHaveBeenCalledTimes(1);
  });

  it('retains lifecycle reads for a custom-generated run ID', async () => {
    const workflow = buildExplicitlyTransientWorkflow('explicit-transient-custom-id');
    const storage = new MockStore();
    new Mastra({
      idGenerator: () => 'custom-generated-run-id',
      logger: false,
      storage,
      workflows: { [workflow.id]: workflow },
    });
    const workflowsStore = (await storage.getStore('workflows'))!;
    const read = vi.spyOn(workflowsStore, 'loadWorkflowSnapshot');
    const persist = vi.spyOn(workflowsStore, 'persistWorkflowSnapshot');

    const run = await workflow.createRun();
    await run.start({ inputData: { value: 'custom' } });

    expect(run.transientExecution).toBe(false);
    expect(read).toHaveBeenCalled();
    expect(persist).toHaveBeenCalledTimes(1);
  });

  it('preserves durable cancellation for an explicit run ID', async () => {
    const workflow = buildExplicitlyTransientWorkflow('explicit-transient-durable-cancel');
    const storage = new MockStore();
    new Mastra({ logger: false, storage, workflows: { [workflow.id]: workflow } });
    const workflowsStore = (await storage.getStore('workflows'))!;
    const update = vi.spyOn(workflowsStore, 'updateWorkflowState');

    const run = await workflow.createRun({ runId: 'durable-cancel-run-id' });
    await run.cancel();

    expect(run.transientExecution).toBe(false);
    expect(run.workflowRunStatus).toBe('canceled');
    expect(update).toHaveBeenCalled();
  });

  it('rejects when durable cancellation cannot be persisted', async () => {
    const workflow = buildExplicitlyTransientWorkflow('explicit-transient-durable-cancel-write-failure');
    const storage = new MockStore();
    const pubsub = new EventEmitterPubSub();
    new Mastra({ logger: false, storage, workflows: { [workflow.id]: workflow } });
    const workflowsStore = (await storage.getStore('workflows'))!;
    const update = vi.spyOn(workflowsStore, 'updateWorkflowState').mockRejectedValueOnce(new Error('storage offline'));
    const publish = vi.spyOn(pubsub, 'publish');
    const run = await workflow.createRun({ runId: 'durable-cancel-write-failure', pubsub });

    await expect(run.cancel()).rejects.toThrow('storage offline');

    expect(update).toHaveBeenCalledWith({
      workflowName: workflow.id,
      runId: run.runId,
      opts: { status: 'canceled' },
    });
    expect(run.abortController.signal.aborted).toBe(true);
    expect(run.workflowRunStatus).toBe('canceled');
    await expect(
      workflowsStore.loadWorkflowSnapshot({ workflowName: workflow.id, runId: run.runId }),
    ).resolves.toMatchObject({ status: 'pending' });
    expect(
      publish.mock.calls.some(([, event]) =>
        ['workflow.canceled', 'workflow.finished'].includes(String(event.data?.event?.type)),
      ),
    ).toBe(false);
  });

  it('preserves durable suspend and resume for an explicit run ID', async () => {
    const approval = createApprovalStep('approval');
    const workflow = createWorkflow({
      id: 'explicit-transient-durable-resume',
      inputSchema: ioSchema,
      outputSchema: ioSchema,
      steps: [approval],
      options: { executionMode: 'transient' },
    })
      .then(approval)
      .commit();
    const storage = new MockStore();
    new Mastra({ logger: false, storage, workflows: { [workflow.id]: workflow } });

    const run = await workflow.createRun({ runId: 'durable-resume-run-id' });
    await expect(run.start({ inputData: { value: 'resume' } })).resolves.toMatchObject({ status: 'suspended' });
    await expect(run.resume({ step: 'approval', resumeData: { approved: true } })).resolves.toMatchObject({
      status: 'success',
    });

    expect(run.transientExecution).toBe(false);
  });
});

function defineCustomIdGeneratorCollisionTest(
  engine: string,
  workflowFactory: (config: Record<string, unknown>) => ReturnType<typeof createWorkflow>,
) {
  describe(`${engine} createRun custom ID generator collision`, () => {
    it('retains the storage read and synchronizes status for a deterministic generated ID', async () => {
      const runId = 'deterministic-run-id';
      const workflow = workflowFactory({
        id: `${engine}-non-persisting-deterministic`,
        inputSchema: ioSchema,
        outputSchema: ioSchema,
        options: { shouldPersistSnapshot: () => false },
      })
        .then(
          createStep({
            id: 'passthrough',
            inputSchema: ioSchema,
            outputSchema: ioSchema,
            execute: async ({ inputData }) => inputData,
          }),
        )
        .commit();
      const storage = new MockStore();
      const workflowsStore = (await storage.getStore('workflows'))!;
      await workflowsStore.persistWorkflowSnapshot({
        workflowName: workflow.id,
        runId,
        snapshot: {
          runId,
          status: 'success',
          value: {},
          context: {},
          activePaths: [],
          activeStepsPath: {},
          suspendedPaths: {},
          resumeLabels: {},
          serializedStepGraph: [],
          waitingPaths: {},
          timestamp: Date.now(),
        } as WorkflowRunState,
      });
      new Mastra({
        idGenerator: () => runId,
        logger: false,
        storage,
        workflows: { [workflow.id]: workflow },
      });
      const read = vi.spyOn(workflowsStore, 'getWorkflowRunById');

      const run = await workflow.createRun();

      expect(read).toHaveBeenCalledWith({ runId, workflowName: workflow.id });
      expect(run.workflowRunStatus).toBe('success');
    });
  });
}

defineCustomIdGeneratorCollisionTest('default', createWorkflow);
defineCustomIdGeneratorCollisionTest('evented', createEventedWorkflow as never);
