import { EventEmitterPubSub } from '@mastra/core/events';
import { Mastra } from '@mastra/core/mastra';
import { MockStore, WORKFLOW_RESUME_RESULT_RECEIPT_MAX_BYTES } from '@mastra/core/storage';
import { Inngest } from 'inngest';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { z } from 'zod';
import { InngestExecutionEngine } from './execution-engine';
import { __setInngestRealtimeSubscribeForTests } from './run';
import type { InngestRun } from './run';
import { init } from './index';

const realtime = vi.hoisted(() => ({
  callback: undefined as undefined | ((message: any) => Promise<void>),
}));

/**
 * Focused unit tests for InngestRun.resumeAsync().
 *
 * These tests do NOT require a live Inngest dev server. They mock `inngest.send()`
 * and assert the core invariant from issue #17156: `resumeAsync()` dispatches the
 * resume event and returns immediately with `{ runId }`, WITHOUT polling via
 * `getRunOutput()`.
 */
describe('InngestRun.resumeAsync()', () => {
  let inngest: Inngest;
  let sendMock: ReturnType<typeof vi.fn>;

  function buildWorkflow() {
    const { createWorkflow, createStep } = init(inngest);

    const step1 = createStep({
      id: 'step1',
      inputSchema: z.object({ value: z.string() }),
      resumeSchema: z.object({ resumed: z.string() }),
      suspendSchema: z.object({}),
      outputSchema: z.object({ result: z.string() }),
      execute: async ({ inputData, resumeData, suspend }) => {
        if (!resumeData) {
          return suspend({});
        }
        return { result: `${inputData.value}:${resumeData.resumed}` };
      },
    });

    const workflow = createWorkflow({
      id: 'resume-async-wf',
      inputSchema: z.object({ value: z.string() }),
      outputSchema: z.object({ result: z.string() }),
      steps: [step1],
      options: { shouldPersistSnapshot: ({ workflowStatus }) => workflowStatus === 'suspended' },
    });
    workflow.then(step1).commit();

    return { workflow, step1 };
  }

  async function createSuspendedRun() {
    const { workflow, step1 } = buildWorkflow();

    const mastra = new Mastra({
      storage: new MockStore(),

      workflows: { 'resume-async-wf': workflow as any },
    });
    workflow.__setPubsubFactory(() => new EventEmitterPubSub());

    const run = (await workflow.createRun()) as unknown as InngestRun;

    // Seed a suspended snapshot directly so we don't need to run the workflow.
    const storage = mastra.getStorage()!;
    const workflowsStore = await storage.getStore('workflows');
    await workflowsStore!.persistWorkflowSnapshot({
      workflowName: 'resume-async-wf',
      runId: run.runId,
      resourceId: 'resource-from-snapshot',
      snapshot: {
        runId: run.runId,
        resourceId: 'resource-from-snapshot',
        serializedStepGraph: run.serializedStepGraph,
        status: 'suspended',
        value: {},

        context: { input: { value: 'hello' } } as any,
        activePaths: [],
        suspendedPaths: { step1: [0] },
        activeStepsPath: {},
        resumeLabels: {},
        waitingPaths: {},
        timestamp: Date.now(),
        executionGeneration: 'resume-async-execution',
        lifecycleResumeAttempt: 0,
        lifecycleStepStates: {},
      },
    });

    return { run, step1, mastra, workflow, workflowsStore: workflowsStore! };
  }

  async function executeDispatchedResume(workflow: ReturnType<typeof buildWorkflow>['workflow']) {
    vi.spyOn(InngestExecutionEngine.prototype, 'execute').mockResolvedValue({
      status: 'success',
      input: { value: 'hello' },
      steps: {},
      result: { result: 'hello:world' },
      state: {},
    } as never);
    const inngestFunction = workflow.getFunction() as unknown as {
      fn: (context: {
        event: { data: Record<string, unknown> };
        step: { run: <T>(id: string, operation: () => Promise<T>) => Promise<T> };
        attempt: number;
      }) => Promise<unknown>;
    };

    return inngestFunction.fn({
      event: sendMock.mock.calls.at(-1)![0],
      step: {
        run: async (_id, operation) => operation(),
      },
      attempt: 0,
    });
  }

  beforeEach(() => {
    vi.restoreAllMocks();
    sendMock = vi.fn().mockResolvedValue({ ids: ['evt_123'] });
    inngest = new Inngest({ id: 'mastra-test', baseUrl: 'http://localhost:9999' });
    // Replace the real transport with our mock.

    (inngest as any).send = sendMock;
    realtime.callback = undefined;
    __setInngestRealtimeSubscribeForTests(((_options: unknown, callback: (message: any) => Promise<void>) => {
      realtime.callback = callback;
      return Promise.resolve({ cancel: vi.fn().mockResolvedValue(undefined) });
    }) as never);
  });

  async function finalizeDispatchedResume(run: InngestRun, workflowsStore: any) {
    const event = sendMock.mock.calls.at(-1)![0];
    const current = await workflowsStore.loadWorkflowSnapshot({
      workflowName: 'resume-async-wf',
      runId: run.runId,
    });
    const result = {
      status: 'success' as const,
      input: { value: 'hello' },
      steps: {},
      state: { finished: true },
      result: { result: 'hello:world' },
    };
    const finalization = await workflowsStore.finalizeWorkflowResume({
      workflowName: 'resume-async-wf',
      runId: run.runId,
      resourceId: current!.resourceId,
      resumeOperationHash: event.data.resumeOperationHash,
      executionGeneration: current!.executionGeneration!,
      lifecycleResumeAttempt: current!.lifecycleResumeAttempt!,
      lifecycleStepStates: current!.lifecycleStepStates!,
      shouldPersistSnapshot: false,
      receiptKey: event.data.receiptKey,
      snapshot: { ...current!, status: 'success', value: result.state, context: {}, result: result.result },
      result,
    });
    expect(finalization.status).toBe('finalized');
    return { event, result };
  }

  async function finalizeDispatchedResumeAsTerminal(run: InngestRun, workflowsStore: any) {
    const event = sendMock.mock.calls.at(-1)![0];
    const current = await workflowsStore.loadWorkflowSnapshot({
      workflowName: 'resume-async-wf',
      runId: run.runId,
    });
    const result = {
      status: 'success' as const,
      input: { value: 'hello' },
      steps: {},
      state: { finished: true },
      result: { result: 'hello:world' },
    };
    const finalization = await workflowsStore.finalizeWorkflowResume({
      workflowName: 'resume-async-wf',
      runId: run.runId,
      resourceId: current!.resourceId,
      resumeOperationHash: event.data.resumeOperationHash,
      executionGeneration: current!.executionGeneration!,
      lifecycleResumeAttempt: current!.lifecycleResumeAttempt!,
      lifecycleStepStates: current!.lifecycleStepStates!,
      shouldPersistSnapshot: true,
      receiptKey: event.data.receiptKey,
      snapshot: { ...current!, status: 'success', value: result.state, context: {}, result: result.result },
      result,
    });
    expect(finalization.status).toBe('finalized');
    return { event, result };
  }

  it('returns { runId } immediately and does NOT poll getRunOutput', async () => {
    const { run } = await createSuspendedRun();

    const getRunOutputSpy = vi.spyOn(run, 'getRunOutput');

    const result = await run.resumeAsync({ step: 'step1', resumeData: { resumed: 'world' } });

    expect(result).toEqual({ runId: run.runId });
    expect(getRunOutputSpy).not.toHaveBeenCalled();
  });

  it('fails closed before dispatch when the storage adapter lacks atomic resume support', async () => {
    const { run, workflowsStore } = await createSuspendedRun();
    vi.spyOn(workflowsStore, 'getWorkflowResumeCapabilities').mockReturnValue({});

    await expect(run.resumeAsync({ step: 'step1', resumeData: { resumed: 'world' } })).rejects.toThrow(
      'does not support atomic resume admission',
    );
    expect(sendMock).not.toHaveBeenCalled();
    await expect(
      workflowsStore.loadWorkflowSnapshot({ workflowName: 'resume-async-wf', runId: run.runId }),
    ).resolves.toMatchObject({ status: 'suspended', lifecycleResumeAttempt: 0 });
  });

  it('fails closed before dispatch when atomic resume lacks fenced step updates', async () => {
    const { run, workflowsStore } = await createSuspendedRun();
    vi.spyOn(workflowsStore, 'getWorkflowResumeCapabilities').mockReturnValue({ atomicResumeVersion: 1 });

    await expect(run.resumeAsync({ step: 'step1', resumeData: { resumed: 'world' } })).rejects.toThrow(
      'fenced step updates',
    );
    expect(sendMock).not.toHaveBeenCalled();
    await expect(
      workflowsStore.loadWorkflowSnapshot({ workflowName: 'resume-async-wf', runId: run.runId }),
    ).resolves.toMatchObject({ status: 'suspended', lifecycleResumeAttempt: 0 });
  });

  it('dispatches the resume event with the correct payload', async () => {
    const { run } = await createSuspendedRun();

    await run.resumeAsync({ step: 'step1', resumeData: { resumed: 'world' } });

    expect(sendMock).toHaveBeenCalledTimes(1);
    const sentEvent = sendMock.mock.calls[0][0];
    expect(sentEvent.name).toBe('workflow.resume-async-wf');
    expect(sentEvent.data.runId).toBe(run.runId);
    expect(sentEvent.data.resume.steps).toEqual(['step1']);
    expect(sentEvent.data.resume.resumePayload).toEqual({ resumed: 'world' });
    expect(sentEvent.data.executionGeneration).toBe('resume-async-execution');
    expect(sentEvent.data.lifecycleResumeAttempt).toBe(1);
    expect(sentEvent.data.lifecycleStepStates).toEqual({});
    expect(sentEvent.data.resourceId).toBe('resource-from-snapshot');
    expect(sentEvent.data.resumeOperationHash).toMatch(/^sha256:[a-f0-9]{64}$/);
    expect(sentEvent.data.receiptKey).toBe(sentEvent.id);
    expect(sentEvent.data).not.toHaveProperty('initialState');
    expect(sentEvent.data).not.toHaveProperty('stepResults');
    expect(sentEvent.data.resume).not.toHaveProperty('stepResults');
    expect(sentEvent.data).not.toHaveProperty('snapshotBeforeResume');
  });

  it('keeps the event small while the storage checkpoint round-trips large state', async () => {
    const { run, workflowsStore } = await createSuspendedRun();
    const snapshot = await workflowsStore.loadWorkflowSnapshot({
      workflowName: 'resume-async-wf',
      runId: run.runId,
    });
    await workflowsStore.persistWorkflowSnapshot({
      workflowName: 'resume-async-wf',
      runId: run.runId,
      resourceId: snapshot!.resourceId,
      snapshot: {
        ...snapshot!,
        value: { largeState: 's'.repeat(300_000) },
        context: {
          ...snapshot!.context,
          largeStep: { status: 'success', output: 'c'.repeat(300_000) } as never,
        },
      },
    });

    await run.resumeAsync({ step: 'step1', resumeData: { resumed: 'world' } });

    const sentEvent = sendMock.mock.calls[0]![0];
    expect(Buffer.byteLength(JSON.stringify(sentEvent), 'utf8')).toBeLessThan(10_000);
    expect(JSON.stringify(sentEvent)).not.toContain('largeState');
    const admitted = await workflowsStore.loadWorkflowSnapshot({
      workflowName: 'resume-async-wf',
      runId: run.runId,
    });
    const roundTripped = JSON.parse(JSON.stringify(admitted));
    expect(roundTripped.resumeCheckpoint.snapshot.value.largeState).toHaveLength(300_000);
    expect(roundTripped.resumeCheckpoint.snapshot.context.largeStep.output).toHaveLength(300_000);
  });

  it('updates the snapshot to running before sending the event', async () => {
    const { run, workflowsStore } = await createSuspendedRun();

    let snapshotAtSendTime: Awaited<ReturnType<typeof workflowsStore.loadWorkflowSnapshot>>;
    sendMock.mockImplementation(async () => {
      snapshotAtSendTime = await workflowsStore.loadWorkflowSnapshot({
        workflowName: 'resume-async-wf',
        runId: run.runId,
      });
      return { ids: ['evt_123'] };
    });

    await run.resumeAsync({ step: 'step1', resumeData: { resumed: 'world' } });

    expect(snapshotAtSendTime?.status).toBe('running');
    expect(snapshotAtSendTime?.executionGeneration).toBe('resume-async-execution');
    expect(snapshotAtSendTime?.lifecycleResumeAttempt).toBe(1);
  });

  it('restores the last suspended snapshot remotely for an awaited resume when the terminal snapshot is rejected', async () => {
    const { run, workflow, workflowsStore } = await createSuspendedRun();
    vi.spyOn(run, 'getRunOutput').mockImplementation(async () => {
      await executeDispatchedResume(workflow);
      return {
        output: {
          result: {
            status: 'success',
            input: { value: 'hello' },
            steps: {},
            result: { result: 'hello:world' },
          },
        },
      } as never;
    });

    await expect(run.resume({ step: 'step1', resumeData: { resumed: 'world' } })).resolves.toMatchObject({
      status: 'success',
    });

    await expect(
      workflowsStore.loadWorkflowSnapshot({
        workflowName: 'resume-async-wf',
        runId: run.runId,
      }),
    ).resolves.toMatchObject({
      status: 'suspended',
      suspendedPaths: { step1: [0] },
      executionGeneration: 'resume-async-execution',
      lifecycleResumeAttempt: 1,
    });
  });

  it('restores the last suspended snapshot remotely after resumeAsync returns', async () => {
    const { run, workflow, workflowsStore } = await createSuspendedRun();

    await expect(run.resumeAsync({ step: 'step1', resumeData: { resumed: 'world' } })).resolves.toEqual({
      runId: run.runId,
    });
    await executeDispatchedResume(workflow);

    await expect(
      workflowsStore.loadWorkflowSnapshot({
        workflowName: 'resume-async-wf',
        runId: run.runId,
      }),
    ).resolves.toMatchObject({
      status: 'suspended',
      suspendedPaths: { step1: [0] },
      executionGeneration: 'resume-async-execution',
      lifecycleResumeAttempt: 1,
    });
  });

  it('returns the exact terminal receipt for an identical resume operation without redispatching', async () => {
    const { run, workflowsStore } = await createSuspendedRun();
    const params = { step: 'step1', resumeData: { resumed: 'world' } } as const;
    await run.resumeAsync(params);
    await finalizeDispatchedResumeAsTerminal(run, workflowsStore as never);

    await expect(run.resumeAsync(params)).resolves.toEqual({ runId: run.runId });
    expect(sendMock).toHaveBeenCalledTimes(1);
  });

  it('admits a new attempt when the previous identical resume operation suspends again', async () => {
    const { run, workflowsStore } = await createSuspendedRun();
    const params = { step: 'step1', resumeData: { resumed: 'world' } } as const;

    await run.resumeAsync(params);
    const firstEvent = sendMock.mock.calls[0]![0];
    await finalizeDispatchedResume(run, workflowsStore as never);

    await expect(run.resumeAsync(params)).resolves.toEqual({ runId: run.runId });
    expect(sendMock).toHaveBeenCalledTimes(2);
    const secondEvent = sendMock.mock.calls[1]![0];
    expect(secondEvent.data.resumeOperationHash).toBe(firstEvent.data.resumeOperationHash);
    expect(secondEvent.data.lifecycleResumeAttempt).toBe(2);
    expect(secondEvent.data.receiptKey).not.toBe(firstEvent.data.receiptKey);
    await expect(
      workflowsStore.loadWorkflowSnapshot({ workflowName: 'resume-async-wf', runId: run.runId }),
    ).resolves.toMatchObject({
      status: 'running',
      lifecycleResumeAttempt: 2,
      resumeCheckpoint: {
        resumeOperationHash: firstEvent.data.resumeOperationHash,
        lifecycleResumeAttempt: 2,
      },
    });
  });

  it('rejects a conflicting resume payload instead of returning another operation terminal receipt', async () => {
    const { run, workflowsStore } = await createSuspendedRun();
    await run.resumeAsync({ step: 'step1', resumeData: { resumed: 'world' } });
    await finalizeDispatchedResumeAsTerminal(run, workflowsStore as never);

    await expect(run.resumeAsync({ step: 'step1', resumeData: { resumed: 'different' } })).rejects.toThrow(
      'terminal result belongs to a different resume operation',
    );
    expect(sendMock).toHaveBeenCalledTimes(1);
  });

  it('waits for the exact durable receipt instead of returning the restored suspended snapshot', async () => {
    const { run, workflowsStore } = await createSuspendedRun();
    await run.resumeAsync({ step: 'step1', resumeData: { resumed: 'world' } });
    const event = sendMock.mock.calls[0]![0];
    const outputPromise = run.getRunOutput('evt_123', 2_000, {
      receiptKey: event.data.receiptKey,
      resumeOperationHash: event.data.resumeOperationHash,
      executionGeneration: event.data.executionGeneration,
      lifecycleResumeAttempt: event.data.lifecycleResumeAttempt,
    } as never);

    const { result } = await finalizeDispatchedResume(run, workflowsStore as never);
    await expect(outputPromise).resolves.toEqual({ output: { result } });
    await expect(
      workflowsStore.loadWorkflowSnapshot({ workflowName: 'resume-async-wf', runId: run.runId }),
    ).resolves.toMatchObject({
      status: 'suspended',
      lifecycleResumeAttempt: 1,
      resumeResultReceipt: { receiptKey: event.data.receiptKey },
    });
  });

  it('ignores minimal and stale finish events until the exact durable resume receipt wins', async () => {
    const { run, workflowsStore } = await createSuspendedRun();
    await run.resumeAsync({ step: 'step1', resumeData: { resumed: 'world' } });
    const event = sendMock.mock.calls.at(-1)![0];
    const outputPromise = run.getRunOutput('evt_123', 2_000, {
      receiptKey: event.data.receiptKey,
      resumeOperationHash: event.data.resumeOperationHash,
      executionGeneration: event.data.executionGeneration,
      lifecycleResumeAttempt: event.data.lifecycleResumeAttempt,
    } as never);
    let settled = false;
    void outputPromise.finally(() => {
      settled = true;
    });
    await vi.waitFor(() => expect(realtime.callback).toBeTypeOf('function'));
    await realtime.callback!({
      data: { type: 'workflow-finish', payload: { status: 'success', result: { forged: 'minimal' } } },
    });
    await Promise.resolve();
    expect(settled).toBe(false);

    await realtime.callback!({
      data: {
        type: 'workflow-finish',
        payload: {
          receiptKey: 'stale-receipt',
          resumeOperationHash: event.data.resumeOperationHash,
          executionGeneration: event.data.executionGeneration,
          lifecycleResumeAttempt: event.data.lifecycleResumeAttempt,
          workflowResult: { status: 'success', steps: {}, result: { forged: 'stale' } },
        },
      },
    });
    await Promise.resolve();
    expect(settled).toBe(false);

    const { result } = await finalizeDispatchedResume(run, workflowsStore as never);
    await realtime.callback!({
      data: {
        type: 'workflow-finish',
        payload: {
          receiptKey: event.data.receiptKey,
          resumeOperationHash: event.data.resumeOperationHash,
          executionGeneration: event.data.executionGeneration,
          lifecycleResumeAttempt: event.data.lifecycleResumeAttempt,
          workflowResult: { ...result, result: { forged: 'ignored' } },
        },
      },
    });

    await expect(outputPromise).resolves.toEqual({ output: { result } });
  });

  it('resolves an oversized resume result as an explicit bounded failure instead of timing out', async () => {
    const { run, workflowsStore } = await createSuspendedRun();
    await run.resumeAsync({ step: 'step1', resumeData: { resumed: 'world' } });
    const event = sendMock.mock.calls[0]![0];
    const current = await workflowsStore.loadWorkflowSnapshot({
      workflowName: 'resume-async-wf',
      runId: run.runId,
    });
    const outputPromise = run.getRunOutput('evt_123', 2_000, {
      receiptKey: event.data.receiptKey,
      resumeOperationHash: event.data.resumeOperationHash,
      executionGeneration: event.data.executionGeneration,
      lifecycleResumeAttempt: event.data.lifecycleResumeAttempt,
    } as never);
    await workflowsStore.finalizeWorkflowResume({
      workflowName: 'resume-async-wf',
      runId: run.runId,
      resourceId: current!.resourceId,
      resumeOperationHash: event.data.resumeOperationHash,
      executionGeneration: current!.executionGeneration!,
      lifecycleResumeAttempt: current!.lifecycleResumeAttempt!,
      lifecycleStepStates: current!.lifecycleStepStates!,
      shouldPersistSnapshot: false,
      receiptKey: event.data.receiptKey,
      snapshot: { ...current!, status: 'success', result: { tooLarge: true } },
      result: {
        status: 'success',
        steps: {},
        result: 'x'.repeat(WORKFLOW_RESUME_RESULT_RECEIPT_MAX_BYTES),
      },
    });

    await expect(outputPromise).resolves.toMatchObject({
      output: {
        result: {
          status: 'failed',
          error: { name: 'WorkflowResumeResultTooLargeError' },
        },
      },
    });
  });

  it('retains admission after a lost send acknowledgement and retries the same deterministic event', async () => {
    const { run, workflowsStore } = await createSuspendedRun();

    sendMock.mockRejectedValueOnce(new Error('inngest send failed')).mockResolvedValueOnce({ ids: ['evt_retry'] });

    await expect(run.resumeAsync({ step: 'step1', resumeData: { resumed: 'world' } })).rejects.toThrow(
      'inngest send failed',
    );

    const admitted = await workflowsStore.loadWorkflowSnapshot({
      workflowName: 'resume-async-wf',
      runId: run.runId,
    });
    expect(admitted).toMatchObject({
      status: 'running',
      executionGeneration: 'resume-async-execution',
      lifecycleResumeAttempt: 1,
      resumeCheckpoint: { runId: run.runId },
    });

    await expect(run.resumeAsync({ step: 'step1', resumeData: { resumed: 'world' } })).resolves.toEqual({
      runId: run.runId,
    });
    expect(sendMock).toHaveBeenCalledTimes(2);
    expect(sendMock.mock.calls[1]![0].id).toBe(sendMock.mock.calls[0]![0].id);
  });

  it('retains the admitted checkpoint when the send response has no event id', async () => {
    const { run, workflowsStore } = await createSuspendedRun();
    sendMock.mockResolvedValueOnce({ ids: [] });

    await expect(run.resumeAsync({ step: 'step1', resumeData: { resumed: 'world' } })).rejects.toThrow(
      'Event ID is not set',
    );

    const snapshot = await workflowsStore.loadWorkflowSnapshot({
      workflowName: 'resume-async-wf',
      runId: run.runId,
    });
    expect(snapshot?.status).toBe('running');
    expect(snapshot?.executionGeneration).toBe('resume-async-execution');
    expect(snapshot?.lifecycleResumeAttempt).toBe(1);
    expect(snapshot?.resumeCheckpoint).toMatchObject({ runId: run.runId });
  });

  it('does not let an already-admitted caller roll back the winner when its resend fails', async () => {
    const { run: firstRun, workflow, workflowsStore } = await createSuspendedRun();
    const secondRun = (await workflow.createRun({ runId: firstRun.runId })) as unknown as InngestRun;
    sendMock.mockResolvedValueOnce({ ids: ['evt_winner'] }).mockRejectedValueOnce(new Error('duplicate resend failed'));

    const [winner, duplicate] = await Promise.allSettled([
      firstRun.resumeAsync({ step: 'step1', resumeData: { resumed: 'world' } }),
      secondRun.resumeAsync({ step: 'step1', resumeData: { resumed: 'world' } }),
    ]);

    expect(winner.status).toBe('fulfilled');
    expect(duplicate.status).toBe('rejected');
    await expect(
      workflowsStore.loadWorkflowSnapshot({ workflowName: 'resume-async-wf', runId: firstRun.runId }),
    ).resolves.toMatchObject({
      status: 'running',
      lifecycleResumeAttempt: 1,
      resumeCheckpoint: { resumeOperationHash: expect.stringMatching(/^sha256:/) },
    });
  });

  it('rejects a different concurrent resume operation before dispatch', async () => {
    const { run: firstRun, workflow, workflowsStore } = await createSuspendedRun();
    const secondRun = (await workflow.createRun({ runId: firstRun.runId })) as unknown as InngestRun;

    const [winner, conflict] = await Promise.allSettled([
      firstRun.resumeAsync({ step: 'step1', resumeData: { resumed: 'world' } }),
      secondRun.resumeAsync({ step: 'step1', resumeData: { resumed: 'different' } }),
    ]);

    expect(winner.status).toBe('fulfilled');
    expect(conflict.status).toBe('rejected');
    if (conflict.status === 'rejected') {
      expect(String(conflict.reason)).toContain('operation_conflict');
    }
    expect(sendMock).toHaveBeenCalledTimes(1);
    await expect(
      workflowsStore.loadWorkflowSnapshot({ workflowName: 'resume-async-wf', runId: firstRun.runId }),
    ).resolves.toMatchObject({ status: 'running', lifecycleResumeAttempt: 1 });
  });
});
