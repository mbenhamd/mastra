import { EventEmitterPubSub } from '@mastra/core/events';
import { Mastra } from '@mastra/core/mastra';
import { getProcessorWorkflowPhases, setProcessorWorkflowPhases } from '@mastra/core/processors';
import { RequestContext } from '@mastra/core/request-context';
import { MockStore } from '@mastra/core/storage';
import { TRANSIENT_EXECUTION_SYMBOL } from '@mastra/core/workflows/_constants';
import { Inngest } from 'inngest';
import { describe, expect, it, vi } from 'vitest';
import { z } from 'zod/v4';

import { InngestExecutionEngine } from './execution-engine';
import { init } from './index';

function createTestWorkflow(storage = new MockStore()) {
  const inngest = new Inngest({ id: 'create-run-contract', isDev: true });
  const { createStep, createWorkflow } = init(inngest);
  const inputSchema = z.object({
    count: z.string().transform(Number),
    mode: z.enum(['safe', 'fast']).default('safe'),
  });
  const step = createStep({
    id: 'parse-input',
    inputSchema,
    outputSchema: z.object({ count: z.number() }),
    execute: async ({ inputData }) => ({ count: inputData.count }),
  });
  const workflow = createWorkflow({
    id: 'create-run-contract-workflow',
    inputSchema,
    outputSchema: z.object({ count: z.number() }),
    requestContextSchema: z.object({ tenantId: z.string().optional() }),
    steps: [step],
  })
    .then(step)
    .commit();
  const mastra = new Mastra({
    logger: false,
    storage,
    workflows: { workflow },
  });

  return { inngest, mastra, workflow };
}

describe('Inngest createRun contract', () => {
  it('preserves processor phase restrictions when cloning a workflow', () => {
    const inngest = new Inngest({ id: 'clone-processor-phase-contract', isDev: true });
    const { createStep, createWorkflow, cloneWorkflow } = init(inngest);
    const step = createStep({
      id: 'final-only-step',
      inputSchema: z.any(),
      outputSchema: z.any(),
      execute: async ({ inputData }) => inputData,
    });
    const workflow = setProcessorWorkflowPhases(
      createWorkflow({ id: 'final-only-workflow', inputSchema: z.any(), outputSchema: z.any() }).then(step).commit(),
      ['outputResult'],
    );

    const clone = cloneWorkflow(workflow, { id: 'final-only-workflow-clone' });

    expect(getProcessorWorkflowPhases(clone)).toEqual(['outputResult']);
  });

  it('validates raw input and serializes disableScorers through startAsync', async () => {
    const { inngest, mastra, workflow } = createTestWorkflow();
    const send = vi.spyOn(inngest, 'send').mockResolvedValue({ ids: ['event-1'] } as never);
    const run = await workflow.createRun({ disableScorers: true });

    await run.startAsync({ inputData: { count: '2' } });

    expect(run.disableScorers).toBe(true);
    const workflowsStore = await mastra.getStorage()!.getStore('workflows');
    await expect(
      workflowsStore!.loadWorkflowSnapshot({
        workflowName: workflow.id,
        runId: run.runId,
      }),
    ).resolves.toMatchObject({
      status: 'running',
      runOptions: {
        disableScorers: true,
      },
    });
    expect(send).toHaveBeenCalledTimes(1);
    expect(send.mock.calls[0]![0]).toMatchObject({
      name: 'workflow.create-run-contract-workflow',
      data: {
        inputData: { count: 2, mode: 'safe' },
        disableScorers: true,
      },
    });
  });

  it('validates raw input and serializes disableScorers through start', async () => {
    const { inngest, workflow } = createTestWorkflow();
    const send = vi.spyOn(inngest, 'send').mockResolvedValue({ ids: ['event-2'] } as never);
    const run = await workflow.createRun({ disableScorers: true });
    vi.spyOn(run, 'getRunOutput').mockResolvedValue({
      output: {
        result: {
          status: 'success',
          input: { count: 3, mode: 'safe' },
          steps: {},
          result: { count: 3 },
        },
      },
    } as never);

    const result = await run.start({ inputData: { count: '3' } });

    expect(result.status).toBe('success');
    expect(send.mock.calls[0]![0]).toMatchObject({
      data: {
        inputData: { count: 3, mode: 'safe' },
        disableScorers: true,
      },
    });
  });

  it('passes disableScorers into the engine and preserves its request context snapshot update', async () => {
    const { mastra, workflow } = createTestWorkflow();
    workflow.__setPubsubFactory(() => new EventEmitterPubSub());
    const workflowsStore = await mastra.getStorage()!.getStore('workflows');
    const execute = vi.spyOn(InngestExecutionEngine.prototype, 'execute').mockImplementation(async () => {
      const pendingSnapshot = await workflowsStore!.loadWorkflowSnapshot({
        workflowName: workflow.id,
        runId: 'remote-run',
      });
      await workflowsStore!.persistWorkflowSnapshot({
        workflowName: workflow.id,
        runId: 'remote-run',
        snapshot: {
          ...pendingSnapshot!,
          status: 'running',
          requestContext: { tenantId: 'tenant-remote' },
        },
      });
      return {
        status: 'success',
        input: { count: 4, mode: 'safe' },
        steps: {},
        result: { count: 4 },
        state: {},
      } as never;
    });
    const inngestFunction = workflow.getFunction() as unknown as {
      fn: (context: {
        event: { data: Record<string, unknown> };
        step: { run: <T>(id: string, operation: () => Promise<T>) => Promise<T> };
        attempt: number;
      }) => Promise<unknown>;
    };

    await inngestFunction.fn({
      event: {
        data: {
          inputData: { count: 4, mode: 'safe' },
          initialState: {},
          runId: 'remote-run',
          disableScorers: true,
          requestContext: { tenantId: 'tenant-remote' },
        },
      },
      step: {
        run: async (_id, operation) => operation(),
      },
      attempt: 0,
    });

    expect(execute).toHaveBeenCalledWith(expect.objectContaining({ disableScorers: true }));
    await expect(
      workflowsStore!.loadWorkflowSnapshot({
        workflowName: workflow.id,
        runId: 'remote-run',
      }),
    ).resolves.toMatchObject({
      status: 'success',
      runOptions: {
        disableScorers: true,
      },
      requestContext: { tenantId: 'tenant-remote' },
    });
  });

  it('preserves and validates the workflow requestContextSchema on Inngest runs', async () => {
    const { inngest, mastra, workflow } = createTestWorkflow();
    const send = vi.spyOn(inngest, 'send').mockResolvedValue({ ids: ['event-context'] } as never);
    const requestContext = new RequestContext<{ tenantId?: string }>();
    requestContext.set('tenantId', 'tenant-1');

    const run = await workflow.createRun();
    await run.startAsync({ inputData: { count: '5' }, requestContext });

    expect(send.mock.calls[0]![0]).toMatchObject({
      data: {
        requestContext: { tenantId: 'tenant-1' },
      },
    });

    const invalidRequestContext = new RequestContext<Record<string, unknown>>();
    invalidRequestContext.set('tenantId', 42);
    const invalidRun = await workflow.createRun();

    await expect(
      invalidRun.startAsync({
        inputData: { count: '6' },
        requestContext: invalidRequestContext as unknown as RequestContext<{ tenantId?: string }>,
      }),
    ).rejects.toThrow("Request context validation failed for workflow 'create-run-contract-workflow'");
    expect(send).toHaveBeenCalledTimes(1);
    const workflowsStore = await mastra.getStorage()!.getStore('workflows');
    await expect(
      workflowsStore!.loadWorkflowSnapshot({
        workflowName: workflow.id,
        runId: invalidRun.runId,
      }),
    ).resolves.toMatchObject({
      status: 'pending',
      context: {},
    });
  });

  it('restores the pending snapshot when start dispatch fails for either start entrypoint', async () => {
    const { inngest, mastra, workflow } = createTestWorkflow();
    const send = vi.spyOn(inngest, 'send');
    const workflowsStore = await mastra.getStorage()!.getStore('workflows');

    const asyncRun = await workflow.createRun({ disableScorers: true });
    send.mockRejectedValueOnce(new Error('startAsync dispatch failed'));
    await expect(asyncRun.startAsync({ inputData: { count: '7' } })).rejects.toThrow('startAsync dispatch failed');
    await expect(
      workflowsStore!.loadWorkflowSnapshot({
        workflowName: workflow.id,
        runId: asyncRun.runId,
      }),
    ).resolves.toMatchObject({
      status: 'pending',
      runOptions: {
        disableScorers: true,
      },
    });

    const awaitedRun = await workflow.createRun();
    send.mockRejectedValueOnce(new Error('start dispatch failed'));
    await expect(awaitedRun.start({ inputData: { count: '8' } })).rejects.toThrow('start dispatch failed');
    await expect(
      workflowsStore!.loadWorkflowSnapshot({
        workflowName: workflow.id,
        runId: awaitedRun.runId,
      }),
    ).resolves.toMatchObject({
      status: 'pending',
    });
  });

  it('restores the pending snapshot when Inngest accepts no event id', async () => {
    const { inngest, mastra, workflow } = createTestWorkflow();
    vi.spyOn(inngest, 'send').mockResolvedValue({ ids: [] } as never);
    const run = await workflow.createRun();

    await expect(run.startAsync({ inputData: { count: '9' } })).rejects.toThrow('Event ID is not set');

    const workflowsStore = await mastra.getStorage()!.getStore('workflows');
    await expect(
      workflowsStore!.loadWorkflowSnapshot({
        workflowName: workflow.id,
        runId: run.runId,
      }),
    ).resolves.toMatchObject({
      status: 'pending',
    });
  });

  it('rehydrates disableScorers from the durable snapshot after worker replacement', async () => {
    const storage = new MockStore();
    const firstWorker = createTestWorkflow(storage);
    vi.spyOn(firstWorker.inngest, 'send').mockResolvedValue({ ids: ['initial-event'] } as never);
    const firstRun = await firstWorker.workflow.createRun({
      runId: 'cold-resume-run',
      disableScorers: true,
    });
    await firstRun.startAsync({ inputData: { count: '10' } });

    const workflowsStore = await firstWorker.mastra.getStorage()!.getStore('workflows');
    const runningSnapshot = await workflowsStore!.loadWorkflowSnapshot({
      workflowName: firstWorker.workflow.id,
      runId: firstRun.runId,
    });
    expect(runningSnapshot).toMatchObject({
      status: 'running',
      runOptions: {
        disableScorers: true,
      },
    });
    await workflowsStore!.persistWorkflowSnapshot({
      workflowName: firstWorker.workflow.id,
      runId: firstRun.runId,
      snapshot: {
        ...runningSnapshot!,
        status: 'suspended',
        context: {
          input: {
            count: 10,
            mode: 'safe',
          },
        },
        suspendedPaths: {
          'parse-input': [0],
        },
      },
    });

    const replacementWorker = createTestWorkflow(storage);
    const replacementSend = vi
      .spyOn(replacementWorker.inngest, 'send')
      .mockResolvedValue({ ids: ['resume-event'] } as never);
    const replacementRun = await replacementWorker.workflow.createRun({
      runId: firstRun.runId,
    });

    expect(replacementRun.disableScorers).toBe(true);
    await replacementRun.resumeAsync({
      step: 'parse-input',
      resumeData: {
        count: 11,
      },
    });

    expect(replacementSend).toHaveBeenCalledWith(
      expect.objectContaining({
        data: expect.objectContaining({
          runId: firstRun.runId,
          disableScorers: true,
        }),
      }),
    );
  });

  it('rehydrates resourceId for a cold resume after worker replacement', async () => {
    const storage = new MockStore();
    const firstWorker = createTestWorkflow(storage);
    const firstRun = await firstWorker.workflow.createRun({
      runId: 'cold-resource-resume-run',
      resourceId: 'resource-1',
    });
    const workflowsStore = await firstWorker.mastra.getStorage()!.getStore('workflows');
    const pendingSnapshot = await workflowsStore!.loadWorkflowSnapshot({
      workflowName: firstWorker.workflow.id,
      runId: firstRun.runId,
    });
    await workflowsStore!.persistWorkflowSnapshot({
      workflowName: firstWorker.workflow.id,
      runId: firstRun.runId,
      resourceId: 'resource-1',
      snapshot: {
        ...pendingSnapshot!,
        status: 'suspended',
        context: { input: { count: 10, mode: 'safe' } },
        suspendedPaths: { 'parse-input': [0] },
      },
    });

    const replacementWorker = createTestWorkflow(storage);
    const replacementSend = vi
      .spyOn(replacementWorker.inngest, 'send')
      .mockResolvedValue({ ids: ['resume-event'] } as never);
    const getWorkflowRunById = vi
      .spyOn(workflowsStore!, 'getWorkflowRunById')
      .mockRejectedValue(new Error('workflow row lookup is unavailable'));
    const replacementRun = await replacementWorker.workflow.createRun({ runId: firstRun.runId });

    await replacementRun.resumeAsync({
      step: 'parse-input',
      resumeData: { count: 11 },
    });

    expect(replacementSend).toHaveBeenCalledWith(
      expect.objectContaining({
        data: expect.objectContaining({
          runId: firstRun.runId,
          resourceId: 'resource-1',
        }),
      }),
    );
    expect(getWorkflowRunById).not.toHaveBeenCalled();
    await expect(
      workflowsStore!.loadWorkflowSnapshot({
        workflowName: firstWorker.workflow.id,
        runId: firstRun.runId,
      }),
    ).resolves.toMatchObject({ resourceId: 'resource-1' });
  });

  it('does not require a workflow-row lookup when no durable snapshot exists', async () => {
    const { mastra, workflow } = createTestWorkflow();
    const workflowsStore = await mastra.getStorage()!.getStore('workflows');
    const getWorkflowRunById = vi
      .spyOn(workflowsStore!, 'getWorkflowRunById')
      .mockRejectedValue(new Error('workflow table is not initialized'));

    const run = await workflow.createRun({ runId: 'new-run-without-snapshot' });

    expect(run.runId).toBe('new-run-without-snapshot');
    expect(getWorkflowRunById).not.toHaveBeenCalled();
    await expect(
      workflowsStore!.loadWorkflowSnapshot({
        workflowName: workflow.id,
        runId: run.runId,
      }),
    ).resolves.toMatchObject({ status: 'pending' });
  });

  it('preserves a persisted explicit false over a later truthy reattachment option', async () => {
    const storage = new MockStore();
    const firstWorker = createTestWorkflow(storage);
    const firstRun = await firstWorker.workflow.createRun({
      runId: 'cold-false-precedence-run',
      disableScorers: false,
    });

    const replacementWorker = createTestWorkflow(storage);
    const replacementRun = await replacementWorker.workflow.createRun({
      runId: firstRun.runId,
      disableScorers: true,
    });

    expect(replacementRun.disableScorers).toBe(false);
    const workflowsStore = await replacementWorker.mastra.getStorage()!.getStore('workflows');
    await expect(
      workflowsStore!.loadWorkflowSnapshot({
        workflowName: replacementWorker.workflow.id,
        runId: firstRun.runId,
      }),
    ).resolves.toMatchObject({
      status: 'pending',
      runOptions: {
        disableScorers: false,
      },
    });
  });

  it('rehydrates disableScorers for cold time travel after worker replacement', async () => {
    const storage = new MockStore();
    const firstWorker = createTestWorkflow(storage);
    const firstRun = await firstWorker.workflow.createRun({
      runId: 'cold-time-travel-run',
      disableScorers: true,
    });
    const workflowsStore = await firstWorker.mastra.getStorage()!.getStore('workflows');
    const pendingSnapshot = await workflowsStore!.loadWorkflowSnapshot({
      workflowName: firstWorker.workflow.id,
      runId: firstRun.runId,
    });
    await workflowsStore!.persistWorkflowSnapshot({
      workflowName: firstWorker.workflow.id,
      runId: firstRun.runId,
      snapshot: {
        ...pendingSnapshot!,
        status: 'success',
        context: {
          input: {
            count: 12,
            mode: 'safe',
          },
        },
      },
    });

    const replacementWorker = createTestWorkflow(storage);
    const replacementRun = await replacementWorker.workflow.createRun({ runId: firstRun.runId });
    const replacementSend = vi
      .spyOn(replacementWorker.inngest, 'send')
      .mockResolvedValue({ ids: ['time-travel-event'] } as never);
    vi.spyOn(replacementRun, 'getRunOutput').mockResolvedValue({
      output: {
        result: {
          status: 'success',
          input: { count: 13, mode: 'safe' },
          steps: {},
          result: { count: 13 },
        },
      },
    } as never);

    await replacementRun.timeTravel({
      step: 'parse-input',
      inputData: { count: '13' },
    });

    expect(replacementRun.disableScorers).toBe(true);
    expect(replacementSend).toHaveBeenCalledWith(
      expect.objectContaining({
        data: expect.objectContaining({
          runId: firstRun.runId,
          disableScorers: true,
        }),
      }),
    );
    await expect(
      workflowsStore!.loadWorkflowSnapshot({
        workflowName: firstWorker.workflow.id,
        runId: firstRun.runId,
      }),
    ).resolves.toMatchObject({
      status: 'running',
      runOptions: {
        disableScorers: true,
      },
    });
  });

  it.each([
    { dispatchOutcome: 'send rejection', eventIds: undefined },
    { dispatchOutcome: 'missing event id', eventIds: [] },
  ])('restores the prior snapshot when time-travel dispatch ends in $dispatchOutcome', async ({ eventIds }) => {
    const { inngest, mastra, workflow } = createTestWorkflow();
    const run = await workflow.createRun({ disableScorers: true });
    const workflowsStore = await mastra.getStorage()!.getStore('workflows');
    await workflowsStore!.persistWorkflowSnapshot({
      workflowName: workflow.id,
      runId: run.runId,
      snapshot: {
        ...(await workflowsStore!.loadWorkflowSnapshot({
          workflowName: workflow.id,
          runId: run.runId,
        }))!,
        status: 'success',
        context: {
          input: {
            count: 14,
            mode: 'safe',
          },
        },
      },
    });
    const send = vi.spyOn(inngest, 'send');
    const expectedError = eventIds ? 'Event ID is not set' : 'time-travel dispatch failed';
    if (eventIds) {
      send.mockResolvedValue({ ids: eventIds } as never);
    } else {
      send.mockRejectedValue(new Error(expectedError));
    }

    await expect(
      run.timeTravel({
        step: 'parse-input',
        inputData: { count: '15' },
      }),
    ).rejects.toThrow(expectedError);

    await expect(
      workflowsStore!.loadWorkflowSnapshot({
        workflowName: workflow.id,
        runId: run.runId,
      }),
    ).resolves.toMatchObject({
      status: 'success',
      context: {
        input: {
          count: 14,
          mode: 'safe',
        },
      },
      runOptions: {
        disableScorers: true,
      },
    });
  });

  it('keeps the ordinary createRun path unchanged when no run options are provided', async () => {
    const { inngest, workflow } = createTestWorkflow();
    const send = vi.spyOn(inngest, 'send').mockResolvedValue({ ids: ['event-3'] } as never);
    const run = await workflow.createRun();

    await run.startAsync({ inputData: { count: '5', mode: 'fast' } });

    expect(run.disableScorers).toBeUndefined();
    expect(send.mock.calls[0]![0]).toMatchObject({
      data: {
        inputData: { count: 5, mode: 'fast' },
        disableScorers: undefined,
      },
    });
  });

  it('rejects per-run PubSub configuration instead of silently dropping it', async () => {
    const { workflow } = createTestWorkflow();

    await expect(workflow.createRun({ pubsub: new EventEmitterPubSub() })).rejects.toThrow(
      'Inngest createRun({ pubsub }) is unsupported because remote function replicas cannot reconstruct a per-run PubSub object',
    );
  });

  it('rejects transient execution inherited from a parent workflow', async () => {
    const { workflow } = createTestWorkflow();

    await expect(workflow.createRun({ [TRANSIENT_EXECUTION_SYMBOL]: true })).rejects.toThrow(
      'Inngest workflows cannot run inside transient workflows',
    );
  });

  it('rejects the inherited Core schedule option instead of silently dropping it', () => {
    const inngest = new Inngest({ id: 'unsupported-core-schedule' });
    const { createWorkflow } = init(inngest);

    expect(() =>
      createWorkflow({
        id: 'unsupported-core-schedule',
        inputSchema: z.object({ value: z.string() }),
        outputSchema: z.object({ value: z.string() }),
        schedule: { cron: '0 9 * * *' },
      }),
    ).toThrow(
      'Inngest workflows do not support the Core schedule option; use cron, inputData, and initialState instead',
    );
  });
});
