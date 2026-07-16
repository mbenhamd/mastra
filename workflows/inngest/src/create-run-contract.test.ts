import { EventEmitterPubSub } from '@mastra/core/events';
import { Mastra } from '@mastra/core/mastra';
import { RequestContext } from '@mastra/core/request-context';
import { MockStore } from '@mastra/core/storage';
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

  it('passes disableScorers from the remote event into the execution engine', async () => {
    const { mastra, workflow } = createTestWorkflow();
    workflow.__setPubsubFactory(() => new EventEmitterPubSub());
    const execute = vi.spyOn(InngestExecutionEngine.prototype, 'execute').mockResolvedValue({
      status: 'success',
      input: { count: 4, mode: 'safe' },
      steps: {},
      result: { count: 4 },
      state: {},
    } as never);
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
        },
      },
      step: {
        run: async (_id, operation) => operation(),
      },
      attempt: 0,
    });

    expect(execute).toHaveBeenCalledWith(expect.objectContaining({ disableScorers: true }));
    const workflowsStore = await mastra.getStorage()!.getStore('workflows');
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
