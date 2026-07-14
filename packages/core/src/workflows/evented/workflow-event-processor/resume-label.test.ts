import { describe, expect, it, vi } from 'vitest';
import { z } from 'zod/v4';
import { createStep, createWorkflow } from '..';
import { EventEmitterPubSub } from '../../../events/event-emitter';
import { Mastra } from '../../../mastra';
import { MockStore } from '../../../storage/mock';
import type { ProcessorArgs } from '.';
import { WorkflowEventProcessor } from '.';

class TestWorkflowEventProcessor extends WorkflowEventProcessor {
  propagateSuspend(args: ProcessorArgs) {
    return this.processWorkflowSuspend(args);
  }

  aggregateBranches(args: any) {
    return this.aggregateBranchResults(args);
  }
}

function createProcessor() {
  const publish = vi.fn().mockResolvedValue(undefined);
  const processor = new TestWorkflowEventProcessor({
    mastra: { pubsub: { publish } } as any,
  });

  return { processor, publish };
}

function createArgs(resumeLabels?: Record<string, { stepId: string; foreachIndex?: number }>): ProcessorArgs {
  return {
    workflow: {
      stepGraph: [
        {
          type: 'step',
          step: { id: 'inner-step' },
        },
      ],
    } as any,
    workflowId: 'inner-workflow',
    runId: 'inner-run',
    executionPath: [0],
    resumeSteps: [],
    stepResults: {},
    prevResult: {
      status: 'suspended',
      suspendPayload: {
        reason: 'approval-required',
        __workflow_meta: {
          path: ['leaf-step'],
          resumeLabels,
        },
      },
    } as any,
    requestContext: {},
    activeStepsPath: {},
    parentWorkflow: {
      workflowId: 'outer-workflow',
      runId: 'outer-run',
      executionPath: [1],
      resume: false,
      stepResults: {},
      stepId: 'nested-workflow-step',
      stepGraph: [],
      activeStepsPath: {},
      resumeSteps: [],
      resumeData: undefined,
      input: {},
    },
  };
}

describe('WorkflowEventProcessor nested resume-label propagation', () => {
  it('maps every nested label to the parent workflow step without storage', async () => {
    const { processor, publish } = createProcessor();

    await processor.propagateSuspend(
      createArgs({
        approve: { stepId: 'leaf-step', foreachIndex: 3 },
        revise: { stepId: 'other-leaf-step' },
      }),
    );

    expect(publish).toHaveBeenCalledWith(
      'workflows',
      expect.objectContaining({
        type: 'workflow.step.end',
        runId: 'outer-run',
        data: expect.objectContaining({
          prevResult: expect.objectContaining({
            suspendPayload: expect.objectContaining({
              reason: 'approval-required',
              __workflow_meta: expect.objectContaining({
                runId: 'inner-run',
                path: ['inner-step', 'leaf-step'],
                resumeLabels: {
                  approve: { stepId: 'nested-workflow-step', foreachIndex: 3 },
                  revise: { stepId: 'nested-workflow-step', foreachIndex: undefined },
                },
              }),
            }),
          }),
        }),
      }),
    );
  });

  it('leaves the non-label suspend path unchanged', async () => {
    const { processor, publish } = createProcessor();

    await processor.propagateSuspend(createArgs());

    expect(publish).toHaveBeenCalledWith(
      'workflows',
      expect.objectContaining({
        data: expect.objectContaining({
          prevResult: expect.objectContaining({
            suspendPayload: expect.objectContaining({
              __workflow_meta: expect.objectContaining({
                path: ['inner-step', 'leaf-step'],
                resumeLabels: undefined,
              }),
            }),
          }),
        }),
      }),
    );
  });

  it.each(['parallel', 'conditional'] as const)('fails a %s suspension with colliding labels', async type => {
    const { processor, publish } = createProcessor();
    const suspendedResult = (stepId: string) => ({
      status: 'suspended',
      suspendPayload: {
        __workflow_meta: {
          resumeLabels: {
            approve: { stepId },
          },
        },
      },
    });

    await processor.aggregateBranches({
      workflow: { id: 'workflow' } as any,
      workflowId: 'workflow',
      runId: 'run',
      branchEntry: {
        type,
        steps: [
          { type: 'step', step: { id: 'branch-a' } },
          { type: 'step', step: { id: 'branch-b' } },
        ],
      } as any,
      branchExecutionPath: [0, 1],
      latestBranchResult: suspendedResult('branch-b') as any,
      resumeSteps: [],
      stepResults: {
        'branch-a': suspendedResult('branch-a'),
        'branch-b': suspendedResult('branch-b'),
      },
      activeStepsPath: {},
      requestContext: {},
      state: {},
    });

    expect(publish).toHaveBeenCalledWith(
      'workflows',
      expect.objectContaining({
        type: 'workflow.fail',
        data: expect.objectContaining({
          prevResult: expect.objectContaining({ status: 'failed' }),
        }),
      }),
    );
    expect(publish).not.toHaveBeenCalledWith('workflows', expect.objectContaining({ type: 'workflow.suspend' }));
  });
});

function createNestedLabelWorkflow(includeLabels = true) {
  const approvalAction = vi.fn(async ({ inputData, resumeData, suspend }) => {
    if (!resumeData) {
      return includeLabels
        ? await suspend({ reason: 'approval-required' }, { resumeLabel: ['approve-nested', 'revise-nested'] })
        : await suspend({ reason: 'approval-required' });
    }

    return { value: inputData.value + resumeData.value };
  });
  const approvalStep = createStep({
    id: 'approval-step',
    inputSchema: z.object({ value: z.number() }),
    outputSchema: z.object({ value: z.number() }),
    resumeSchema: z.object({ value: z.number() }),
    suspendSchema: z.object({ reason: z.string() }),
    execute: approvalAction,
  });

  const nestedWorkflow = createWorkflow({
    id: 'nested-label-workflow',
    inputSchema: z.object({ value: z.number() }),
    outputSchema: z.object({ value: z.number() }),
  })
    .then(approvalStep)
    .commit();

  const parentWorkflow = createWorkflow({
    id: 'parent-label-workflow',
    inputSchema: z.object({ value: z.number() }),
    outputSchema: z.object({ value: z.number() }),
  })
    .then(nestedWorkflow)
    .commit();

  return { approvalAction, nestedWorkflow, parentWorkflow };
}

describe('EventedWorkflow nested resume labels with persistence', () => {
  it('persists every parent label and resumes through an explicit label', async () => {
    const storage = new MockStore();
    const { nestedWorkflow, parentWorkflow } = createNestedLabelWorkflow();
    const mastra = new Mastra({
      logger: false,
      storage,
      pubsub: new EventEmitterPubSub(),
      workflows: { [parentWorkflow.id]: parentWorkflow },
    });

    await mastra.startWorkers();
    try {
      const run = await parentWorkflow.createRun({ runId: 'persisted-nested-label-run' });
      const suspended = await run.start({ inputData: { value: 2 } });
      expect(suspended.status).toBe('suspended');

      const store = await storage.getStore('workflows');
      const snapshot = await store?.loadWorkflowSnapshot({
        workflowName: parentWorkflow.id,
        runId: run.runId,
      });

      expect(snapshot?.resumeLabels).toEqual({
        'approve-nested': { stepId: nestedWorkflow.id, foreachIndex: undefined },
        'revise-nested': { stepId: nestedWorkflow.id, foreachIndex: undefined },
      });

      const resumed = await run.resume({
        label: 'revise-nested',
        resumeData: { value: 5 },
      });

      expect(resumed).toMatchObject({
        status: 'success',
        result: { value: 7 },
      });
    } finally {
      await mastra.stopWorkers();
    }
  });

  it('forwards nested labels through resumeStream', async () => {
    const storage = new MockStore();
    const { parentWorkflow } = createNestedLabelWorkflow();
    const mastra = new Mastra({
      logger: false,
      storage,
      pubsub: new EventEmitterPubSub(),
      workflows: { [parentWorkflow.id]: parentWorkflow },
    });

    await mastra.startWorkers();
    try {
      const run = await parentWorkflow.createRun({ runId: 'streamed-nested-label-run' });
      await run.start({ inputData: { value: 2 } });

      const stream = run.resumeStream({
        label: 'approve-nested',
        resumeData: { value: 5 },
      });
      for await (const _event of stream.fullStream) {
        // Consume the stream so the result settles after all events are observed.
      }

      await expect(stream.result).resolves.toMatchObject({
        status: 'success',
        result: { value: 7 },
      });
    } finally {
      await mastra.stopWorkers();
    }
  });

  it('routes distinct labels to the selected suspended nested branch', async () => {
    const storage = new MockStore();
    const branchA = vi.fn(async ({ inputData, resumeData, suspend }) => {
      if (!resumeData) {
        return await suspend({ reason: 'a' }, { resumeLabel: 'approve-a' });
      }
      return { value: inputData.value + resumeData.delta };
    });
    const branchB = vi.fn(async ({ inputData, resumeData, suspend }) => {
      if (!resumeData) {
        return await suspend({ reason: 'b' }, { resumeLabel: 'approve-b' });
      }
      return { value: inputData.value + resumeData.delta };
    });
    const makeBranch = (id: string, execute: typeof branchA) =>
      createStep({
        id,
        inputSchema: z.object({ value: z.number() }),
        outputSchema: z.object({ value: z.number() }),
        resumeSchema: z.object({ delta: z.number() }),
        suspendSchema: z.object({ reason: z.string() }),
        execute,
      });
    const nestedWorkflow = createWorkflow({
      id: 'nested-parallel-label-workflow',
      inputSchema: z.object({ value: z.number() }),
      outputSchema: z.object({
        'branch-a': z.object({ value: z.number() }),
        'branch-b': z.object({ value: z.number() }),
      }),
    })
      .parallel([makeBranch('branch-a', branchA), makeBranch('branch-b', branchB)])
      .commit();
    const parentWorkflow = createWorkflow({
      id: 'parent-parallel-label-workflow',
      inputSchema: z.object({ value: z.number() }),
      outputSchema: nestedWorkflow.outputSchema,
    })
      .then(nestedWorkflow)
      .commit();
    const mastra = new Mastra({
      logger: false,
      storage,
      pubsub: new EventEmitterPubSub(),
      workflows: { [parentWorkflow.id]: parentWorkflow },
    });

    await mastra.startWorkers();
    try {
      const run = await parentWorkflow.createRun({ runId: 'nested-parallel-label-run' });
      const suspended = await run.start({ inputData: { value: 10 } });
      expect(suspended.status).toBe('suspended');

      const store = await storage.getStore('workflows');
      const snapshot = await store?.loadWorkflowSnapshot({
        workflowName: parentWorkflow.id,
        runId: run.runId,
      });
      expect(snapshot?.resumeLabels).toEqual({
        'approve-a': { stepId: nestedWorkflow.id, foreachIndex: undefined },
        'approve-b': { stepId: nestedWorkflow.id, foreachIndex: undefined },
      });

      const afterB = await run.resume({ label: 'approve-b', resumeData: { delta: 2 } });
      expect(afterB.status).toBe('suspended');
      expect(branchA).toHaveBeenCalledTimes(1);
      expect(branchB).toHaveBeenCalledTimes(2);

      const completed = await run.resume({ label: 'approve-a', resumeData: { delta: 1 } });
      expect(completed).toMatchObject({
        status: 'success',
        result: {
          'branch-a': { value: 11 },
          'branch-b': { value: 12 },
        },
      });
      expect(branchA).toHaveBeenCalledTimes(2);
      expect(branchB).toHaveBeenCalledTimes(2);
    } finally {
      await mastra.stopWorkers();
    }
  });

  it('fails a foreach suspension instead of persisting an ambiguous label', async () => {
    const storage = new MockStore();
    const itemSchema = z.object({ id: z.string() });
    const approvalStep = createStep({
      id: 'foreach-collision-step',
      inputSchema: itemSchema,
      outputSchema: itemSchema,
      suspendSchema: z.object({ reason: z.string() }),
      execute: async ({ inputData, suspend }) =>
        suspend({ reason: inputData.id }, { resumeLabel: 'approve' }) as Promise<{ id: string }>,
    });
    const workflow = createWorkflow({
      id: 'foreach-collision-workflow',
      inputSchema: z.array(itemSchema),
      outputSchema: z.array(itemSchema),
    })
      .foreach(approvalStep, { concurrency: 2 })
      .commit();
    const mastra = new Mastra({
      logger: false,
      storage,
      pubsub: new EventEmitterPubSub(),
      workflows: { [workflow.id]: workflow },
    });

    await mastra.startWorkers();
    try {
      const run = await workflow.createRun({ runId: 'foreach-label-collision-run' });
      const result = await run.start({ inputData: [{ id: 'a' }, { id: 'b' }] });
      expect(result.status).toBe('failed');
      expect((result as any).error?.message).toBe('Invalid workflow resume label metadata');
    } finally {
      await mastra.stopWorkers();
    }
  });

  it('propagates a nested label collision as a workflow failure', async () => {
    const storage = new MockStore();
    const schema = z.object({ value: z.number() });
    const makeApprovalStep = (id: string) =>
      createStep({
        id,
        inputSchema: schema,
        outputSchema: schema,
        suspendSchema: z.object({ reason: z.string() }),
        execute: async ({ suspend }) =>
          suspend({ reason: id }, { resumeLabel: 'approve' }) as Promise<{ value: number }>,
      });
    const nestedWorkflow = createWorkflow({
      id: 'nested-label-collision-workflow',
      inputSchema: schema,
      outputSchema: z.object({
        'nested-branch-a': schema,
        'nested-branch-b': schema,
      }),
    })
      .parallel([makeApprovalStep('nested-branch-a'), makeApprovalStep('nested-branch-b')])
      .commit();
    const parentWorkflow = createWorkflow({
      id: 'parent-label-collision-workflow',
      inputSchema: schema,
      outputSchema: nestedWorkflow.outputSchema,
    })
      .then(nestedWorkflow)
      .commit();
    const mastra = new Mastra({
      logger: false,
      storage,
      pubsub: new EventEmitterPubSub(),
      workflows: { [parentWorkflow.id]: parentWorkflow },
    });

    await mastra.startWorkers();
    try {
      const run = await parentWorkflow.createRun({ runId: 'nested-label-collision-run' });
      const result = await run.start({ inputData: { value: 1 } });
      expect(result.status).toBe('failed');
      expect((result as any).error?.message).toBe('Invalid workflow resume label metadata');
    } finally {
      await mastra.stopWorkers();
    }
  });

  it('preserves and routes a nested foreach label index', async () => {
    const storage = new MockStore();
    const mapAction = vi.fn(async ({ inputData, resumeData, suspend }) => {
      if (!resumeData) {
        return await suspend({ reason: inputData.id }, { resumeLabel: `approve-${inputData.id}` });
      }
      return { id: inputData.id, value: inputData.value + resumeData.delta };
    });
    const mapStep = createStep({
      id: 'nested-map-step',
      inputSchema: z.object({ id: z.string(), value: z.number() }),
      outputSchema: z.object({ id: z.string(), value: z.number() }),
      resumeSchema: z.object({ delta: z.number() }),
      suspendSchema: z.object({ reason: z.string() }),
      execute: mapAction,
    });
    const itemsSchema = z.array(z.object({ id: z.string(), value: z.number() }));
    const nestedWorkflow = createWorkflow({
      id: 'nested-foreach-label-workflow',
      inputSchema: itemsSchema,
      outputSchema: itemsSchema,
    })
      .foreach(mapStep, { concurrency: 3 })
      .commit();
    const parentWorkflow = createWorkflow({
      id: 'parent-foreach-label-workflow',
      inputSchema: itemsSchema,
      outputSchema: itemsSchema,
    })
      .then(nestedWorkflow)
      .commit();
    const mastra = new Mastra({
      logger: false,
      storage,
      pubsub: new EventEmitterPubSub(),
      workflows: { [parentWorkflow.id]: parentWorkflow },
    });

    await mastra.startWorkers();
    try {
      const run = await parentWorkflow.createRun({ runId: 'nested-foreach-label-run' });
      const suspended = await run.start({
        inputData: [
          { id: 'a', value: 1 },
          { id: 'b', value: 2 },
          { id: 'c', value: 3 },
        ],
      });
      expect(suspended.status).toBe('suspended');

      const store = await storage.getStore('workflows');
      const snapshot = await store?.loadWorkflowSnapshot({
        workflowName: parentWorkflow.id,
        runId: run.runId,
      });
      expect(snapshot?.resumeLabels?.['approve-b']).toEqual({
        stepId: nestedWorkflow.id,
        foreachIndex: 1,
      });

      await expect(run.resume({ label: 'approve-b', forEachIndex: 0, resumeData: { delta: 10 } })).rejects.toThrow(
        'Resume label does not match the requested forEachIndex',
      );
      await expect(run.resume({ label: 'approve-b', forEachIndex: 99, resumeData: { delta: 10 } })).rejects.toThrow(
        'Resume label does not match the requested forEachIndex',
      );
      expect(mapAction).toHaveBeenCalledTimes(3);

      const afterB = await run.resume({ label: 'approve-b', forEachIndex: 1, resumeData: { delta: 10 } });
      expect(afterB.status).toBe('suspended');
      expect(mapAction).toHaveBeenCalledTimes(4);
      expect(mapAction.mock.calls[3]?.[0]).toMatchObject({
        inputData: { id: 'b', value: 2 },
      });
    } finally {
      await mastra.stopWorkers();
    }
  });

  it('routes a label across multiple nested workflow boundaries', async () => {
    const storage = new MockStore();
    const leafStep = createStep({
      id: 'deep-label-step',
      inputSchema: z.object({ value: z.number() }),
      outputSchema: z.object({ value: z.number() }),
      resumeSchema: z.object({ delta: z.number() }),
      suspendSchema: z.object({ reason: z.string() }),
      execute: async ({ inputData, resumeData, suspend }) => {
        if (!resumeData) {
          return await suspend({ reason: 'deep-approval' }, { resumeLabel: 'approve-deep' });
        }
        return { value: inputData.value + resumeData.delta };
      },
    });
    const schema = z.object({ value: z.number() });
    const innerWorkflow = createWorkflow({ id: 'deep-inner-workflow', inputSchema: schema, outputSchema: schema })
      .then(leafStep)
      .commit();
    const middleWorkflow = createWorkflow({ id: 'deep-middle-workflow', inputSchema: schema, outputSchema: schema })
      .then(innerWorkflow)
      .commit();
    const outerWorkflow = createWorkflow({ id: 'deep-outer-workflow', inputSchema: schema, outputSchema: schema })
      .then(middleWorkflow)
      .commit();
    const mastra = new Mastra({
      logger: false,
      storage,
      pubsub: new EventEmitterPubSub(),
      workflows: { [outerWorkflow.id]: outerWorkflow },
    });

    await mastra.startWorkers();
    try {
      const run = await outerWorkflow.createRun({ runId: 'deep-nested-label-run' });
      const suspended = await run.start({ inputData: { value: 4 } });
      expect(suspended.status).toBe('suspended');

      const store = await storage.getStore('workflows');
      const snapshot = await store?.loadWorkflowSnapshot({
        workflowName: outerWorkflow.id,
        runId: run.runId,
      });
      expect(snapshot?.resumeLabels?.['approve-deep']).toEqual({
        stepId: middleWorkflow.id,
        foreachIndex: undefined,
      });

      const completed = await run.resume({ label: 'approve-deep', resumeData: { delta: 6 } });
      expect(completed).toMatchObject({ status: 'success', result: { value: 10 } });
    } finally {
      await mastra.stopWorkers();
    }
  });

  it('routes a label to a nested parallel branch inside the selected outer foreach iteration', async () => {
    const storage = new MockStore();
    const branchA = vi.fn(async ({ inputData, resumeData, suspend }) => {
      if (!resumeData) {
        return await suspend({ reason: 'a' }, { resumeLabel: `approve-a-${inputData.id}` });
      }
      return { id: inputData.id, value: inputData.value + resumeData.delta };
    });
    const branchB = vi.fn(async ({ inputData, resumeData, suspend }) => {
      if (!resumeData) {
        return await suspend({ reason: 'b' }, { resumeLabel: `approve-b-${inputData.id}` });
      }
      return { id: inputData.id, value: inputData.value + resumeData.delta };
    });
    const itemSchema = z.object({ id: z.string(), value: z.number() });
    const makeBranch = (id: string, execute: typeof branchA) =>
      createStep({
        id,
        inputSchema: itemSchema,
        outputSchema: itemSchema,
        resumeSchema: z.object({ delta: z.number() }),
        suspendSchema: z.object({ reason: z.string() }),
        execute,
      });
    const nestedOutputSchema = z.object({
      'outer-branch-a': itemSchema,
      'outer-branch-b': itemSchema,
    });
    const nestedWorkflow = createWorkflow({
      id: 'outer-foreach-nested-workflow',
      inputSchema: itemSchema,
      outputSchema: nestedOutputSchema,
    })
      .parallel([makeBranch('outer-branch-a', branchA), makeBranch('outer-branch-b', branchB)])
      .commit();
    const parentWorkflow = createWorkflow({
      id: 'outer-foreach-parent-workflow',
      inputSchema: z.array(itemSchema),
      outputSchema: z.array(nestedOutputSchema),
    })
      .foreach(nestedWorkflow, { concurrency: 2 })
      .commit();
    const mastra = new Mastra({
      logger: false,
      storage,
      pubsub: new EventEmitterPubSub(),
      workflows: { [parentWorkflow.id]: parentWorkflow },
    });

    await mastra.startWorkers();
    try {
      const run = await parentWorkflow.createRun({ runId: 'outer-foreach-nested-label-run' });
      const suspended = await run.start({
        inputData: [
          { id: 'item-1', value: 10 },
          { id: 'item-2', value: 20 },
        ],
      });
      expect(suspended.status).toBe('suspended');

      const store = await storage.getStore('workflows');
      const snapshot = await store?.loadWorkflowSnapshot({
        workflowName: parentWorkflow.id,
        runId: run.runId,
      });
      expect(snapshot?.resumeLabels?.['approve-b-item-2']).toEqual({
        stepId: nestedWorkflow.id,
        foreachIndex: 1,
      });

      await store?.updateWorkflowState({
        workflowName: parentWorkflow.id,
        runId: run.runId,
        opts: {
          suspendedPaths: {
            [nestedWorkflow.id]: [0, 1],
          },
        },
      });
      await expect(run.resume({ label: 'approve-b-item-2', resumeData: { delta: 2 } })).rejects.toThrow(
        'Resume label was not found for this workflow run',
      );
      await store?.updateWorkflowState({
        workflowName: parentWorkflow.id,
        runId: run.runId,
        opts: {
          suspendedPaths: snapshot?.suspendedPaths ?? {},
        },
      });

      await store?.updateWorkflowState({
        workflowName: parentWorkflow.id,
        runId: run.runId,
        opts: {
          resumeLabels: {
            ...snapshot?.resumeLabels,
            'missing-index': { stepId: nestedWorkflow.id },
            'out-of-range': { stepId: nestedWorkflow.id, foreachIndex: 99 },
          },
        },
      });
      await expect(run.resume({ label: 'missing-index', resumeData: { delta: 2 } })).rejects.toThrow(
        'Resume label was not found for this workflow run',
      );
      await expect(run.resume({ label: 'out-of-range', resumeData: { delta: 2 } })).rejects.toThrow(
        'Resume label was not found for this workflow run',
      );

      const afterSelectedBranch = await run.resume({
        label: 'approve-b-item-2',
        forEachIndex: 1,
        resumeData: { delta: 2 },
      });
      expect(afterSelectedBranch.status).toBe('suspended');
      expect(branchA).toHaveBeenCalledTimes(2);
      expect(branchB).toHaveBeenCalledTimes(3);
      expect(branchB.mock.calls.some(([args]) => args.inputData.id === 'item-2' && args.resumeData?.delta === 2)).toBe(
        true,
      );
      expect(branchA.mock.calls.some(([args]) => args.resumeData !== undefined)).toBe(false);
    } finally {
      await mastra.stopWorkers();
    }
  });

  it('preserves explicit step and foreach-index resume for a nested workflow body', async () => {
    const storage = new MockStore();
    const { approvalAction, nestedWorkflow } = createNestedLabelWorkflow(false);
    const itemSchema = z.object({ value: z.number() });
    const parentWorkflow = createWorkflow({
      id: 'explicit-outer-foreach-parent-workflow',
      inputSchema: z.array(itemSchema),
      outputSchema: z.array(itemSchema),
    })
      .foreach(nestedWorkflow, { concurrency: 2 })
      .commit();
    const mastra = new Mastra({
      logger: false,
      storage,
      pubsub: new EventEmitterPubSub(),
      workflows: { [parentWorkflow.id]: parentWorkflow },
    });

    await mastra.startWorkers();
    try {
      const run = await parentWorkflow.createRun({ runId: 'explicit-outer-foreach-resume-run' });
      await run.start({ inputData: [{ value: 1 }, { value: 2 }] });

      const resumed = await run.resume({
        step: nestedWorkflow.id,
        forEachIndex: 1,
        resumeData: { value: 5 },
      });
      expect(resumed.status).toBe('suspended');
      expect(approvalAction).toHaveBeenCalledTimes(3);
      expect(approvalAction.mock.calls[2]?.[0]).toMatchObject({
        inputData: { value: 2 },
        resumeData: { value: 5 },
      });
    } finally {
      await mastra.stopWorkers();
    }
  });

  it('validates explicit nested step paths before dispatch', async () => {
    const storage = new MockStore();
    const { approvalAction, nestedWorkflow, parentWorkflow } = createNestedLabelWorkflow();
    const mastra = new Mastra({
      logger: false,
      storage,
      pubsub: new EventEmitterPubSub(),
      workflows: { [parentWorkflow.id]: parentWorkflow },
    });

    await mastra.startWorkers();
    try {
      const validRun = await parentWorkflow.createRun({ runId: 'valid-explicit-nested-path-run' });
      await validRun.start({ inputData: { value: 2 } });
      const completed = await validRun.resume({
        step: [nestedWorkflow.id, 'approval-step'],
        resumeData: { value: 5 },
      });
      expect(completed).toMatchObject({ status: 'success', result: { value: 7 } });
      expect(approvalAction).toHaveBeenCalledTimes(2);

      const invalidRun = await parentWorkflow.createRun({ runId: 'invalid-explicit-nested-path-run' });
      await invalidRun.start({ inputData: { value: 3 } });
      expect(approvalAction).toHaveBeenCalledTimes(3);
      const failed = await invalidRun.resume({
        step: [nestedWorkflow.id, 'bogus-step'],
        resumeData: { value: 5 },
      });
      expect(failed.status).toBe('failed');
      expect((failed as any).error?.message).toBe('No matching suspended step found in nested workflow');
      expect(approvalAction).toHaveBeenCalledTimes(3);

      const overlongRun = await parentWorkflow.createRun({ runId: 'overlong-explicit-nested-path-run' });
      await overlongRun.start({ inputData: { value: 4 } });
      expect(approvalAction).toHaveBeenCalledTimes(4);
      const overlong = await overlongRun.resume({
        step: [nestedWorkflow.id, 'approval-step', 'bogus-step'],
        resumeData: { value: 5 },
      });
      expect(overlong.status).toBe('failed');
      expect((overlong as any).error?.message).toBe('No matching suspended step found in nested workflow');
      expect(approvalAction).toHaveBeenCalledTimes(4);
    } finally {
      await mastra.stopWorkers();
    }
  });

  it('fails closed when a child label target is not suspended', async () => {
    const storage = new MockStore();
    const { approvalAction, nestedWorkflow, parentWorkflow } = createNestedLabelWorkflow();
    const mastra = new Mastra({
      logger: false,
      storage,
      pubsub: new EventEmitterPubSub(),
      workflows: { [parentWorkflow.id]: parentWorkflow },
    });

    await mastra.startWorkers();
    try {
      const run = await parentWorkflow.createRun({ runId: 'dangling-child-label-run' });
      await run.start({ inputData: { value: 2 } });

      const store = await storage.getStore('workflows');
      const parentSnapshot = await store?.loadWorkflowSnapshot({
        workflowName: parentWorkflow.id,
        runId: run.runId,
      });
      const nestedRunId = (parentSnapshot?.context?.[nestedWorkflow.id] as any)?.suspendPayload?.__workflow_meta?.runId;
      expect(nestedRunId).toBeTypeOf('string');

      await store?.updateWorkflowState({
        workflowName: nestedWorkflow.id,
        runId: nestedRunId,
        opts: {
          resumeLabels: {
            'approve-nested': { stepId: 'approval-step' },
          },
          suspendedPaths: {
            'approval-step': [999],
          },
        },
      });

      const result = await run.resume({ label: 'approve-nested', resumeData: { value: 5 } });
      expect(result.status).toBe('failed');
      expect(approvalAction).toHaveBeenCalledTimes(1);
      expect((result as any).error?.message).toBe('No matching suspended step found in nested workflow');
    } finally {
      await mastra.stopWorkers();
    }
  });

  it('rejects unknown, prototype, and empty labels without echoing label data', async () => {
    const storage = new MockStore();
    const { approvalAction, nestedWorkflow, parentWorkflow } = createNestedLabelWorkflow();
    const mastra = new Mastra({
      logger: false,
      storage,
      pubsub: new EventEmitterPubSub(),
      workflows: { [parentWorkflow.id]: parentWorkflow },
    });

    await mastra.startWorkers();
    try {
      const run = await parentWorkflow.createRun({ runId: 'invalid-nested-label-run' });
      await run.start({ inputData: { value: 2 } });

      const sensitiveUnknownLabel = `missing-${'sensitive'.repeat(100)}`;
      await expect(run.resume({ label: sensitiveUnknownLabel, resumeData: { value: 5 } })).rejects.toThrow(
        'Resume label is invalid',
      );
      await expect(run.resume({ label: '__proto__', resumeData: { value: 5 } })).rejects.toThrow(
        'Resume label was not found for this workflow run',
      );
      await expect(run.resume({ label: '', resumeData: { value: 5 } })).rejects.toThrow(
        'Resume label must be a non-empty string',
      );

      const store = await storage.getStore('workflows');
      await store?.updateWorkflowState({
        workflowName: parentWorkflow.id,
        runId: run.runId,
        opts: {
          resumeLabels: Object.fromEntries(
            Array.from({ length: 65 }, (_, index) => [`label-${index}`, { stepId: nestedWorkflow.id }]),
          ),
        },
      });
      await expect(run.resume({ label: 'label-0', resumeData: { value: 5 } })).rejects.toThrow(
        'Resume label was not found for this workflow run',
      );

      await store?.updateWorkflowState({
        workflowName: parentWorkflow.id,
        runId: run.runId,
        opts: {
          resumeLabels: {
            corrupt: { stepId: '', foreachIndex: -1 },
          },
        },
      });
      await expect(run.resume({ label: 'corrupt', resumeData: { value: 5 } })).rejects.toThrow(
        'Resume label was not found for this workflow run',
      );

      await store?.updateWorkflowState({
        workflowName: parentWorkflow.id,
        runId: run.runId,
        opts: {
          resumeLabels: {
            mismatch: { stepId: nestedWorkflow.id },
          },
          suspendedPaths: {
            [nestedWorkflow.id]: [999],
          },
        },
      });
      await expect(run.resume({ label: 'mismatch', resumeData: { value: 5 } })).rejects.toThrow(
        'Resume label was not found for this workflow run',
      );
      expect(approvalAction).toHaveBeenCalledTimes(1);
    } finally {
      await mastra.stopWorkers();
    }
  });
});
