import { describe, expect, it, vi } from 'vitest';
import { EventEmitterPubSub } from '../../../events/event-emitter';
import { Mastra } from '../../../mastra';
import { MockStore } from '../../../storage/mock';
import { createEmptyWorkflowSnapshot } from '../../../storage/workflow-snapshot';
import type { WorkflowRunState } from '../../../workflows/types';
import { WORKFLOW_TERMINAL_FOREACH_RUN_KEY } from '../../terminal-continuation';
import { WorkflowEventProcessor, resolveNestedWorkflowOwnedRunId } from '.';
import type { ProcessorArgs } from '.';

class ExposedWorkflowEventProcessor extends WorkflowEventProcessor {
  finish(args: ProcessorArgs) {
    return this.endWorkflow(args);
  }

  fail(args: ProcessorArgs) {
    return this.processWorkflowFail(args);
  }

  cancel(args: ProcessorArgs) {
    return this.processWorkflowCancel(args);
  }

  start(args: ProcessorArgs & { initialState?: Record<string, any> }) {
    return this.processWorkflowStart(args);
  }
}

async function setup() {
  const mastra = new Mastra({
    logger: false,
    storage: new MockStore(),
    workflows: {},
    pubsub: new EventEmitterPubSub(),
  });
  const workflows = await mastra.getStorage()?.getStore('workflows');
  if (!workflows) throw new Error('Expected workflow storage');
  return { mastra, workflows, processor: new ExposedWorkflowEventProcessor({ mastra }) };
}

function workflow(id: string, serializedStepGraph: ProcessorArgs['workflow']['serializedStepGraph'] = []) {
  return { id, serializedStepGraph, options: {} } as ProcessorArgs['workflow'];
}

function terminalArgs(
  runId: string,
  status: 'success' | 'failed' | 'canceled',
  finalState: Record<string, unknown>,
): ProcessorArgs {
  return {
    workflow: workflow('child'),
    workflowId: 'child',
    runId,
    executionPath: [0],
    stepResults: { __state: finalState } as ProcessorArgs['stepResults'],
    activeStepsPath: {},
    resumeSteps: [],
    prevResult:
      status === 'failed'
        ? ({ status, error: { name: 'Error', message: 'failed' } } as ProcessorArgs['prevResult'])
        : ({ status, output: { done: true } } as ProcessorArgs['prevResult']),
    requestContext: {},
    state: { stale: true },
  };
}

describe('WorkflowEventProcessor terminal recovery evidence', () => {
  it('resolves restart and time-travel ownership from the exact foreach iteration sidecar', () => {
    const metadata = {
      nestedRunId: 'scalar-run',
      __workflow_meta: {
        [WORKFLOW_TERMINAL_FOREACH_RUN_KEY]: { '0': 'iteration-zero', '1': 'iteration-one' },
      },
    };
    expect(resolveNestedWorkflowOwnedRunId({ metadata, isForEach: true, forEachIndex: 1 })).toBe('iteration-one');
    expect(resolveNestedWorkflowOwnedRunId({ metadata, isForEach: true })).toBeUndefined();
    expect(resolveNestedWorkflowOwnedRunId({ metadata, isForEach: false })).toBe('scalar-run');
  });

  it('passes exact event-local final state to success, failure, and cancellation terminal writes', async () => {
    const { mastra, workflows, processor } = await setup();
    const updateState = vi.spyOn(workflows, 'updateWorkflowState');

    const cases = [
      ['success', 'success-run', { terminal: 'success' }],
      ['failed', 'failed-run', { terminal: 'failed' }],
      ['canceled', 'canceled-run', { terminal: 'canceled' }],
    ] as const;

    for (const [status, runId, finalState] of cases) {
      await workflows.persistWorkflowSnapshot({
        workflowName: 'child',
        runId,
        snapshot: { ...createEmptyWorkflowSnapshot(runId), status: 'running' },
      });
      const args = terminalArgs(runId, status, finalState);

      if (status === 'success') await processor.finish(args);
      else if (status === 'failed') await processor.fail(args);
      else await processor.cancel(args);

      expect(updateState).toHaveBeenLastCalledWith({
        workflowName: 'child',
        runId,
        opts: expect.objectContaining({
          status,
          finalState,
        }),
      });
      await expect(workflows.loadWorkflowSnapshot({ workflowName: 'child', runId })).resolves.toMatchObject({
        context: { __state: finalState },
        value: finalState,
      });
    }

    await mastra.shutdown();
  });

  it('binds distinct nested foreach runs by iteration while preserving existing metadata', async () => {
    const { mastra, workflows, processor } = await setup();
    const parentRunId = 'parent-run';
    const parentSnapshot = {
      ...createEmptyWorkflowSnapshot(parentRunId),
      status: 'running' as const,
      serializedStepGraph: [
        {
          type: 'foreach' as const,
          step: { id: 'child', component: 'WORKFLOW' },
          opts: { concurrency: 2 },
        },
      ],
      context: {
        child: {
          status: 'running' as const,
          payload: [{ value: 0 }, { value: 1 }],
          output: [null, null],
          metadata: {
            preserved: 'step-metadata',
            __workflow_meta: {
              preserved: 'workflow-metadata',
            },
          },
        },
      },
    };
    await workflows.persistWorkflowSnapshot({ workflowName: 'parent', runId: parentRunId, snapshot: parentSnapshot });

    const parentWorkflow = {
      workflowId: 'parent',
      runId: parentRunId,
      stepId: 'child',
      executionPath: [0, 0],
      resume: false,
      stepResults: parentSnapshot.context,
      stepGraph: [
        {
          type: 'foreach',
          step: { id: 'child', component: 'WORKFLOW' },
          opts: { concurrency: 2 },
        },
      ],
      activeStepsPath: { child: [0] },
      resumeSteps: [],
      resumeData: undefined,
      input: { status: 'success', output: parentSnapshot.context.child.payload },
    } as ProcessorArgs['parentWorkflow'];

    for (const [forEachIndex, runId] of [
      [0, 'child-run-0'],
      [1, 'child-run-1'],
    ] as const) {
      await processor.start({
        workflow: workflow('child'),
        workflowId: 'child',
        runId,
        executionPath: [0],
        stepResults: {},
        activeStepsPath: {},
        resumeSteps: [],
        prevResult: { status: 'success', output: { value: forEachIndex } },
        requestContext: {},
        parentWorkflow: { ...parentWorkflow, executionPath: [0, forEachIndex] },
        ...(forEachIndex === 0 ? { forEachIndex } : {}),
      });
    }

    const stored = await workflows.loadWorkflowSnapshot({ workflowName: 'parent', runId: parentRunId });
    const metadata = stored?.context.child?.metadata as Record<string, any>;
    expect(metadata).toMatchObject({
      preserved: 'step-metadata',
      __workflow_meta: {
        preserved: 'workflow-metadata',
        [WORKFLOW_TERMINAL_FOREACH_RUN_KEY]: {
          '0': 'child-run-0',
          '1': 'child-run-1',
        },
      },
    });
    expect(metadata).not.toHaveProperty('nestedRunId');

    const scalarParentRunId = 'scalar-parent-run';
    const scalarGraph = [{ type: 'step' as const, step: { id: 'child', component: 'WORKFLOW' } }];
    await workflows.persistWorkflowSnapshot({
      workflowName: 'parent',
      runId: scalarParentRunId,
      snapshot: {
        ...createEmptyWorkflowSnapshot(scalarParentRunId),
        status: 'running',
        serializedStepGraph: scalarGraph,
        context: {
          child: { status: 'running', payload: {}, metadata: {} },
        } as WorkflowRunState['context'],
      },
    });
    await processor.start({
      workflow: workflow('child'),
      workflowId: 'child',
      runId: 'scalar-child-run',
      executionPath: [0],
      stepResults: {},
      activeStepsPath: {},
      resumeSteps: [],
      prevResult: { status: 'success', output: {} },
      requestContext: {},
      parentWorkflow: {
        ...parentWorkflow,
        runId: scalarParentRunId,
        executionPath: [0],
        stepGraph: scalarGraph,
      },
    });

    const scalarStored = await workflows.loadWorkflowSnapshot({
      workflowName: 'parent',
      runId: scalarParentRunId,
    });
    expect(scalarStored?.context.child?.metadata).toMatchObject({
      nestedRunId: 'scalar-child-run',
    });

    await mastra.shutdown();
  });

  it('does not persist or publish a child when atomic parent admission conflicts', async () => {
    const { mastra, workflows, processor } = await setup();
    const parentRunId = 'conflict-parent-run';
    const parentGraph = [{ type: 'step' as const, step: { id: 'child', component: 'WORKFLOW' } }];
    await workflows.persistWorkflowSnapshot({
      workflowName: 'parent',
      runId: parentRunId,
      snapshot: {
        ...createEmptyWorkflowSnapshot(parentRunId),
        status: 'running',
        serializedStepGraph: parentGraph,
        context: {
          child: { status: 'running', payload: {}, metadata: { nestedRunId: 'existing-child-run' } },
        } as WorkflowRunState['context'],
      },
    });
    const persist = vi.spyOn(workflows, 'persistWorkflowSnapshot');
    const publish = vi.spyOn(mastra.pubsub, 'publish');

    await expect(
      processor.start({
        workflow: workflow('child'),
        workflowId: 'child',
        runId: 'rejected-child-run',
        executionPath: [0],
        stepResults: {},
        activeStepsPath: {},
        resumeSteps: [],
        prevResult: { status: 'success', output: {} },
        requestContext: {},
        parentWorkflow: {
          workflowId: 'parent',
          runId: parentRunId,
          stepId: 'child',
          executionPath: [0],
          resume: false,
          stepResults: {},
          stepGraph: parentGraph,
          activeStepsPath: {},
          resumeSteps: [],
          resumeData: undefined,
          input: { status: 'success', output: {} },
        },
      }),
    ).rejects.toMatchObject({ id: 'MASTRA_WORKFLOW_NESTED_RUN_OWNERSHIP_CONFLICT' });
    expect(persist).not.toHaveBeenCalled();
    expect(publish).not.toHaveBeenCalled();
    await expect(
      workflows.getWorkflowRunById({ workflowName: 'child', runId: 'rejected-child-run' }),
    ).resolves.toBeNull();
    await expect(
      workflows.getWorkflowTerminalRecoveryAncestry({ workflowName: 'child', runId: 'rejected-child-run' }),
    ).resolves.toEqual({ status: 'missing_ancestry' });
    await mastra.shutdown();
  });

  it('does not announce a child when atomic admission rejects a terminal parent', async () => {
    const { mastra, workflows, processor } = await setup();
    const parentRunId = 'terminal-parent-run';
    const parentGraph = [{ type: 'step' as const, step: { id: 'child', component: 'WORKFLOW' } }];
    await workflows.persistWorkflowSnapshot({
      workflowName: 'parent',
      runId: parentRunId,
      snapshot: {
        ...createEmptyWorkflowSnapshot(parentRunId),
        status: 'success',
        serializedStepGraph: parentGraph,
      },
    });
    const persist = vi.spyOn(workflows, 'persistWorkflowSnapshot');
    const publish = vi.spyOn(mastra.pubsub, 'publish');

    await expect(
      processor.start({
        workflow: workflow('child'),
        workflowId: 'child',
        runId: 'late-child-run',
        executionPath: [0],
        stepResults: {},
        activeStepsPath: {},
        resumeSteps: [],
        prevResult: { status: 'success', output: {} },
        requestContext: {},
        parentWorkflow: {
          workflowId: 'parent',
          runId: parentRunId,
          stepId: 'child',
          executionPath: [0],
          resume: false,
          stepResults: {},
          stepGraph: parentGraph,
          activeStepsPath: {},
          resumeSteps: [],
          resumeData: undefined,
          input: { status: 'success', output: {} },
        },
      }),
    ).resolves.toBeUndefined();
    expect(persist).not.toHaveBeenCalled();
    expect(publish).not.toHaveBeenCalled();
    await expect(
      workflows.loadWorkflowSnapshot({ workflowName: 'child', runId: 'late-child-run' }),
    ).resolves.toBeNull();
    await mastra.shutdown();
  });

  it('announces accepted top-level and nested starts before scheduling their first step', async () => {
    const { mastra, workflows, processor } = await setup();
    const publish = vi.spyOn(mastra.pubsub, 'publish');
    const startArgs = {
      workflow: workflow('top-level'),
      workflowId: 'top-level',
      runId: 'top-level-run',
      executionPath: [0],
      stepResults: {},
      activeStepsPath: {},
      resumeSteps: [],
      prevResult: { status: 'success' as const, output: {} },
      requestContext: {},
    };

    await processor.start(startArgs);
    expect(publish.mock.calls.map(([channel, event]) => [channel, event.type])).toEqual([
      ['workflow.events.v2.top-level-run', 'watch'],
      ['workflows', 'workflow.step.run'],
    ]);

    publish.mockClear();
    const parentRunId = 'accepted-parent-run';
    const parentGraph = [{ type: 'step' as const, step: { id: 'child', component: 'WORKFLOW' } }];
    await workflows.persistWorkflowSnapshot({
      workflowName: 'parent',
      runId: parentRunId,
      snapshot: {
        ...createEmptyWorkflowSnapshot(parentRunId),
        status: 'running',
        serializedStepGraph: parentGraph,
      },
    });
    await processor.start({
      ...startArgs,
      workflow: workflow('child'),
      workflowId: 'child',
      runId: 'accepted-child-run',
      parentWorkflow: {
        workflowId: 'parent',
        runId: parentRunId,
        stepId: 'child',
        executionPath: [0],
        resume: false,
        stepResults: {},
        stepGraph: parentGraph,
        activeStepsPath: {},
        resumeSteps: [],
        resumeData: undefined,
        input: { status: 'success', output: {} },
      },
    });
    expect(publish.mock.calls.map(([channel, event]) => [channel, event.type])).toEqual([
      ['workflow.events.v2.accepted-child-run', 'watch'],
      ['workflows', 'workflow.step.run'],
    ]);
    await mastra.shutdown();
  });

  it('reuses immutable retained ancestry when a nested child resumes', async () => {
    const { mastra, workflows, processor } = await setup();
    const parentRunId = 'resume-parent-run';
    const childRunId = 'resume-child-run';
    const parentGraph = [{ type: 'step' as const, step: { id: 'child', component: 'WORKFLOW' } }];
    await workflows.persistWorkflowSnapshot({
      workflowName: 'parent',
      runId: parentRunId,
      snapshot: {
        ...createEmptyWorkflowSnapshot(parentRunId),
        status: 'running',
        serializedStepGraph: parentGraph,
        context: {
          child: { status: 'running', payload: {}, metadata: {} },
        } as WorkflowRunState['context'],
      },
    });
    const parentWorkflow = {
      workflowId: 'parent',
      runId: parentRunId,
      stepId: 'child',
      executionPath: [0],
      resume: false,
      stepResults: {},
      stepGraph: parentGraph,
      activeStepsPath: {},
      resumeSteps: [],
      resumeData: undefined,
      input: { status: 'success', output: {} },
    } as ProcessorArgs['parentWorkflow'];
    const args = {
      workflow: workflow('child'),
      workflowId: 'child',
      runId: childRunId,
      executionPath: [0],
      stepResults: {},
      activeStepsPath: {},
      resumeSteps: [],
      prevResult: { status: 'success' as const, output: {} },
      requestContext: {},
      parentWorkflow,
    };
    await processor.start(args);
    const initial = await workflows.getWorkflowTerminalRecoveryAncestry({ workflowName: 'child', runId: childRunId });
    expect(initial).toMatchObject({
      status: 'found',
      record: { ancestry: [{ resumeMetadata: { wasResume: false, resumeSteps: [] } }] },
    });
    const admittedChild = await workflows.loadWorkflowSnapshot({ workflowName: 'child', runId: childRunId });
    if (!admittedChild) throw new Error('Expected admitted child snapshot');
    await workflows.persistWorkflowSnapshot({
      workflowName: 'child',
      runId: childRunId,
      snapshot: { ...admittedChild, status: 'suspended' },
    });
    const persist = vi.spyOn(workflows, 'persistWorkflowSnapshot');

    await expect(
      processor.start({
        ...args,
        workflow: {
          ...workflow('child'),
          options: { shouldPersistSnapshot: () => false },
        } as ProcessorArgs['workflow'],
        resumeSteps: ['inner-step'],
        parentWorkflow: { ...parentWorkflow, resume: true, resumeSteps: ['child', 'inner-step'] },
      }),
    ).resolves.toBeUndefined();
    expect(persist).not.toHaveBeenCalled();
    await expect(workflows.loadWorkflowSnapshot({ workflowName: 'child', runId: childRunId })).resolves.toMatchObject({
      status: 'suspended',
    });
    await expect(
      workflows.getWorkflowTerminalRecoveryAncestry({ workflowName: 'child', runId: childRunId }),
    ).resolves.toEqual(initial);
    await mastra.shutdown();
  });

  it('retains a continuous child-to-root ancestry before terminalization', async () => {
    const { mastra, workflows, processor } = await setup();
    const rootGraph = [{ type: 'step' as const, step: { id: 'child', component: 'WORKFLOW' } }];
    const childGraph = [{ type: 'step' as const, step: { id: 'grandchild', component: 'WORKFLOW' } }];
    await workflows.persistWorkflowSnapshot({
      workflowName: 'root',
      runId: 'root-run',
      snapshot: {
        ...createEmptyWorkflowSnapshot('root-run'),
        status: 'running',
        serializedStepGraph: rootGraph,
      },
    });
    const rootParent = {
      workflowId: 'root',
      runId: 'root-run',
      stepId: 'child',
      executionPath: [0],
      resume: false,
      stepResults: {},
      stepGraph: rootGraph,
      activeStepsPath: { child: [0] },
      resumeSteps: [],
      resumeData: undefined,
      input: { status: 'success', output: {} },
    } as ProcessorArgs['parentWorkflow'];
    await processor.start({
      workflow: workflow('child', childGraph),
      workflowId: 'child',
      runId: 'child-run',
      executionPath: [0],
      stepResults: {},
      activeStepsPath: {},
      resumeSteps: [],
      prevResult: { status: 'success', output: {} },
      requestContext: {},
      parentWorkflow: rootParent,
    });

    await expect(
      workflows.getWorkflowTerminalRecoveryAncestry({ workflowName: 'child', runId: 'child-run' }),
    ).resolves.toMatchObject({
      status: 'found',
      record: {
        ancestry: [
          {
            childWorkflowName: 'child',
            childRunId: 'child-run',
            parentWorkflowName: 'root',
            parentRunId: 'root-run',
          },
        ],
      },
    });

    const childParent = {
      workflowId: 'child',
      runId: 'child-run',
      stepId: 'grandchild',
      executionPath: [0],
      resume: false,
      stepResults: {},
      stepGraph: childGraph,
      activeStepsPath: { grandchild: [0] },
      resumeSteps: [],
      resumeData: undefined,
      input: { status: 'success', output: {} },
      recoveryAncestry: rootParent?.recoveryAncestry,
    } as ProcessorArgs['parentWorkflow'];
    await processor.start({
      workflow: workflow('grandchild'),
      workflowId: 'grandchild',
      runId: 'grandchild-run',
      executionPath: [0],
      stepResults: {},
      activeStepsPath: {},
      resumeSteps: [],
      prevResult: { status: 'success', output: {} },
      requestContext: {},
      parentWorkflow: childParent,
    });

    await expect(
      workflows.getWorkflowTerminalRecoveryAncestry({ workflowName: 'grandchild', runId: 'grandchild-run' }),
    ).resolves.toMatchObject({
      status: 'found',
      record: {
        ancestry: [
          { childWorkflowName: 'grandchild', childRunId: 'grandchild-run', parentWorkflowName: 'child' },
          { childWorkflowName: 'child', childRunId: 'child-run', parentWorkflowName: 'root' },
        ],
      },
    });

    await mastra.shutdown();
  });
});
