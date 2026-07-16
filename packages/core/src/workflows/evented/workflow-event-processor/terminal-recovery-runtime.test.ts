import { describe, expect, it, vi } from 'vitest';
import { EventEmitterPubSub } from '../../../events/event-emitter';
import { Mastra } from '../../../mastra';
import { MockStore } from '../../../storage/mock';
import { createEmptyWorkflowSnapshot } from '../../../storage/workflow-snapshot';
import type { WorkflowRunState } from '../../../workflows/types';
import { WORKFLOW_TERMINAL_FOREACH_RUN_KEY } from '../../terminal-continuation';
import {
  WorkflowEventProcessor,
  createNestedWorkflowRunId,
  createNestedWorkflowExecutionGeneration,
  resolveNestedWorkflowDispatchRunId,
  resolveNestedWorkflowLoopIteration,
  resolveNestedWorkflowOwnedRunId,
} from '.';
import type { ProcessorArgs } from '.';

function withLifecycleExecution(args: ProcessorArgs): ProcessorArgs {
  return {
    executionGeneration: `test-generation:${args.workflowId}:${args.runId}`,
    lifecycleResumeAttempt: 0,
    lifecycleStepStates: {},
    ...args,
  };
}

class ExposedWorkflowEventProcessor extends WorkflowEventProcessor {
  finish(args: ProcessorArgs) {
    return this.endWorkflow(withLifecycleExecution(args));
  }

  fail(args: ProcessorArgs) {
    return this.processWorkflowFail(withLifecycleExecution(args));
  }

  cancel(args: ProcessorArgs) {
    return this.processWorkflowCancel(withLifecycleExecution(args));
  }

  start(args: ProcessorArgs & { initialState?: Record<string, any> }) {
    return this.processWorkflowStart(
      withLifecycleExecution(args) as ProcessorArgs & {
        initialState?: Record<string, any>;
      },
    );
  }

  runStep(args: ProcessorArgs) {
    return this.processWorkflowStepRun(withLifecycleExecution(args));
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

function workflowControlCalls(publish: ReturnType<typeof vi.spyOn>) {
  return publish.mock.calls
    .filter(([channel]) => !String(channel).startsWith('workflow.lifecycle.v1.'))
    .map(([channel, event]) => [channel, event.type]);
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

  it('ignores inherited, accessor, and non-enumerable ownership without executing accessors', () => {
    let accessorReads = 0;
    const scalarMetadata = Object.create({ nestedRunId: 'inherited-run' });
    Object.defineProperty(scalarMetadata, 'nestedRunId', {
      enumerable: true,
      get: () => {
        accessorReads += 1;
        return 'accessor-run';
      },
    });
    expect(resolveNestedWorkflowOwnedRunId({ metadata: scalarMetadata, isForEach: false })).toBeUndefined();

    const iterationRuns = Object.create({ '0': 'inherited-iteration' });
    Object.defineProperty(iterationRuns, '0', {
      enumerable: true,
      get: () => {
        accessorReads += 1;
        return 'accessor-iteration';
      },
    });
    const foreachMetadata = {
      __workflow_meta: { [WORKFLOW_TERMINAL_FOREACH_RUN_KEY]: iterationRuns },
    };
    expect(
      resolveNestedWorkflowOwnedRunId({ metadata: foreachMetadata, isForEach: true, forEachIndex: 0 }),
    ).toBeUndefined();

    const loopMetadata = Object.create({ iterationCount: 9 });
    Object.defineProperty(loopMetadata, 'iterationCount', {
      enumerable: true,
      get: () => {
        accessorReads += 1;
        return 10;
      },
    });
    expect(resolveNestedWorkflowLoopIteration(loopMetadata)).toBe(0);
    expect(accessorReads).toBe(0);

    const base = {
      parentWorkflowId: 'parent',
      parentRunId: 'parent-run',
      nestedWorkflowId: 'child',
      stepId: 'child',
      executionPath: [0],
    };
    expect(resolveNestedWorkflowDispatchRunId({ ...base, ownedRunId: undefined })).toBe(
      createNestedWorkflowRunId(base),
    );
  });

  it('fails closed on malformed retained scalar and foreach owner identities', () => {
    const invalidOwners: unknown[] = ['', 'x'.repeat(513), `run${String.fromCharCode(0xd800)}`, null, 42];
    const base = {
      parentWorkflowId: 'parent',
      parentRunId: 'parent-run',
      nestedWorkflowId: 'child',
      stepId: 'child',
      executionPath: [0],
    };
    for (const ownedRunId of invalidOwners) {
      expect(() =>
        resolveNestedWorkflowOwnedRunId({ metadata: { nestedRunId: ownedRunId }, isForEach: false }),
      ).toThrow(TypeError);
      expect(() =>
        resolveNestedWorkflowOwnedRunId({
          metadata: { __workflow_meta: { [WORKFLOW_TERMINAL_FOREACH_RUN_KEY]: { '0': ownedRunId } } },
          isForEach: true,
          forEachIndex: 0,
        }),
      ).toThrow(TypeError);
      if (typeof ownedRunId === 'string') {
        expect(() => resolveNestedWorkflowDispatchRunId({ ...base, ownedRunId })).toThrow(TypeError);
      }
    }
    for (const forEachIndex of [-1, Number.MAX_SAFE_INTEGER + 1]) {
      expect(() =>
        resolveNestedWorkflowOwnedRunId({
          metadata: { __workflow_meta: { [WORKFLOW_TERMINAL_FOREACH_RUN_KEY]: { [String(forEachIndex)]: 'run' } } },
          isForEach: true,
          forEachIndex,
        }),
      ).toThrow(TypeError);
    }
  });

  it('derives one nested run id per canonical broker-delivery coordinate', () => {
    const base = {
      parentWorkflowId: 'parent',
      parentRunId: 'parent-run',
      nestedWorkflowId: 'child',
      stepId: 'child',
      executionPath: [0],
    };
    expect(createNestedWorkflowRunId(base)).toBe(createNestedWorkflowRunId(base));
    expect(createNestedWorkflowRunId(base)).not.toBe(createNestedWorkflowRunId({ ...base, executionPath: [1] }));
    expect(createNestedWorkflowRunId({ ...base, loopIteration: 1 })).not.toBe(
      createNestedWorkflowRunId({ ...base, loopIteration: 2 }),
    );
    expect(resolveNestedWorkflowDispatchRunId({ ...base, ownedRunId: 'retained-run' })).toBe('retained-run');
    expect(resolveNestedWorkflowDispatchRunId(base)).not.toBe(
      resolveNestedWorkflowDispatchRunId({ ...base, parentRunId: 'new-execution-generation' }),
    );
  });

  it('derives child execution lineage from the parent lineage and stable child coordinate', () => {
    const base = {
      parentWorkflowId: 'parent',
      parentRunId: 'parent-run',
      parentExecutionGeneration: 'parent-generation-a',
      nestedWorkflowId: 'child',
      nestedRunId: 'child-run',
      stepId: 'child',
      executionPath: [0],
    };
    expect(createNestedWorkflowExecutionGeneration(base)).toBe(createNestedWorkflowExecutionGeneration(base));
    expect(createNestedWorkflowExecutionGeneration(base)).toMatch(/^wfeg:v1:[a-f0-9]{64}$/);
    expect(createNestedWorkflowExecutionGeneration(base)).not.toBe(
      createNestedWorkflowExecutionGeneration({ ...base, parentExecutionGeneration: 'parent-generation-b' }),
    );
  });

  it('rejects unbounded or non-canonical nested execution coordinates', () => {
    const base = {
      parentWorkflowId: 'parent',
      parentRunId: 'parent-run',
      nestedWorkflowId: 'child',
      stepId: 'child',
      executionPath: [0],
    };
    for (const field of ['parentWorkflowId', 'parentRunId', 'nestedWorkflowId', 'stepId'] as const) {
      expect(() => createNestedWorkflowRunId({ ...base, [field]: 'x'.repeat(513) })).toThrow(TypeError);
    }
    expect(() => createNestedWorkflowRunId({ ...base, executionPath: Array(257).fill(0) })).toThrow(TypeError);
    expect(() => createNestedWorkflowRunId({ ...base, executionPath: [Number.MAX_SAFE_INTEGER + 1] })).toThrow(
      TypeError,
    );
    expect(() => createNestedWorkflowRunId({ ...base, executionPath: Array(2) })).toThrow(TypeError);
    expect(() => createNestedWorkflowRunId({ ...base, loopIteration: Number.MAX_SAFE_INTEGER + 1 })).toThrow(TypeError);
    expect(() =>
      resolveNestedWorkflowDispatchRunId({ ...base, executionPath: Array(257).fill(0), ownedRunId: 'retained-run' }),
    ).toThrow(TypeError);
  });

  it('publishes the same child run id when an ordinary nested start is redelivered', async () => {
    const { mastra, processor } = await setup();
    const nestedStep = { id: 'child', component: 'WORKFLOW' as const, options: {} };
    const parentWorkflow = {
      id: 'parent',
      stepGraph: [{ type: 'step' as const, step: nestedStep }],
      serializedStepGraph: [{ type: 'step' as const, step: { id: 'child', component: 'WORKFLOW' } }],
      retryConfig: { attempts: 0 },
      options: {},
    } as unknown as ProcessorArgs['workflow'];
    const args: ProcessorArgs = {
      workflow: parentWorkflow,
      workflowId: 'parent',
      runId: 'parent-run',
      executionPath: [0],
      stepResults: {},
      activeStepsPath: {},
      resumeSteps: [],
      prevResult: { status: 'success', output: {} },
      requestContext: {},
      executionGeneration: 'parent-generation-a',
      lifecycleResumeAttempt: 0,
      lifecycleStepStates: {},
    };
    const publish = vi.spyOn(mastra.pubsub, 'publish');

    await processor.runStep(structuredClone(args));
    await processor.runStep(structuredClone(args));

    const starts = publish.mock.calls
      .map(([, event]) => event)
      .filter(event => event.type === 'workflow.start') as Array<{
      data: { runId: string; executionGeneration: string };
    }>;
    expect(starts).toHaveLength(2);
    expect(starts[0]!.data.runId).toMatch(/^wfn:v1:[a-f0-9]{64}$/);
    expect(starts[1]!.data.runId).toBe(starts[0]!.data.runId);
    expect(starts[0]!.data.executionGeneration).toMatch(/^wfeg:v1:[a-f0-9]{64}$/);
    expect(starts[1]!.data.executionGeneration).toBe(starts[0]!.data.executionGeneration);

    await processor.runStep(structuredClone({ ...args, executionGeneration: 'parent-generation-b' }));
    const nextParentStart = publish.mock.calls
      .map(([, event]) => event)
      .filter(event => event.type === 'workflow.start')
      .at(-1)!;
    expect(nextParentStart.data.runId).toBe(starts[0]!.data.runId);
    expect(nextParentStart.data.executionGeneration).not.toBe(starts[0]!.data.executionGeneration);
    await mastra.shutdown();
  });

  it('promotes a retained nested loop from transient ownership into durable recovery', async () => {
    const { mastra, workflows, processor } = await setup();
    const parentRunId = 'loop-parent-run';
    const childRunId = 'loop-child-run';
    const parentGraph = [
      {
        type: 'loop' as const,
        step: { id: 'child', component: 'WORKFLOW' },
        serializedCondition: { id: 'child-condition', fn: '() => false' },
        loopType: 'dountil' as const,
      },
    ];
    await workflows.persistWorkflowSnapshot({
      workflowName: 'parent',
      runId: parentRunId,
      snapshot: {
        ...createEmptyWorkflowSnapshot(parentRunId),
        status: 'running',
        serializedStepGraph: parentGraph,
      },
    });
    const shouldPersistSnapshot = vi.fn(() => false);
    const childWorkflow = {
      ...workflow('child'),
      options: { shouldPersistSnapshot },
    } as ProcessorArgs['workflow'];
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
      input: { status: 'success' as const, output: {} },
    } as ProcessorArgs['parentWorkflow'];
    const publish = vi.spyOn(mastra.pubsub, 'publish');

    const args: ProcessorArgs = {
      workflow: childWorkflow,
      workflowId: 'child',
      runId: childRunId,
      executionPath: [0],
      stepResults: {},
      activeStepsPath: {},
      resumeSteps: [],
      prevResult: { status: 'success', output: {} },
      requestContext: {},
      parentWorkflow,
    };
    await expect(processor.start(args)).resolves.toBeUndefined();
    expect(shouldPersistSnapshot).toHaveBeenCalledTimes(1);

    await workflows.persistWorkflowSnapshot({
      workflowName: 'child',
      runId: childRunId,
      snapshot: { ...createEmptyWorkflowSnapshot(childRunId), status: 'suspended' },
    });
    publish.mockClear();
    await expect(
      processor.start({
        ...args,
        resumeSteps: ['inner'],
        parentWorkflow: { ...parentWorkflow, resume: true, resumeSteps: ['child', 'inner'] },
      }),
    ).resolves.toBeUndefined();
    expect(shouldPersistSnapshot).toHaveBeenCalledTimes(2);
    expect(shouldPersistSnapshot).toHaveBeenLastCalledWith({
      stepResults: {},
      workflowStatus: 'running',
    });
    expect(publish.mock.calls.some(([, event]) => event.type === 'workflow.step.run')).toBe(true);
    await expect(
      workflows.getWorkflowTerminalRecoveryAncestry({ workflowName: 'child', runId: childRunId }),
    ).resolves.toMatchObject({ status: 'found' });
    await mastra.shutdown();
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

  it('fails closed before dispatch when a recovery-capable store has no durable parent snapshot', async () => {
    const { mastra, workflows, processor } = await setup();
    const transientWorkflow = workflow('child');
    transientWorkflow.options.shouldPersistSnapshot = () => false;
    const persist = vi.spyOn(workflows, 'persistWorkflowSnapshot');
    const publish = vi.spyOn(mastra.pubsub, 'publish');

    await expect(
      processor.start({
        workflow: transientWorkflow,
        workflowId: 'child',
        runId: 'missing-parent-child-run',
        executionPath: [0],
        stepResults: {},
        activeStepsPath: {},
        resumeSteps: [],
        prevResult: { status: 'success', output: {} },
        requestContext: {},
        parentWorkflow: {
          workflowId: 'parent',
          runId: 'missing-parent-run',
          stepId: 'child',
          executionPath: [0],
          resume: false,
          stepResults: {},
          stepGraph: [{ type: 'step', step: { id: 'child', component: 'WORKFLOW' } }],
          activeStepsPath: {},
          resumeSteps: [],
          resumeData: undefined,
          input: { status: 'success', output: {} },
        },
      }),
    ).rejects.toMatchObject({ id: 'MASTRA_WORKFLOW_TERMINAL_RECOVERY_PARENT_MISSING' });
    expect(persist).not.toHaveBeenCalled();
    expect(publish).not.toHaveBeenCalled();
    await mastra.shutdown();
  });

  it.each(['terminal-snapshot', 'terminal-marker', 'terminal-tombstone'] as const)(
    'does not dispatch a nonpersisting child with no child evidence under a parent %s',
    async terminalEvidence => {
      const { mastra, workflows, processor } = await setup();
      const parentRunId = `transient-terminal-parent-${terminalEvidence}`;
      const parentGraph = [{ type: 'step' as const, step: { id: 'child', component: 'WORKFLOW' } }];
      await workflows.persistWorkflowSnapshot({
        workflowName: 'parent',
        runId: parentRunId,
        snapshot: {
          ...createEmptyWorkflowSnapshot(parentRunId),
          status: terminalEvidence === 'terminal-snapshot' ? 'success' : 'running',
          serializedStepGraph: parentGraph,
        },
      });
      if (terminalEvidence !== 'terminal-snapshot') {
        await expect(
          workflows.claimWorkflowTerminalization({
            workflowName: 'parent',
            runId: parentRunId,
            eventKey: 'parent-terminal-event',
            terminalStatus: 'failed',
            ownerId: 'parent-terminal-owner',
            leaseMs: 10_000,
          }),
        ).resolves.toMatchObject({ status: 'acquired' });
        if (terminalEvidence === 'terminal-tombstone') {
          await workflows.deleteWorkflowRunById({ workflowName: 'parent', runId: parentRunId });
        }
      }
      const transientWorkflow = workflow('child');
      transientWorkflow.options.shouldPersistSnapshot = () => false;
      const persist = vi.spyOn(workflows, 'persistWorkflowSnapshot');
      const publish = vi.spyOn(mastra.pubsub, 'publish');
      persist.mockClear();

      await expect(
        processor.start({
          workflow: transientWorkflow,
          workflowId: 'child',
          runId: `transient-child-${terminalEvidence}`,
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
      await mastra.shutdown();
    },
  );

  it.each(['terminal-snapshot', 'terminal-journal', 'terminal-tombstone'] as const)(
    'does not rewrite or republish an already-admitted child with an existing %s',
    async terminalEvidence => {
      const { mastra, workflows, processor } = await setup();
      const parentRunId = `redelivery-parent-${terminalEvidence}`;
      const childRunId = `redelivery-child-${terminalEvidence}`;
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
          input: { status: 'success' as const, output: {} },
        },
      } satisfies ProcessorArgs;
      const persist = vi.spyOn(workflows, 'persistWorkflowSnapshot');
      const publish = vi.spyOn(mastra.pubsub, 'publish');
      await processor.start(args);

      if (terminalEvidence === 'terminal-snapshot') {
        await workflows.persistWorkflowSnapshot({
          workflowName: 'child',
          runId: childRunId,
          snapshot: { ...createEmptyWorkflowSnapshot(childRunId), status: 'success' },
        });
      } else {
        await expect(
          workflows.claimWorkflowTerminalization({
            workflowName: 'child',
            runId: childRunId,
            eventKey: 'terminal-event',
            terminalStatus: 'failed',
            ownerId: 'terminal-owner',
            leaseMs: 10_000,
          }),
        ).resolves.toMatchObject({ status: 'acquired' });
        if (terminalEvidence === 'terminal-tombstone') {
          await workflows.deleteWorkflowRunById({ workflowName: 'child', runId: childRunId });
        }
      }
      persist.mockClear();
      publish.mockClear();

      const replayWorkflow = workflow('child');
      if (terminalEvidence === 'terminal-tombstone') {
        replayWorkflow.options.shouldPersistSnapshot = () => false;
      }
      await expect(processor.start({ ...args, workflow: replayWorkflow })).resolves.toBeUndefined();

      expect(persist).not.toHaveBeenCalled();
      expect(publish).not.toHaveBeenCalled();
      const retained = expect(workflows.loadWorkflowSnapshot({ workflowName: 'child', runId: childRunId })).resolves;
      if (terminalEvidence === 'terminal-tombstone') {
        await retained.toBeNull();
      } else {
        await retained.toMatchObject({ status: terminalEvidence === 'terminal-snapshot' ? 'success' : 'running' });
      }
      await mastra.shutdown();
    },
  );

  it('fails closed when only retained ancestry remains for a nonpersisting child replay', async () => {
    const { mastra, workflows, processor } = await setup();
    const parentRunId = 'ancestry-only-parent-run';
    const childRunId = 'ancestry-only-child-run';
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
        input: { status: 'success' as const, output: {} },
      },
    } satisfies ProcessorArgs;
    await processor.start(args);
    await workflows.deleteWorkflowRunById({ workflowName: 'child', runId: childRunId });
    const replayWorkflow = workflow('child');
    replayWorkflow.options.shouldPersistSnapshot = () => false;
    const persist = vi.spyOn(workflows, 'persistWorkflowSnapshot');
    const publish = vi.spyOn(mastra.pubsub, 'publish');
    persist.mockClear();
    publish.mockClear();

    await expect(processor.start({ ...args, workflow: replayWorkflow })).rejects.toMatchObject({
      id: 'MASTRA_WORKFLOW_NESTED_RUN_RETAINED_SNAPSHOT_MISSING',
    });
    expect(persist).not.toHaveBeenCalled();
    expect(publish).not.toHaveBeenCalled();
    await mastra.shutdown();
  });

  it.each(['running', 'suspended'] as const)(
    'redispatches an already-admitted %s child without replacing retained progress',
    async childStatus => {
      const { mastra, workflows, processor } = await setup();
      const parentRunId = `progress-parent-${childStatus}`;
      const childRunId = `progress-child-${childStatus}`;
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
          input: { status: 'success' as const, output: {} },
        },
      } satisfies ProcessorArgs;

      await processor.start(args);
      const admitted = await workflows.loadWorkflowSnapshot({ workflowName: 'child', runId: childRunId });
      if (!admitted) throw new Error('Expected admitted child snapshot');
      const retained: WorkflowRunState = {
        ...admitted,
        status: childStatus,
        timestamp: admitted.timestamp + 1,
        context: {
          ...admitted.context,
          completed: { status: 'success', output: { preserved: true } },
          __state: { checkpoint: childStatus },
        } as WorkflowRunState['context'],
        value: { checkpoint: childStatus },
        ...(childStatus === 'suspended'
          ? { suspendedPaths: { awaiting: [0] }, resumeLabels: { resume: { stepId: 'awaiting' } } }
          : {}),
      };
      await workflows.persistWorkflowSnapshot({ workflowName: 'child', runId: childRunId, snapshot: retained });
      const persist = vi.spyOn(workflows, 'persistWorkflowSnapshot');
      const publish = vi.spyOn(mastra.pubsub, 'publish');
      persist.mockClear();
      publish.mockClear();

      await expect(processor.start(args)).resolves.toBeUndefined();

      expect(persist).not.toHaveBeenCalled();
      await expect(workflows.loadWorkflowSnapshot({ workflowName: 'child', runId: childRunId })).resolves.toEqual(
        retained,
      );
      expect(workflowControlCalls(publish)).toEqual([
        [`workflow.events.v2.${childRunId}`, 'watch'],
        ['workflows', 'workflow.step.run'],
      ]);
      await mastra.shutdown();
    },
  );

  it('rejects retained child graph drift on a nonpersisting durable replay before publication', async () => {
    const { mastra, workflows, processor } = await setup();
    const parentRunId = 'graph-drift-parent-run';
    const childRunId = 'graph-drift-child-run';
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
        input: { status: 'success' as const, output: {} },
      },
    } satisfies ProcessorArgs;
    await processor.start(args);

    const driftedWorkflow = workflow('child', [{ type: 'step', step: { id: 'drifted', component: 'WORKFLOW' } }]);
    driftedWorkflow.options.shouldPersistSnapshot = () => false;
    const persist = vi.spyOn(workflows, 'persistWorkflowSnapshot');
    const publish = vi.spyOn(mastra.pubsub, 'publish');
    persist.mockClear();
    publish.mockClear();

    await expect(processor.start({ ...args, workflow: driftedWorkflow })).rejects.toMatchObject({
      id: 'MASTRA_WORKFLOW_TERMINAL_RECOVERY_GRAPH_CONFLICT',
    });
    expect(persist).not.toHaveBeenCalled();
    expect(publish).not.toHaveBeenCalled();
    await mastra.shutdown();
  });

  it('reports malformed durable parent state before nested start publication', async () => {
    const { mastra, workflows, processor } = await setup();
    const parentRunId = 'malformed-parent-run';
    const childRunId = 'malformed-parent-child-run';
    const parentGraph = [{ type: 'step' as const, step: { id: 'child', component: 'WORKFLOW' } }];
    await workflows.persistWorkflowSnapshot({
      workflowName: 'parent',
      runId: parentRunId,
      snapshot: {
        runId: parentRunId,
        status: 'running',
        timestamp: Date.now(),
        context: {},
        serializedStepGraph: parentGraph,
      } as unknown as WorkflowRunState,
    });
    const publish = vi.spyOn(mastra.pubsub, 'publish');

    await expect(
      processor.start({
        workflow: workflow('child'),
        workflowId: 'child',
        runId: childRunId,
        executionPath: [0],
        stepResults: {},
        activeStepsPath: {},
        resumeSteps: [],
        prevResult: { status: 'success' as const, output: {} },
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
          input: { status: 'success' as const, output: {} },
        },
      } satisfies ProcessorArgs),
    ).rejects.toMatchObject({ id: 'MASTRA_WORKFLOW_NESTED_RUN_PARENT_SNAPSHOT_CONFLICT' });
    expect(publish).not.toHaveBeenCalled();
    await expect(workflows.loadWorkflowSnapshot({ workflowName: 'child', runId: childRunId })).resolves.toBeNull();
    await expect(
      workflows.getWorkflowTerminalRecoveryAncestry({ workflowName: 'child', runId: childRunId }),
    ).resolves.toEqual({ status: 'missing_ancestry' });
    await mastra.shutdown();
  });

  it('does not overwrite progress injected after atomic admission and before start publication completes', async () => {
    const { mastra, workflows, processor } = await setup();
    const parentRunId = 'barrier-parent-run';
    const childRunId = 'barrier-child-run';
    const childWorkflow = workflow('child');
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
    const progress: WorkflowRunState = {
      ...createEmptyWorkflowSnapshot(childRunId),
      status: 'running',
      serializedStepGraph: childWorkflow.serializedStepGraph,
      context: {
        completed: { status: 'success', output: { durable: true } },
        __state: { checkpoint: 'after-admission' },
      } as WorkflowRunState['context'],
      value: { checkpoint: 'after-admission' },
    };
    const persist = vi.spyOn(workflows, 'persistWorkflowSnapshot');
    let injected = false;
    vi.spyOn(mastra.pubsub, 'publish').mockImplementation(async (channel, event) => {
      if (!injected && channel === `workflow.events.v2.${childRunId}`) {
        injected = true;
        await workflows.persistWorkflowSnapshot({ workflowName: 'child', runId: childRunId, snapshot: progress });
      }
      void event;
    });

    await processor.start({
      workflow: childWorkflow,
      workflowId: 'child',
      runId: childRunId,
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
    });

    expect(injected).toBe(true);
    expect(persist).toHaveBeenCalledTimes(1);
    await expect(workflows.loadWorkflowSnapshot({ workflowName: 'child', runId: childRunId })).resolves.toEqual(
      progress,
    );
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
    expect(workflowControlCalls(publish)).toEqual([
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
    expect(workflowControlCalls(publish)).toEqual([
      ['workflow.events.v2.accepted-child-run', 'watch'],
      ['workflows', 'workflow.step.run'],
    ]);
    await mastra.shutdown();
  });

  it('keeps a brand-new nonpersistent nested run outside durable recovery admission', async () => {
    const { mastra, workflows, processor } = await setup();
    const parentRunId = 'transient-parent-run';
    const childRunId = 'transient-child-run';
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
    const admit = vi.spyOn(workflows, 'admitWorkflowNestedRun');
    const persist = vi.spyOn(workflows, 'persistWorkflowSnapshot');
    const publish = vi.spyOn(mastra.pubsub, 'publish');

    await processor.start({
      workflow: {
        ...workflow('child'),
        options: { shouldPersistSnapshot: () => false },
      } as ProcessorArgs['workflow'],
      workflowId: 'child',
      runId: childRunId,
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
    });

    expect(admit).not.toHaveBeenCalled();
    expect(persist).not.toHaveBeenCalled();
    await expect(workflows.loadWorkflowSnapshot({ workflowName: 'child', runId: childRunId })).resolves.toBeNull();
    await expect(
      workflows.getWorkflowTerminalRecoveryAncestry({ workflowName: 'child', runId: childRunId }),
    ).resolves.toEqual({ status: 'missing_ancestry' });
    const retainedParent = await workflows.loadWorkflowSnapshot({ workflowName: 'parent', runId: parentRunId });
    expect(retainedParent?.context.child).toBeUndefined();
    expect(workflowControlCalls(publish)).toEqual([
      ['workflow.events.v2.transient-child-run', 'watch'],
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
