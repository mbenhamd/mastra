import { describe, expect, it, vi } from 'vitest';
import { z } from 'zod/v4';
import { Mastra } from '../../mastra';
import type { SerializedStepFlowEntry, WorkflowRunState, WorkflowTerminalEffectRecord } from '../types';
import { createStep, createWorkflow } from '../workflow';
import { getWorkflowTerminalEffectIntegrity } from './effect-integrity';
import { MAX_TERMINAL_LOOP_ITERATIONS } from './graph-fingerprint';
import {
  completeWorkflowTerminalLoopDecision,
  createWorkflowTerminalLoopDecisionRequest,
  planWorkflowTerminalParentContinuation,
} from './planner';

const RECOVERY_ENVELOPE_HASH = `sha256:${'a'.repeat(64)}` as const;

function effect(
  parentExecutionPath: number[] = [0],
  parentStepId = 'nested',
  terminalStatus: 'success' | 'failed' | 'canceled' = 'success',
  runId = 'child-run',
): Extract<WorkflowTerminalEffectRecord, { kind: 'parent-workflow-step-end' }> {
  const identity = {
    version: 1,
    kind: 'parent-workflow-step-end',
    workflowName: 'child',
    runId,
    sourceEventKey: 'event',
    terminalStatus,
    recoveryEnvelopeHash: RECOVERY_ENVELOPE_HASH,
    parentWorkflowName: 'parent',
    parentRunId: 'parent-run',
    parentStepId,
    parentExecutionPath,
  } as const;
  return { ...identity, ...getWorkflowTerminalEffectIntegrity(identity), createdAt: 1 };
}

function snapshot(
  graph: SerializedStepFlowEntry[] = [
    { type: 'step', step: { id: 'nested', component: 'WORKFLOW' } },
    { type: 'sleep', id: 'after', duration: 10 },
  ],
  sourceStepId = 'nested',
  sourcePath: number[] = [0],
): WorkflowRunState {
  return {
    runId: 'parent-run',
    status: 'running',
    context: {
      [sourceStepId]: {
        status: 'running',
        payload: {},
        startedAt: 1,
        metadata: { nestedRunId: 'child-run' },
      },
      __state: { secret: 'must-not-be-planned' },
    } as WorkflowRunState['context'],
    activePaths: sourcePath,
    activeStepsPath: { [sourceStepId]: sourcePath },
    serializedStepGraph: graph,
    suspendedPaths: {},
    resumeLabels: {},
    value: { secret: true },
    waitingPaths: {},
    timestamp: 1,
    requestContext: { token: 'must-not-be-planned' },
  };
}

function input(parentSnapshot = snapshot(), terminalEffect = effect(), parentRevision = 'revision-1') {
  return { version: 1 as const, effect: terminalEffect, parentRevision, parentSnapshot };
}

describe('workflow terminal parent continuation planner', () => {
  it('plans persisted conditional and loop graphs emitted by the native Workflow builder', () => {
    const schema = z.object({ value: z.number() });
    const nestedWorkflow = (id: string) =>
      createWorkflow({ id, inputSchema: schema, outputSchema: schema })
        .then(
          createStep({
            id: `${id}-leaf`,
            inputSchema: schema,
            outputSchema: schema,
            execute: async ({ inputData }) => inputData,
          }),
        )
        .commit();
    const persistedGraph = (graph: readonly SerializedStepFlowEntry[]) =>
      JSON.parse(JSON.stringify(graph)) as SerializedStepFlowEntry[];

    const branchChild = nestedWorkflow('branch-child');
    const branchParent = createWorkflow({ id: 'native-branch-parent', inputSchema: schema, outputSchema: schema })
      .branch([[async () => true, branchChild]])
      .commit();
    const loopChild = nestedWorkflow('loop-child');
    const loopParent = createWorkflow({ id: 'native-loop-parent', inputSchema: schema, outputSchema: schema })
      .dowhile(loopChild, async () => false)
      .commit();
    const mastra = new Mastra({
      logger: false,
      workflows: {
        'native-branch-parent': branchParent,
        'native-loop-parent': loopParent,
      },
    });
    expect(
      planWorkflowTerminalParentContinuation(
        input(
          snapshot(
            persistedGraph(mastra.getWorkflow('native-branch-parent').serializedStepGraph),
            branchChild.id,
            [0, 0],
          ),
          effect([0, 0], branchChild.id),
        ),
      ),
    ).toMatchObject({ action: { kind: 'complete-entry', reason: 'conditional-continue' } });

    const loopInput = input(
      snapshot(persistedGraph(mastra.getWorkflow('native-loop-parent').serializedStepGraph), loopChild.id),
      effect([0], loopChild.id),
    );
    const loopDecision = completeWorkflowTerminalLoopDecision(
      createWorkflowTerminalLoopDecisionRequest(loopInput),
      false,
    );
    expect(planWorkflowTerminalParentContinuation({ ...loopInput, evaluatedDecision: loopDecision })).toMatchObject({
      action: { kind: 'complete-entry', reason: 'loop-exit' },
    });
  });

  it.each([
    [
      'step',
      { type: 'step', step: { id: 'next-step' } } as SerializedStepFlowEntry,
      { kind: 'step', stepId: 'next-step', executionPath: [1] },
    ],
    [
      'sleep',
      { type: 'sleep', id: 'next-sleep', duration: 10 } as SerializedStepFlowEntry,
      { kind: 'entry', entryType: 'sleep', entryId: 'next-sleep', executionPath: [1] },
    ],
    [
      'sleepUntil',
      { type: 'sleepUntil', id: 'next-date', date: '2026-01-01T00:00:00.000Z' } as SerializedStepFlowEntry,
      { kind: 'entry', entryType: 'sleepUntil', entryId: 'next-date', executionPath: [1] },
    ],
    [
      'parallel',
      { type: 'parallel', steps: [{ type: 'step', step: { id: 'parallel-child' } }] } as SerializedStepFlowEntry,
      { kind: 'container', containerType: 'parallel', executionPath: [1] },
    ],
    [
      'conditional',
      {
        type: 'conditional',
        steps: [{ type: 'step', step: { id: 'conditional-child' } }],
        serializedConditions: [{ id: 'conditional-child-condition', fn: '() => true' }],
      } as SerializedStepFlowEntry,
      { kind: 'container', containerType: 'conditional', executionPath: [1] },
    ],
    [
      'loop',
      {
        type: 'loop',
        step: { id: 'loop-child' },
        loopType: 'dowhile',
        serializedCondition: { id: 'loop-child-condition', fn: '() => false' },
      } as SerializedStepFlowEntry,
      { kind: 'container', containerType: 'loop', executionPath: [1] },
    ],
    [
      'foreach',
      { type: 'foreach', step: { id: 'foreach-child' }, opts: { concurrency: 2 } } as SerializedStepFlowEntry,
      { kind: 'container', containerType: 'foreach', executionPath: [1] },
    ],
  ])('selects the immediate %s successor without carrying runtime payloads', (_label, targetEntry, target) => {
    const parent = snapshot([{ type: 'step', step: { id: 'nested', component: 'WORKFLOW' } }, targetEntry]);
    const contract = planWorkflowTerminalParentContinuation(input(parent));
    expect(contract).toMatchObject({
      action: { kind: 'run-entry', reason: 'next-step', target },
      patch: { kind: 'merge-child-terminal', parentRunWrite: { kind: 'preserve' } },
    });
    const serialized = JSON.stringify(contract);
    expect(serialized).not.toContain('must-not-be-planned');
    expect(serialized).not.toContain('"requestContext":');
    expect(serialized).not.toContain('"result":');
  });

  it('finishes only a final sequential source and maps failure/cancellation explicitly', () => {
    const parent = snapshot([{ type: 'step', step: { id: 'nested', component: 'WORKFLOW' } }]);
    expect(planWorkflowTerminalParentContinuation(input(parent)).action).toEqual({
      kind: 'finish-parent',
      reason: 'parent-end',
    });
    expect(planWorkflowTerminalParentContinuation(input(parent, effect([0], 'nested', 'failed')))).toMatchObject({
      action: { kind: 'fail-parent', reason: 'parent-fail' },
      patch: { parentRunWrite: { kind: 'set', status: 'failed' } },
    });
    expect(planWorkflowTerminalParentContinuation(input(parent, effect([0], 'nested', 'canceled')))).toMatchObject({
      action: { kind: 'cancel-parent', reason: 'child-canceled' },
      patch: { parentRunWrite: { kind: 'set', status: 'canceled' } },
    });
  });

  it.each(['success', 'failed', 'canceled', 'tripwire', 'bailed'] as const)(
    'returns a no-op for an already-%s parent',
    status => {
      const parent = snapshot([{ type: 'step', step: { id: 'nested', component: 'WORKFLOW' } }]);
      parent.status = status;
      expect(planWorkflowTerminalParentContinuation(input(parent)).action).toEqual({
        kind: 'noop',
        reason: 'already-terminal',
      });
    },
  );

  it.each([
    ['parallel', 'running', 'wait', 'parallel-aggregation'],
    ['parallel', 'success', 'complete-entry', 'parallel-continue'],
    ['parallel', 'suspended', 'suspend-parent', 'branch-suspended'],
    ['conditional', 'running', 'wait', 'conditional-aggregation'],
    ['conditional', 'skipped', 'complete-entry', 'conditional-continue'],
    ['conditional', 'suspended', 'suspend-parent', 'branch-suspended'],
  ] as const)('plans %s sibling %s as %s/%s', (containerType, siblingStatus, kind, reason) => {
    const graph =
      containerType === 'parallel'
        ? ([
            {
              type: 'parallel',
              steps: [
                { type: 'step', step: { id: 'left', component: 'WORKFLOW' } },
                { type: 'step', step: { id: 'right' } },
              ],
            },
          ] as SerializedStepFlowEntry[])
        : ([
            {
              type: 'conditional',
              steps: [
                { type: 'step', step: { id: 'left', component: 'WORKFLOW' } },
                { type: 'step', step: { id: 'right' } },
              ],
              serializedConditions: [
                { id: 'left-condition', fn: '() => true' },
                { id: 'right-condition', fn: '() => false' },
              ],
            },
          ] as SerializedStepFlowEntry[]);
    const parent = snapshot(graph, 'left', [0, 0]);
    parent.context.right = { status: siblingStatus } as WorkflowRunState['context'][string];
    if (siblingStatus === 'running') parent.activeStepsPath.right = [0, 1];
    const contract = planWorkflowTerminalParentContinuation(input(parent, effect([0, 0], 'left')));
    expect(contract.action).toMatchObject({ kind, reason });
    expect(contract.patch).toMatchObject({
      kind: 'merge-child-terminal',
      parentRunWrite: kind === 'suspend-parent' ? { kind: 'set-suspended' } : { kind: 'preserve' },
    });
  });

  it('quarantines contradictory branch and source ownership state with a structural-only stable digest', () => {
    const parent = snapshot(
      [
        {
          type: 'parallel',
          steps: [
            { type: 'step', step: { id: 'left', component: 'WORKFLOW' } },
            { type: 'step', step: { id: 'right' } },
          ],
        },
      ],
      'left',
      [0, 0],
    );
    parent.context.right = { status: 'failed', error: { secret: 'one' } } as any;
    const first = planWorkflowTerminalParentContinuation(input(parent, effect([0, 0], 'left')));
    parent.context.right = { status: 'failed', error: { secret: 'two' } } as any;
    const second = planWorkflowTerminalParentContinuation(input(parent, effect([0, 0], 'left')));
    expect(first).toMatchObject({ action: { kind: 'quarantine', reason: 'plan-conflict' }, patch: { kind: 'none' } });
    expect(second.action).toEqual(first.action);

    const wrongOwner = snapshot();
    (wrongOwner.context.nested as any).metadata.nestedRunId = 'different-child';
    expect(planWorkflowTerminalParentContinuation(input(wrongOwner))).toMatchObject({
      action: { kind: 'quarantine', reason: 'plan-conflict' },
    });
  });

  it.each([
    ['dowhile', true, 'loop-continue'],
    ['dowhile', false, 'loop-exit'],
    ['dountil', true, 'loop-exit'],
    ['dountil', false, 'loop-continue'],
  ] as const)('binds %s=%s to %s', (loopType, conditionResult, reason) => {
    const graph: SerializedStepFlowEntry[] = [
      {
        type: 'loop',
        step: { id: 'nested', component: 'WORKFLOW' },
        loopType,
        serializedCondition: { id: 'nested-condition', fn: '() => true' },
      },
    ];
    const parent = snapshot(graph);
    (parent.context.nested as any).metadata.iterationCount = 3;
    const plannerInput = input(parent);
    const request = createWorkflowTerminalLoopDecisionRequest(plannerInput);
    expect(request).toMatchObject({ loopType, previousIterationCount: 3 });
    const evaluatedDecision = completeWorkflowTerminalLoopDecision(request, conditionResult);
    const contract = planWorkflowTerminalParentContinuation({ ...plannerInput, evaluatedDecision });
    expect(contract).toMatchObject({
      action: {
        reason,
        loopDecision: { loopType, conditionResult, previousIterationCount: 3, nextIterationCount: 4 },
      },
      patch: { loopWrite: { kind: 'set-iteration', stepId: 'nested', iterationCount: 4 } },
    });
  });

  it('rejects missing, surplus, or stale evaluated loop decisions', () => {
    const graph: SerializedStepFlowEntry[] = [
      {
        type: 'loop',
        step: { id: 'nested', component: 'WORKFLOW' },
        loopType: 'dowhile',
        serializedCondition: { id: 'nested-condition', fn: '() => true' },
      },
    ];
    const parent = snapshot(graph);
    const plannerInput = input(parent);
    expect(() => planWorkflowTerminalParentContinuation(plannerInput)).toThrow(/requires an evaluated decision/);
    const decision = completeWorkflowTerminalLoopDecision(
      createWorkflowTerminalLoopDecisionRequest(plannerInput),
      true,
    );
    expect(() =>
      planWorkflowTerminalParentContinuation({
        ...plannerInput,
        parentRevision: 'revision-2',
        evaluatedDecision: decision,
      }),
    ).toThrow(/stale or unbound/);
    expect(() => planWorkflowTerminalParentContinuation({ ...input(), evaluatedDecision: decision })).toThrow(
      /Non-loop planning rejects/,
    );

    const wrongOwner = snapshot(graph);
    (wrongOwner.context.nested as any).metadata.nestedRunId = 'stale-child';
    expect(() => createWorkflowTerminalLoopDecisionRequest(input(wrongOwner))).toThrow(/inactive or unowned/);
    expect(() => createWorkflowTerminalLoopDecisionRequest(input(parent, effect([0], 'nested', 'failed')))).toThrow(
      /inactive or unowned/,
    );
  });

  it.each(['unrelated-active-step', 'source-at-wrong-coordinate'] as const)(
    'rejects loop callback and continuation planning with %s',
    contradiction => {
      const graph: SerializedStepFlowEntry[] = [
        {
          type: 'loop',
          step: { id: 'nested', component: 'WORKFLOW' },
          loopType: 'dowhile',
          serializedCondition: { id: 'nested-condition', fn: '() => true' },
        },
      ];
      const validParent = snapshot(graph);
      const request = createWorkflowTerminalLoopDecisionRequest(input(validParent));

      for (const conditionResult of [true, false]) {
        const parent = structuredClone(validParent);
        if (contradiction === 'unrelated-active-step') parent.activeStepsPath.unrelated = [1];
        else parent.activeStepsPath.nested = [1];

        expect(() => createWorkflowTerminalLoopDecisionRequest(input(parent))).toThrow(/inactive or unowned/);
        const evaluatedDecision = completeWorkflowTerminalLoopDecision(request, conditionResult);
        if (contradiction === 'unrelated-active-step') {
          expect(planWorkflowTerminalParentContinuation({ ...input(parent), evaluatedDecision })).toMatchObject({
            action: { kind: 'quarantine', reason: 'plan-conflict' },
          });
        } else {
          expect(() => planWorkflowTerminalParentContinuation({ ...input(parent), evaluatedDecision })).toThrow(
            /Plan-conflict planning rejects evaluated decisions/,
          );
          expect(planWorkflowTerminalParentContinuation(input(parent))).toMatchObject({
            action: { kind: 'quarantine', reason: 'plan-conflict' },
          });
        }
      }
    },
  );

  it('allows a boundary loop exit but rejects another iteration past the configured count', () => {
    const graph: SerializedStepFlowEntry[] = [
      {
        type: 'loop',
        step: { id: 'nested', component: 'WORKFLOW' },
        loopType: 'dowhile',
        serializedCondition: { id: 'nested-condition', fn: '() => true' },
      },
    ];
    const parent = snapshot(graph);
    (parent.context.nested as any).metadata.iterationCount = MAX_TERMINAL_LOOP_ITERATIONS;
    const plannerInput = input(parent);
    const request = createWorkflowTerminalLoopDecisionRequest(plannerInput);
    expect(
      planWorkflowTerminalParentContinuation({
        ...plannerInput,
        evaluatedDecision: completeWorkflowTerminalLoopDecision(request, false),
      }),
    ).toMatchObject({
      action: {
        kind: 'complete-entry',
        loopDecision: { nextIterationCount: MAX_TERMINAL_LOOP_ITERATIONS + 1 },
      },
    });
    expect(() =>
      planWorkflowTerminalParentContinuation({
        ...plannerInput,
        evaluatedDecision: completeWorkflowTerminalLoopDecision(request, true),
      }),
    ).toThrow(/iteration count is exhausted/);
  });

  function foreachParent(
    payload: unknown[],
    output: unknown[],
    sourceIndex: number,
    iterationRuns: Record<string, string>,
    iterationStates: Record<string, string> = {},
  ) {
    const parent = snapshot(
      [{ type: 'foreach', step: { id: 'each', component: 'WORKFLOW' }, opts: { concurrency: 2 } }],
      'each',
      [0, sourceIndex],
    );
    parent.context.each = {
      status: 'running',
      payload,
      output,
      metadata: { __workflow_meta: { iterationRunIds: iterationRuns, terminalIterationStates: iterationStates } },
    } as WorkflowRunState['context'][string];
    return parent;
  }

  it.each([
    ['pending', ['a', 'b'], [null, null], 0, { '0': 'child-run', '1': 'other' }, {}, 'wait', 'foreach-aggregation'],
    ['continue', ['a', 'b'], [null], 0, { '0': 'child-run' }, {}, 'run-entry', 'foreach-continue'],
    [
      'complete-null',
      ['a', 'b'],
      [null, null],
      1,
      { '1': 'child-run' },
      { '0': 'success' },
      'complete-entry',
      'foreach-complete',
    ],
    [
      'suspended',
      ['a', 'b'],
      [{ status: 'suspended' }, null],
      1,
      { '0': 'suspended-run', '1': 'child-run' },
      {},
      'suspend-parent',
      'foreach-suspended',
    ],
  ] as const)(
    'plans foreach %s as %s/%s',
    (_label, payload, output, sourceIndex, iterationRuns, states, kind, reason) => {
      const parent = foreachParent([...payload], [...output], sourceIndex, iterationRuns, states);
      const contract = planWorkflowTerminalParentContinuation(input(parent, effect([0, sourceIndex], 'each')));
      expect(contract.action).toMatchObject({ kind, reason });
    },
  );

  it('quarantines missing foreach ownership and failed sibling state', () => {
    const missingOwner = foreachParent(['a'], [null], 0, {});
    expect(planWorkflowTerminalParentContinuation(input(missingOwner, effect([0, 0], 'each')))).toMatchObject({
      action: { kind: 'quarantine', reason: 'plan-conflict' },
    });
    const failedSibling = foreachParent(['a', 'b'], [false, null], 1, { '1': 'child-run' }, { '0': 'failed' });
    expect(planWorkflowTerminalParentContinuation(input(failedSibling, effect([0, 1], 'each')))).toMatchObject({
      action: { kind: 'quarantine', reason: 'plan-conflict' },
    });
  });

  it('quarantines every successful foreach action with unrelated or wrong-coordinate active work', () => {
    const cases = [
      foreachParent(['a', 'b'], [null, null], 0, { '0': 'child-run', '1': 'other' }),
      foreachParent(['a', 'b'], [null], 0, { '0': 'child-run' }),
      foreachParent(['a', 'b'], [null, null], 1, { '1': 'child-run' }, { '0': 'success' }),
      foreachParent(['a', 'b'], [{ status: 'suspended' }, null], 1, { '0': 'suspended-run', '1': 'child-run' }),
    ];

    for (const validParent of cases) {
      const sourceIndex = validParent.activeStepsPath.each![1]!;
      for (const contradiction of ['unrelated-active-step', 'source-at-wrong-coordinate'] as const) {
        const parent = structuredClone(validParent);
        if (contradiction === 'unrelated-active-step') parent.activeStepsPath.unrelated = [1];
        else parent.activeStepsPath.each = [1];
        expect(planWorkflowTerminalParentContinuation(input(parent, effect([0, sourceIndex], 'each')))).toMatchObject({
          action: { kind: 'quarantine', reason: 'plan-conflict' },
        });
      }
    }
  });

  it('quarantines foreach active coordinates for unstarted and terminal iterations', () => {
    const unstarted = foreachParent(['a', 'b'], [null], 0, { '0': 'child-run' });
    unstarted.activeStepsPath.each = [0, 1];
    expect(planWorkflowTerminalParentContinuation(input(unstarted, effect([0, 0], 'each')))).toMatchObject({
      action: { kind: 'quarantine', reason: 'plan-conflict' },
    });

    const terminal = foreachParent(['a', 'b'], [null, null], 1, { '1': 'child-run' }, { '0': 'success' });
    terminal.activeStepsPath.each = [0, 0];
    expect(planWorkflowTerminalParentContinuation(input(terminal, effect([0, 1], 'each')))).toMatchObject({
      action: { kind: 'quarantine', reason: 'plan-conflict' },
    });
  });

  it.each([undefined, 'success', 'suspended'] as const)(
    'uses per-index foreach ownership instead of the aggregate %s status',
    status => {
      const parent = foreachParent(['a'], [null], 0, { '0': 'child-run' });
      if (status === undefined) delete parent.context.each!.status;
      else parent.context.each!.status = status;
      expect(planWorkflowTerminalParentContinuation(input(parent, effect([0, 0], 'each')))).toMatchObject({
        action: { kind: 'complete-entry', reason: 'foreach-complete' },
      });
    },
  );

  it.each(['waiting', 'suspended'] as const)(
    'allows durable aggregation waits but quarantines dispatch from a %s parent',
    status => {
      const sequential = snapshot();
      sequential.status = status;
      expect(planWorkflowTerminalParentContinuation(input(sequential))).toMatchObject({
        action: { kind: 'quarantine', reason: 'plan-conflict' },
      });

      const branch = snapshot(
        [
          {
            type: 'parallel',
            steps: [
              { type: 'step', step: { id: 'left', component: 'WORKFLOW' } },
              { type: 'step', step: { id: 'right' } },
            ],
          },
        ],
        'left',
        [0, 0],
      );
      branch.status = status;
      branch.context.right = { status: 'running' } as WorkflowRunState['context'][string];
      branch.activeStepsPath.right = [0, 1];
      expect(planWorkflowTerminalParentContinuation(input(branch, effect([0, 0], 'left')))).toMatchObject({
        action: { kind: 'wait', reason: 'parallel-aggregation' },
      });

      const foreach = foreachParent(['a', 'b'], [null, null], 0, { '0': 'child-run', '1': 'other' });
      foreach.status = status;
      expect(planWorkflowTerminalParentContinuation(input(foreach, effect([0, 0], 'each')))).toMatchObject({
        action: { kind: 'wait', reason: 'foreach-aggregation' },
      });
    },
  );

  it('quarantines successful complete/suspend/continue actions from non-running parents', () => {
    const completeBranch = snapshot(
      [
        {
          type: 'parallel',
          steps: [
            { type: 'step', step: { id: 'left', component: 'WORKFLOW' } },
            { type: 'step', step: { id: 'right' } },
          ],
        },
      ],
      'left',
      [0, 0],
    );
    completeBranch.status = 'waiting';
    completeBranch.context.right = { status: 'success' } as WorkflowRunState['context'][string];
    expect(planWorkflowTerminalParentContinuation(input(completeBranch, effect([0, 0], 'left')))).toMatchObject({
      action: { kind: 'quarantine', reason: 'plan-conflict' },
    });

    const suspendedBranch = structuredClone(completeBranch);
    suspendedBranch.status = 'suspended';
    suspendedBranch.context.right = { status: 'suspended' } as WorkflowRunState['context'][string];
    expect(planWorkflowTerminalParentContinuation(input(suspendedBranch, effect([0, 0], 'left')))).toMatchObject({
      action: { kind: 'quarantine', reason: 'plan-conflict' },
    });

    for (const parent of [
      foreachParent(['a', 'b'], [null], 0, { '0': 'child-run' }),
      foreachParent(['a'], [null], 0, { '0': 'child-run' }),
      foreachParent(['a', 'b'], [{ status: 'suspended' }, null], 1, { '1': 'child-run' }),
    ]) {
      parent.status = 'waiting';
      const sourceIndex = parent.activeStepsPath.each![1]!;
      expect(planWorkflowTerminalParentContinuation(input(parent, effect([0, sourceIndex], 'each')))).toMatchObject({
        action: { kind: 'quarantine', reason: 'plan-conflict' },
      });
    }
  });

  it('rejects loop callbacks outside a running owned parent revision', () => {
    const graph: SerializedStepFlowEntry[] = [
      {
        type: 'loop',
        step: { id: 'nested', component: 'WORKFLOW' },
        loopType: 'dowhile',
        serializedCondition: { id: 'nested-condition', fn: '() => true' },
      },
    ];
    const runningInput = input(snapshot(graph));
    const decision = completeWorkflowTerminalLoopDecision(
      createWorkflowTerminalLoopDecisionRequest(runningInput),
      true,
    );
    for (const status of ['waiting', 'suspended'] as const) {
      const parent = snapshot(graph);
      parent.status = status;
      expect(() => createWorkflowTerminalLoopDecisionRequest(input(parent))).toThrow(/inactive or unowned/);
      expect(planWorkflowTerminalParentContinuation(input(parent))).toMatchObject({
        action: { kind: 'quarantine', reason: 'plan-conflict' },
      });
      expect(() => planWorkflowTerminalParentContinuation({ ...input(parent), evaluatedDecision: decision })).toThrow(
        /rejects evaluated decisions/,
      );
    }
  });

  it.each(['pending', 'paused'] as const)('quarantines an inactive %s parent', status => {
    const parent = snapshot();
    parent.status = status;
    expect(planWorkflowTerminalParentContinuation(input(parent))).toMatchObject({
      action: { kind: 'quarantine', reason: 'plan-conflict' },
    });
  });

  it.each([
    ['branch', 'failed', 'waiting', 'fail-parent'],
    ['branch', 'canceled', 'suspended', 'cancel-parent'],
    ['loop', 'failed', 'suspended', 'fail-parent'],
    ['loop', 'canceled', 'waiting', 'cancel-parent'],
    ['foreach', 'failed', 'waiting', 'fail-parent'],
    ['foreach', 'canceled', 'suspended', 'cancel-parent'],
  ] as const)('propagates %s %s evidence from a %s parent', (topology, terminalStatus, status, actionKind) => {
    let parent: WorkflowRunState;
    let terminalEffect: ReturnType<typeof effect>;
    if (topology === 'branch') {
      parent = snapshot(
        [
          {
            type: 'parallel',
            steps: [
              { type: 'step', step: { id: 'left', component: 'WORKFLOW' } },
              { type: 'step', step: { id: 'right' } },
            ],
          },
        ],
        'left',
        [0, 0],
      );
      terminalEffect = effect([0, 0], 'left', terminalStatus);
    } else if (topology === 'loop') {
      parent = snapshot([
        {
          type: 'loop',
          step: { id: 'nested', component: 'WORKFLOW' },
          loopType: 'dowhile',
          serializedCondition: { id: 'nested-condition', fn: '() => true' },
        },
      ]);
      terminalEffect = effect([0], 'nested', terminalStatus);
    } else {
      parent = foreachParent(['a'], [null], 0, { '0': 'child-run' });
      terminalEffect = effect([0, 0], 'each', terminalStatus);
    }
    if (topology === 'loop' || topology === 'foreach') parent.activeStepsPath.unrelated = [1];
    parent.status = status;
    expect(planWorkflowTerminalParentContinuation(input(parent, terminalEffect))).toMatchObject({
      action: { kind: actionKind },
    });
  });

  it('quarantines duplicate sequential dispatch when another path is already active', () => {
    for (const activeStepId of ['after', 'unrelated']) {
      const parent = snapshot();
      parent.activeStepsPath[activeStepId] = [1];
      expect(planWorkflowTerminalParentContinuation(input(parent))).toMatchObject({
        action: { kind: 'quarantine', reason: 'plan-conflict' },
      });
    }
  });

  it('handles three-way branch aggregation and rejects unknown sibling states', () => {
    const graph: SerializedStepFlowEntry[] = [
      {
        type: 'conditional',
        steps: ['left', 'middle', 'right'].map(id => ({
          type: 'step' as const,
          step: { id, ...(id === 'left' ? { component: 'WORKFLOW' as const } : {}) },
        })),
        serializedConditions: ['left', 'middle', 'right'].map(id => ({ id: `${id}-condition`, fn: '() => true' })),
      },
    ];
    const parent = snapshot(graph, 'left', [0, 0]);
    parent.context.middle = { status: 'skipped' } as WorkflowRunState['context'][string];
    parent.context.right = { status: 'suspended' } as WorkflowRunState['context'][string];
    expect(planWorkflowTerminalParentContinuation(input(parent, effect([0, 0], 'left')))).toMatchObject({
      action: { kind: 'suspend-parent', reason: 'branch-suspended' },
    });

    delete parent.context.middle;
    const missingSibling = planWorkflowTerminalParentContinuation(input(parent, effect([0, 0], 'left')));
    expect(missingSibling).toMatchObject({
      action: {
        kind: 'quarantine',
        reason: 'plan-conflict',
        conflictDigest: expect.stringMatching(/^sha256:[a-f0-9]{64}$/),
      },
      patch: { kind: 'none' },
    });
    expect(planWorkflowTerminalParentContinuation(input(parent, effect([0, 0], 'left'))).action).toEqual(
      missingSibling.action,
    );
    expect(missingSibling.action).not.toHaveProperty('target');

    parent.context.middle = { status: 'unknown' } as any;
    expect(planWorkflowTerminalParentContinuation(input(parent, effect([0, 0], 'left')))).toMatchObject({
      action: { kind: 'quarantine', reason: 'plan-conflict' },
    });

    parent.context.middle = { status: 'running' } as WorkflowRunState['context'][string];
    parent.activeStepsPath.middle = [0, 1];
    expect(planWorkflowTerminalParentContinuation(input(parent, effect([0, 0], 'left')))).toMatchObject({
      action: { kind: 'wait', reason: 'conditional-aggregation' },
    });
  });

  it('quarantines branch state that marks a terminal sibling as still active', () => {
    const parent = snapshot(
      [
        {
          type: 'parallel',
          steps: [
            { type: 'step', step: { id: 'left', component: 'WORKFLOW' } },
            { type: 'step', step: { id: 'right' } },
          ],
        },
      ],
      'left',
      [0, 0],
    );
    parent.context.right = { status: 'success' } as WorkflowRunState['context'][string];
    parent.activeStepsPath.right = [0, 1];
    expect(planWorkflowTerminalParentContinuation(input(parent, effect([0, 0], 'left')))).toMatchObject({
      action: { kind: 'quarantine', reason: 'plan-conflict' },
    });

    parent.context.right = { status: 'suspended' } as WorkflowRunState['context'][string];
    expect(planWorkflowTerminalParentContinuation(input(parent, effect([0, 0], 'left')))).toMatchObject({
      action: { kind: 'suspend-parent', reason: 'branch-suspended' },
    });

    delete parent.activeStepsPath.right;
    parent.context.right = { status: 'running' } as WorkflowRunState['context'][string];
    expect(planWorkflowTerminalParentContinuation(input(parent, effect([0, 0], 'left')))).toMatchObject({
      action: { kind: 'quarantine', reason: 'plan-conflict' },
    });

    parent.context.right = { status: 'paused' } as WorkflowRunState['context'][string];
    expect(planWorkflowTerminalParentContinuation(input(parent, effect([0, 0], 'left')))).toMatchObject({
      action: { kind: 'quarantine', reason: 'plan-conflict' },
    });
  });

  it('fails closed on malformed foreach sidecars, bounds, ownership, and output state', () => {
    const cases: WorkflowRunState[] = [];
    const magicRuns = Object.create(null) as Record<string, string>;
    Object.defineProperty(magicRuns, '__proto__', { enumerable: true, value: 'spoof' });
    Object.defineProperty(magicRuns, '0', { enumerable: true, value: 'child-run' });
    cases.push(foreachParent(['a'], [null], 0, magicRuns));
    cases.push(foreachParent(['a'], [null], 0, { '0': 'child-run', '1': 'out-of-bounds' }));
    cases.push(foreachParent(['a'], [null], 0, { '0': 'child-run' }, { '0': 'mystery' }));
    cases.push(foreachParent(['a'], [null], 0, { '0': 'child-run' }, { '0': 'success' }));
    cases.push(foreachParent(['a'], [null, null], 0, { '0': 'child-run' }));
    cases.push(foreachParent(['a', 'b'], [null, { status: 'running' }], 0, { '0': 'child-run' }));
    cases.push(foreachParent(['a', 'b'], [null, null], 0, { '0': 'child-run' }));
    cases.push(foreachParent(['a', 'b'], [null, { status: 'suspended' }], 0, { '0': 'child-run' }));
    cases.push(foreachParent(['a', 'b'], [null], 0, { '0': 'child-run', '1': 'future-run' }));
    cases.push(foreachParent(['a', 'b'], [null, null], 0, { '0': 'child-run', '1': 'child-run' }));
    cases.push(foreachParent(['a', 'b'], [null, null], 0, { '0': 'child-run', '1': 'invalid\0run' }));
    cases.push(foreachParent(['a', 'b'], [null, null], 0, { '0': 'child-run', '1': '\ud800' }));
    for (const parent of cases) {
      expect(planWorkflowTerminalParentContinuation(input(parent, effect([0, 0], 'each')))).toMatchObject({
        action: { kind: 'quarantine', reason: 'plan-conflict' },
      });
    }

    const malformedFailure = foreachParent(['a'], [null], 0, { '0': 'child-run' }, { '0': 'unknown' });
    expect(
      planWorkflowTerminalParentContinuation(input(malformedFailure, effect([0, 0], 'each', 'failed'))),
    ).toMatchObject({
      action: { kind: 'quarantine', reason: 'plan-conflict' },
    });
    expect(
      planWorkflowTerminalParentContinuation(input(malformedFailure, effect([0, 0], 'each', 'canceled'))),
    ).toMatchObject({
      action: { kind: 'quarantine', reason: 'plan-conflict' },
    });

    const failedWithUnownedSibling = foreachParent(['a', 'b'], [null, null], 0, { '0': 'child-run' });
    expect(
      planWorkflowTerminalParentContinuation(input(failedWithUnownedSibling, effect([0, 0], 'each', 'failed'))),
    ).toMatchObject({ action: { kind: 'fail-parent', reason: 'parent-fail' } });
  });

  it('continues foreach past suspended siblings when no started iteration remains pending', () => {
    const parent = foreachParent(
      ['a', 'b', 'c'],
      [null, { status: 'suspended' }],
      0,
      { '0': 'child-run', '1': 'suspended-run' },
      {},
    );
    expect(planWorkflowTerminalParentContinuation(input(parent, effect([0, 0], 'each')))).toMatchObject({
      action: {
        kind: 'run-entry',
        reason: 'foreach-continue',
        target: { kind: 'foreach-iteration', iterationIndex: 2 },
      },
    });
  });

  it('waits for a running three-way parallel sibling even when another sibling is suspended', () => {
    const parent = snapshot(
      [
        {
          type: 'parallel',
          steps: ['left', 'middle', 'right'].map(id => ({
            type: 'step' as const,
            step: { id, ...(id === 'left' ? { component: 'WORKFLOW' as const } : {}) },
          })),
        },
      ],
      'left',
      [0, 0],
    );
    parent.context.middle = { status: 'suspended' } as WorkflowRunState['context'][string];
    parent.context.right = { status: 'running' } as WorkflowRunState['context'][string];
    parent.activeStepsPath.middle = [0, 1];
    parent.activeStepsPath.right = [0, 2];
    expect(planWorkflowTerminalParentContinuation(input(parent, effect([0, 0], 'left')))).toMatchObject({
      action: { kind: 'wait', reason: 'parallel-aggregation' },
    });
  });

  it('binds loop decision keys to evidence, revision, graph, source, loop type, and count', () => {
    const loopGraph = (
      id = 'nested',
      loopType: 'dowhile' | 'dountil' = 'dowhile',
      fn = '() => true',
    ): SerializedStepFlowEntry[] => [
      {
        type: 'loop',
        step: { id, component: 'WORKFLOW' },
        loopType,
        serializedCondition: { id: `${id}-condition`, fn },
      },
    ];
    const requestKey = (
      parent: WorkflowRunState,
      terminalEffect = effect([0], Object.keys(parent.activeStepsPath)[0]!),
      revision = 'revision-1',
    ) => createWorkflowTerminalLoopDecisionRequest(input(parent, terminalEffect, revision)).decisionKey;
    const base = snapshot(loopGraph());
    const changedEvidence = snapshot(loopGraph());
    (changedEvidence.context.nested as any).metadata.nestedRunId = 'other-child';
    const changedCount = snapshot(loopGraph());
    (changedCount.context.nested as any).metadata.iterationCount = 1;
    const otherSource = snapshot(loopGraph('other'), 'other');
    const keys = [
      requestKey(base),
      requestKey(snapshot(loopGraph()), effect([0], 'nested'), 'revision-2'),
      requestKey(changedEvidence, effect([0], 'nested', 'success', 'other-child')),
      requestKey(snapshot(loopGraph('nested', 'dowhile', '() => false'))),
      requestKey(otherSource, effect([0], 'other')),
      requestKey(snapshot(loopGraph('nested', 'dountil'))),
      requestKey(changedCount),
    ];
    expect(new Set(keys)).toHaveLength(keys.length);
  });

  it('matches the fixed loop-decision and plan-conflict digest vectors', () => {
    const graph: SerializedStepFlowEntry[] = [
      {
        type: 'loop',
        step: { id: 'nested', component: 'WORKFLOW' },
        loopType: 'dowhile',
        serializedCondition: { id: 'nested-condition', fn: '() => true' },
      },
    ];
    const parent = snapshot(graph);
    (parent.context.nested as any).metadata.iterationCount = 3;
    expect(createWorkflowTerminalLoopDecisionRequest(input(parent)).decisionKey).toBe(
      'sha256:4d54157d5b67f421b87ed4a54af5679ab015e451cf7d4ce20d5c2e306891ffe3',
    );

    const wrongOwner = structuredClone(parent);
    (wrongOwner.context.nested as any).metadata.nestedRunId = 'other';
    expect(planWorkflowTerminalParentContinuation(input(wrongOwner))).toMatchObject({
      action: {
        kind: 'quarantine',
        reason: 'plan-conflict',
        conflictDigest: 'sha256:e979778c41752e4b80aa24f719d8dab4fdc532830c0aeded669885d5c52ca88a',
      },
    });
  });

  it('validates terminal evidence integrity before planning', () => {
    const forged = { ...effect(), parentRunId: 'different-parent' };
    expect(() => planWorkflowTerminalParentContinuation(input(snapshot(), forged))).toThrow(/effect integrity/);
    const negativeZeroPath = { ...effect([0]), parentExecutionPath: [-0] };
    expect(() => planWorkflowTerminalParentContinuation(input(snapshot(), negativeZeroPath))).toThrow(/invalid index/);
  });

  it('ignores unrelated payloads and accessors without invoking or leaking them', () => {
    const parent = snapshot();
    const getter = vi.fn(() => 'secret');
    Object.defineProperty(parent.context, 'input', { enumerable: true, value: ['secret-input'] });
    Object.defineProperty(parent.context, 'unrelated', { enumerable: true, get: getter });
    Object.defineProperty(parent.context.nested!, 'output', { enumerable: true, get: getter });
    Object.defineProperty((parent.context.nested as any).metadata, 'secret', { enumerable: true, get: getter });
    const contract = planWorkflowTerminalParentContinuation(input(parent));
    expect(contract.action).toMatchObject({ kind: 'run-entry', reason: 'next-step' });
    expect(getter).not.toHaveBeenCalled();
    expect(JSON.stringify(contract)).not.toContain('secret-input');
    expect(JSON.stringify(contract)).not.toContain('secret');
  });

  it('rejects proxies before traps and never coerces hostile structural values', () => {
    const envelopeTrap = vi.fn();
    const envelope = new Proxy(input(), {
      getPrototypeOf() {
        envelopeTrap();
        return Object.prototype;
      },
      ownKeys() {
        envelopeTrap();
        return [];
      },
    });
    expect(() => planWorkflowTerminalParentContinuation(envelope)).toThrow(/plain data object/);
    expect(envelopeTrap).not.toHaveBeenCalled();

    const graphTrap = vi.fn();
    const parent = snapshot();
    parent.serializedStepGraph = [
      new Proxy(parent.serializedStepGraph[0]!, {
        getOwnPropertyDescriptor(target, key) {
          graphTrap();
          return Reflect.getOwnPropertyDescriptor(target, key);
        },
      }),
    ];
    expect(() => planWorkflowTerminalParentContinuation(input(parent))).toThrow(/must not be a proxy/);
    expect(graphTrap).not.toHaveBeenCalled();

    const coercion = vi.fn(() => 'running');
    const branch = snapshot(
      [
        {
          type: 'parallel',
          steps: [
            { type: 'step', step: { id: 'left', component: 'WORKFLOW' } },
            { type: 'step', step: { id: 'right' } },
          ],
        },
      ],
      'left',
      [0, 0],
    );
    branch.context.right = { status: { [Symbol.toPrimitive]: coercion } } as any;
    expect(planWorkflowTerminalParentContinuation(input(branch, effect([0, 0], 'left')))).toMatchObject({
      action: { kind: 'quarantine', reason: 'plan-conflict' },
    });
    expect(coercion).not.toHaveBeenCalled();
  });

  it('rejects oversized foreach structural collections before allocating a planning copy', () => {
    const oversized = [] as unknown[];
    oversized.length = 100_001;
    const parent = foreachParent(oversized, [null], 0, { '0': 'child-run' });
    expect(() => planWorkflowTerminalParentContinuation(input(parent, effect([0, 0], 'each')))).toThrow(
      /collection-item limit/,
    );
  });

  it('uses graph-conflict quarantine only for a fingerprintable mismatched coordinate', () => {
    const parent = snapshot();
    const first = planWorkflowTerminalParentContinuation(input(parent, effect([9])));
    const second = planWorkflowTerminalParentContinuation(input(parent, effect([8])));
    expect(first).toMatchObject({ action: { kind: 'quarantine', reason: 'graph-conflict' } });
    expect(second).toMatchObject({ action: { kind: 'quarantine', reason: 'graph-conflict' } });
    expect(second.action).not.toEqual(first.action);

    const malformed = snapshot();
    malformed.serializedStepGraph = [{ type: 'step', step: { id: '' } }] as SerializedStepFlowEntry[];
    expect(() => planWorkflowTerminalParentContinuation(input(malformed))).toThrow(/well-formed bounded string/);
  });

  it('normalizes negative-zero snapshot coordinates and returns isolated repeatable contracts', () => {
    const negative = snapshot();
    negative.activePaths = [-0];
    negative.activeStepsPath.nested = [-0];
    const negativeContract = planWorkflowTerminalParentContinuation(input(negative, effect([0])));
    const normalContract = planWorkflowTerminalParentContinuation(input(snapshot(), effect([0])));
    expect(negativeContract.contractHash).toBe(normalContract.contractHash);
    if (negativeContract.source.kind !== 'step') throw new Error('Expected step source');
    expect(Object.is(negativeContract.source.executionPath[0], -0)).toBe(false);

    (negativeContract.action as any).reason = 'mutated';
    expect(planWorkflowTerminalParentContinuation(input(snapshot(), effect([0])))).toEqual(normalContract);
  });

  it('rejects hostile planner envelopes without executing accessors, clocks, randomness, or callbacks', () => {
    const getter = vi.fn(() => input());
    const hostile = {};
    Object.defineProperty(hostile, 'version', { get: getter, enumerable: true });
    expect(() => planWorkflowTerminalParentContinuation(hostile)).toThrow(/accessor/);
    expect(getter).not.toHaveBeenCalled();

    const sparsePath = Array(2);
    sparsePath[1] = 0;
    expect(() => planWorkflowTerminalParentContinuation(input(snapshot(), effect(sparsePath)))).toThrow(/dense/);
    expect(() => planWorkflowTerminalParentContinuation({ ...input(), extra: true })).toThrow(/unknown field/);
    expect(() => planWorkflowTerminalParentContinuation(Object.assign(Object.create(null), input()))).not.toThrow();
    expect(() => planWorkflowTerminalParentContinuation(Object.assign(new (class PlannerInput {})(), input()))).toThrow(
      /plain data object/,
    );

    const now = vi.spyOn(Date, 'now');
    const random = vi.spyOn(Math, 'random');
    planWorkflowTerminalParentContinuation(input());
    expect(now).not.toHaveBeenCalled();
    expect(random).not.toHaveBeenCalled();
    now.mockRestore();
    random.mockRestore();
  });
});
