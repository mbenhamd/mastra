import { afterEach, beforeEach, describe, expect, expectTypeOf, it, vi } from 'vitest';
import type { WorkflowRunState } from '../../../workflows';
import {
  applyWorkflowTerminalParentContinuationPatch,
  createWorkflowTerminalGraphFingerprint,
  createWorkflowTerminalParentContinuationContract,
} from '../../../workflows/terminal-continuation';
import { InMemoryStore } from '../../mock';
import { createEmptyWorkflowSnapshot } from '../../workflow-snapshot';
import { InMemoryDB } from '../inmemory-db';
import type { WorkflowsStorage } from './base';
import { WorkflowsStorage as WorkflowsStorageBase } from './base';
import { WorkflowsInMemory } from './inmemory';
import { claimWorkflowTerminalizationRecord } from './terminalization';

describe('WorkflowsStorage terminalization defaults', () => {
  it('reports unsupported explicitly when an adapter has not implemented the capability', async () => {
    const workflows = Object.create(WorkflowsStorageBase.prototype) as WorkflowsStorage;
    const run = { workflowName: 'workflow', runId: 'run' };

    expect(workflows.supportsWorkflowTerminalizationJournal()).toBe(false);
    await expect(
      workflows.claimWorkflowTerminalization({
        ...run,
        eventKey: 'event',
        terminalStatus: 'failed',
        ownerId: 'worker',
        leaseMs: 1_000,
      }),
    ).resolves.toEqual({ status: 'unsupported' });
    await expect(workflows.getWorkflowTerminalization(run)).resolves.toEqual({ status: 'unsupported' });
    await expect(
      workflows.advanceWorkflowTerminalization({
        ...run,
        ownerId: 'worker',
        claimToken: 'token',
        claimGeneration: 1,
        expectedPhase: 'terminalization_pending',
        nextPhase: 'run_state_persisted',
      }),
    ).resolves.toEqual({ status: 'unsupported' });
    await expect(
      workflows.releaseWorkflowTerminalization({
        ...run,
        ownerId: 'worker',
        claimToken: 'token',
        claimGeneration: 1,
      }),
    ).resolves.toEqual({ status: 'unsupported' });
    await expect(workflows.deleteCompletedWorkflowTerminalizations({ ...run, olderThan: new Date() })).resolves.toEqual(
      { status: 'unsupported', count: 0 },
    );
    await expect(
      workflows.persistWorkflowTerminalState({
        ...run,
        ownerId: 'worker',
        claimToken: 'token',
        claimGeneration: 1,
        snapshot: { ...createEmptyWorkflowSnapshot(run.runId), status: 'failed' },
      }),
    ).resolves.toEqual({ status: 'unsupported' });
    await expect(
      workflows.reserveWorkflowTerminalDestinationReceipt({
        ...run,
        ownerId: 'worker',
        claimToken: 'token',
        claimGeneration: 1,
        effectKind: 'workflow-finish',
        consumerId: 'finish-dispatcher',
      }),
    ).resolves.toEqual({ status: 'unsupported' });
    await expect(
      workflows.getWorkflowTerminalDestinationReceipt({
        ...run,
        ownerId: 'worker',
        claimToken: 'token',
        claimGeneration: 1,
        effectKind: 'workflow-finish',
        consumerId: 'finish-dispatcher',
      }),
    ).resolves.toEqual({ status: 'unsupported' });
    await expect(
      workflows.getWorkflowTerminalParentContext({
        ...run,
        ownerId: 'worker',
        claimToken: 'token',
        claimGeneration: 1,
      }),
    ).resolves.toEqual({ status: 'unsupported' });
    await expect(
      workflows.getWorkflowTerminalContinuationPlan({
        ...run,
        ownerId: 'worker',
        claimToken: 'token',
        claimGeneration: 1,
      }),
    ).resolves.toEqual({ status: 'unsupported' });
    await expect(
      workflows.applyWorkflowTerminalParentEffect({
        ...run,
        ownerId: 'worker',
        claimToken: 'token',
        claimGeneration: 1,
        contract: {} as never,
      }),
    ).resolves.toEqual({ status: 'unsupported' });
  });
});

describe('WorkflowsInMemory terminalization journal', () => {
  const workflowName = 'workflow';
  const runId = 'run';
  const eventKey = 'event';
  const ownerId = 'worker-a';
  const run = { workflowName, runId };

  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-01-01T00:00:00.000Z'));
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  async function setupWithDb(): Promise<{ db: InMemoryDB; workflows: WorkflowsStorage }> {
    const db = new InMemoryDB();
    const workflows = new WorkflowsInMemory({ db });
    await workflows.persistWorkflowSnapshot({
      ...run,
      snapshot: createEmptyWorkflowSnapshot(runId),
    });
    return { db, workflows };
  }

  async function setup(): Promise<WorkflowsStorage> {
    return (await setupWithDb()).workflows;
  }

  async function acquire(
    workflows: WorkflowsStorage,
    overrides: Partial<Parameters<WorkflowsStorage['claimWorkflowTerminalization']>[0]> = {},
  ) {
    const result = await workflows.claimWorkflowTerminalization({
      ...run,
      eventKey,
      terminalStatus: 'failed',
      ownerId,
      leaseMs: 10_000,
      ...overrides,
    });
    if (result.status !== 'acquired') throw new Error(`Expected acquired, received ${result.status}`);
    return result.record;
  }

  function fence(claim: Awaited<ReturnType<typeof acquire>>) {
    return {
      ...run,
      ownerId: claim.ownerId,
      claimToken: claim.claimToken,
      claimGeneration: claim.claimGeneration,
    };
  }

  function withStatefulIdentity<T extends { workflowName: string; runId: string }>(
    operation: T,
    alternate: { workflowName: string; runId: string },
  ): { operation: T; reads: () => { workflowName: number; runId: number } } {
    const intended = { workflowName: operation.workflowName, runId: operation.runId };
    let workflowNameReads = 0;
    let runIdReads = 0;
    Object.defineProperties(operation, {
      workflowName: {
        enumerable: true,
        get: () => (workflowNameReads++ === 0 ? intended.workflowName : alternate.workflowName),
      },
      runId: {
        enumerable: true,
        get: () => (runIdReads++ === 0 ? intended.runId : alternate.runId),
      },
    });
    return { operation, reads: () => ({ workflowName: workflowNameReads, runId: runIdReads }) };
  }

  async function persistFailedState(workflows: WorkflowsStorage, claim: Awaited<ReturnType<typeof acquire>>) {
    return workflows.persistWorkflowTerminalState({
      ...fence(claim),
      snapshot: { ...createEmptyWorkflowSnapshot(runId), status: 'failed' },
    });
  }

  async function completeAfterDownstreamEvidence(
    db: InMemoryDB,
    workflows: WorkflowsStorage,
    claim: Awaited<ReturnType<typeof acquire>>,
  ) {
    await persistFailedState(workflows, claim);
    const key = JSON.stringify([workflowName, runId]);
    const record = db.workflowTerminalizations.get(key);
    if (!record) throw new Error('Expected terminalization record');
    db.workflowTerminalizations.set(key, { ...record, phase: 'finish_effect_recorded' });
    return workflows.advanceWorkflowTerminalization({
      ...fence(claim),
      expectedPhase: 'finish_effect_recorded',
      nextPhase: 'complete',
    });
  }

  const mergePatch = {
    kind: 'merge-child-terminal',
    resultWrite: 'source-coordinate',
    resultSource: 'retained-child-terminal-envelope',
    payloadWrite: 'preserve-parent-step-payload',
    metadataWrite: 'merge-child-and-bind-nested-run-id',
    stateWrite: 'replace-context-__state-from-retained-child',
    requestContextWrite: 'merge-from-retained-child',
    activeStepsWrite: 'derive-from-source-coordinate',
    snapshotTimestampWrite: 'storage-clock',
    parentRunWrite: { kind: 'preserve' },
    loopWrite: { kind: 'preserve' },
  } as const;

  async function setupGraphBoundParentApplication(
    terminalStatus: 'success' | 'failed' | 'canceled' = 'success',
    mode: 'next' | 'parallel-wait' | 'noop' | 'foreach-single-suspend' | 'foreach-multi-suspend' = 'next',
  ) {
    const db = new InMemoryDB();
    const workflows = new WorkflowsInMemory({ db });
    const parent = { workflowName: 'parent', runId: 'parent-run' };
    const child = { workflowName: 'child', runId: 'child-run' };
    const now = Date.now();
    const foreachSuspendedCount =
      mode === 'foreach-single-suspend' ? 1 : mode === 'foreach-multi-suspend' ? 2 : undefined;
    const sourceStepId = foreachSuspendedCount !== undefined ? 'each' : mode === 'parallel-wait' ? 'left' : 'nested';
    const sourcePath =
      foreachSuspendedCount !== undefined ? [0, foreachSuspendedCount] : mode === 'parallel-wait' ? [0, 0] : [0];
    const sourceResult =
      foreachSuspendedCount === undefined
        ? {
            status: 'running' as const,
            payload: { paperId: 'p1' },
            startedAt: now - 20,
            metadata: { parent: true, nestedRunId: child.runId },
          }
        : {
            status: 'running' as const,
            payload: Array.from({ length: foreachSuspendedCount + 1 }, (_, index) => `input-${index}`),
            output: [
              {
                status: 'suspended',
                suspendPayload: {
                  __streamState: { messageList: { memoryInfo: { resourceId: 'resource-0' } } },
                  __workflow_meta: { resumeLabels: { first: { stepId: 'each', foreachIndex: 0 } } },
                },
              },
              ...(foreachSuspendedCount === 2
                ? [
                    {
                      status: 'suspended',
                      suspendPayload: {
                        __streamState: { messageList: { memoryInfo: { resourceId: 'resource-1' } } },
                        __workflow_meta: { resumeLabels: { second: { stepId: 'each', foreachIndex: 1 } } },
                      },
                    },
                  ]
                : []),
              null,
            ],
            startedAt: now - 20,
            metadata: {
              __workflow_meta: { iterationRunIds: { [String(foreachSuspendedCount)]: child.runId } },
            },
          };
    const parentSnapshot: WorkflowRunState = {
      ...createEmptyWorkflowSnapshot(parent.runId),
      status: mode === 'noop' ? 'success' : 'running',
      value: { stale: true },
      context: {
        [sourceStepId]: sourceResult,
        ...(mode === 'parallel-wait'
          ? {
              right: {
                status: 'running',
                payload: { sibling: true },
                startedAt: now - 20,
              },
            }
          : {}),
        __state: { stale: true },
      } as WorkflowRunState['context'],
      serializedStepGraph:
        foreachSuspendedCount !== undefined
          ? [{ type: 'foreach', step: { id: 'each', component: 'WORKFLOW' }, opts: { concurrency: 2 } }]
          : mode === 'parallel-wait'
            ? [
                {
                  type: 'parallel',
                  steps: [
                    { type: 'step', step: { id: 'left', component: 'WORKFLOW' } },
                    { type: 'step', step: { id: 'right' } },
                  ],
                },
              ]
            : [
                { type: 'step', step: { id: 'nested', component: 'WORKFLOW' } },
                { type: 'sleep', id: 'after-child', duration: 10 },
              ],
      activePaths: mode === 'noop' ? [] : sourcePath,
      activeStepsPath:
        foreachSuspendedCount !== undefined
          ? { each: sourcePath }
          : mode === 'parallel-wait'
            ? { left: [0, 0], right: [0, 1] }
            : mode === 'noop'
              ? {}
              : { nested: [0] },
      requestContext: { parent: true, shared: 'parent' },
      timestamp: now - 20,
    };
    await workflows.persistWorkflowSnapshot({ ...parent, snapshot: parentSnapshot });
    await workflows.persistWorkflowSnapshot({ ...child, snapshot: createEmptyWorkflowSnapshot(child.runId) });
    const claimed = await workflows.claimWorkflowTerminalization({
      ...child,
      eventKey: 'child-terminal',
      terminalStatus,
      ownerId,
      leaseMs: 10_000,
    });
    if (claimed.status !== 'acquired') throw new Error('Expected child claim');
    const terminalResult =
      terminalStatus === 'success'
        ? {
            status: 'success' as const,
            output: { answer: 42 },
            startedAt: now - 10,
            endedAt: now,
            metadata: { child: true },
          }
        : terminalStatus === 'failed'
          ? {
              status: 'failed' as const,
              error: { name: 'Error', message: 'child failed' },
              startedAt: now - 10,
              endedAt: now,
            }
          : { status: 'canceled' as const, startedAt: now - 10, endedAt: now };
    const childSnapshot: WorkflowRunState = {
      ...createEmptyWorkflowSnapshot(child.runId),
      status: terminalStatus,
      result: terminalResult,
      ...(terminalStatus === 'failed' ? { error: { name: 'Error', message: 'child failed' } } : {}),
      value: { final: true },
      context: { __state: { final: true } } as WorkflowRunState['context'],
      requestContext: { child: true, shared: 'child' },
      timestamp: now,
    };
    await workflows.persistWorkflowTerminalState({
      ...child,
      ownerId: claimed.record.ownerId,
      claimToken: claimed.record.claimToken,
      claimGeneration: claimed.record.claimGeneration,
      snapshot: childSnapshot,
    });
    const prepared = await workflows.prepareWorkflowTerminalEffect({
      ...child,
      ownerId: claimed.record.ownerId,
      claimToken: claimed.record.claimToken,
      claimGeneration: claimed.record.claimGeneration,
      expectedPhase: 'run_state_persisted',
      effect: {
        kind: 'parent-workflow-step-end',
        parentWorkflowName: parent.workflowName,
        parentRunId: parent.runId,
        parentStepId: sourceStepId,
        parentExecutionPath: sourcePath,
      },
    });
    if (prepared.status !== 'prepared' || prepared.effect.kind !== 'parent-workflow-step-end') {
      throw new Error('Expected parent effect');
    }
    const fence = {
      ...child,
      ownerId: claimed.record.ownerId,
      claimToken: claimed.record.claimToken,
      claimGeneration: claimed.record.claimGeneration,
    };
    const context = await workflows.getWorkflowTerminalParentContext(fence);
    if (context.status !== 'found') throw new Error('Expected parent context');
    const contractSource =
      foreachSuspendedCount === undefined
        ? ({ kind: 'step', stepId: sourceStepId, executionPath: sourcePath } as const)
        : ({
            kind: 'foreach-iteration',
            stepId: sourceStepId,
            containerPath: [0],
            iterationIndex: foreachSuspendedCount,
          } as const);
    const contractBase = {
      version: 1,
      terminalEffectKey: prepared.effect.effectKey,
      terminalEffectPayloadHash: prepared.effect.payloadHash as `sha256:${string}`,
      executionMode: 'continuous',
      expectedParentRevision: context.revision,
      graphFingerprint: createWorkflowTerminalGraphFingerprint(context.snapshot.serializedStepGraph),
      childTerminalStatus: terminalStatus,
      observedParentStatus: mode === 'noop' ? 'success' : 'running',
      source: contractSource,
    } as const;
    const contract =
      terminalStatus === 'success' && mode === 'noop'
        ? createWorkflowTerminalParentContinuationContract({
            ...contractBase,
            childTerminalStatus: 'success',
            observedParentStatus: 'success',
            action: { kind: 'noop', reason: 'already-terminal' },
            patch: { kind: 'none' },
          })
        : terminalStatus === 'success' && foreachSuspendedCount !== undefined
          ? createWorkflowTerminalParentContinuationContract({
              ...contractBase,
              childTerminalStatus: 'success',
              action: {
                kind: 'suspend-parent',
                reason: 'foreach-suspended',
                target: { kind: 'container', containerType: 'foreach', executionPath: [0] },
              },
              patch: {
                ...mergePatch,
                parentRunWrite: {
                  kind: 'set-suspended',
                  resultSource: 'aggregate-container',
                  activePathSource: 'source-coordinate',
                  suspendedPathsSource: 'aggregate-container',
                  resumeLabelsSource: 'aggregate-container',
                },
              },
            })
          : terminalStatus === 'success' && mode === 'parallel-wait'
            ? createWorkflowTerminalParentContinuationContract({
                ...contractBase,
                childTerminalStatus: 'success',
                action: {
                  kind: 'wait',
                  reason: 'parallel-aggregation',
                  coordinate: { kind: 'container', containerType: 'parallel', executionPath: [0] },
                },
                patch: mergePatch,
              })
            : terminalStatus === 'success'
              ? createWorkflowTerminalParentContinuationContract({
                  ...contractBase,
                  childTerminalStatus: 'success',
                  action: {
                    kind: 'run-entry',
                    reason: 'next-step',
                    target: { kind: 'entry', entryType: 'sleep', entryId: 'after-child', executionPath: [1] },
                  },
                  patch: mergePatch,
                })
              : terminalStatus === 'failed'
                ? createWorkflowTerminalParentContinuationContract({
                    ...contractBase,
                    childTerminalStatus: 'failed',
                    action: { kind: 'fail-parent', reason: 'parent-fail' },
                    patch: {
                      ...mergePatch,
                      parentRunWrite: {
                        kind: 'set',
                        status: 'failed',
                        resultSource: 'source-coordinate',
                        activePathSource: 'source-coordinate',
                      },
                    },
                  })
                : createWorkflowTerminalParentContinuationContract({
                    ...contractBase,
                    childTerminalStatus: 'canceled',
                    action: { kind: 'cancel-parent', reason: 'child-canceled' },
                    patch: {
                      ...mergePatch,
                      parentRunWrite: {
                        kind: 'set',
                        status: 'canceled',
                        resultSource: 'source-coordinate',
                        activePathSource: 'source-coordinate',
                      },
                    },
                  });
    const retained = db.workflowTerminalSnapshots.get(JSON.stringify([child.workflowName, child.runId]));
    if (!retained) throw new Error('Expected retained child snapshot');
    return { db, workflows, parent, context, effect: prepared.effect, contract, fence, retained };
  }

  it('atomically applies the exact PF-1781 patch and stores a pending framework action', async () => {
    const fixture = await setupGraphBoundParentApplication();
    const expected = applyWorkflowTerminalParentContinuationPatch({
      contract: fixture.contract,
      effect: fixture.effect,
      parentRevision: fixture.context.revision,
      parentWorkflowName: fixture.parent.workflowName,
      parentSnapshot: fixture.context.snapshot,
      retainedChild: fixture.retained,
      storageTimestamp: Date.now(),
      executionMode: 'continuous',
    });

    const applied = await fixture.workflows.applyWorkflowTerminalParentEffect({
      ...fixture.fence,
      contract: fixture.contract,
    });
    expect(applied).toMatchObject({
      status: 'applied',
      plan: {
        contract: { contractHash: fixture.contract.contractHash },
        frameworkActionKey: expect.stringMatching(/^wta:v1:[a-f0-9]{64}$/),
      },
    });
    await expect(fixture.workflows.loadWorkflowSnapshot(fixture.parent)).resolves.toEqual(expected);
    await expect(fixture.workflows.getWorkflowTerminalContinuationPlan(fixture.fence)).resolves.toMatchObject({
      status: 'found',
      applicationState: 'applied',
      dispatchState: 'pending',
    });
    await expect(fixture.workflows.getWorkflowTerminalization(fixture.fence)).resolves.toMatchObject({
      status: 'found',
      record: { phase: 'parent_outbox_pending' },
    });
  });

  it.each([
    ['single', 'foreach-single-suspend', true],
    ['multiple', 'foreach-multi-suspend', false],
  ] as const)(
    'persists %s foreach recovery payloads without leaking them into the stored plan',
    async (_label, mode, expectsCompatibilityHoist) => {
      const fixture = await setupGraphBoundParentApplication('success', mode);
      await expect(
        fixture.workflows.applyWorkflowTerminalParentEffect({ ...fixture.fence, contract: fixture.contract }),
      ).resolves.toMatchObject({ status: 'applied' });

      const stored = await fixture.workflows.loadWorkflowSnapshot(fixture.parent);
      if (!stored) throw new Error('Expected stored foreach parent');
      const suspendPayload = (stored.context.each as any).suspendPayload;
      expect(suspendPayload.__workflow_meta.iterationSuspendPayloads['0']).toMatchObject({
        __streamState: { messageList: { memoryInfo: { resourceId: 'resource-0' } } },
      });
      if (mode === 'foreach-multi-suspend') {
        expect(suspendPayload.__workflow_meta.iterationSuspendPayloads['1']).toMatchObject({
          __streamState: { messageList: { memoryInfo: { resourceId: 'resource-1' } } },
        });
      }
      expect(Boolean(suspendPayload.__streamState)).toBe(expectsCompatibilityHoist);

      const plan = await fixture.workflows.getWorkflowTerminalContinuationPlan(fixture.fence);
      expect(plan).toMatchObject({ status: 'found', applicationState: 'applied', dispatchState: 'pending' });
      expect(JSON.stringify(plan)).not.toContain('__streamState');
      expect(JSON.stringify(plan)).not.toContain('resource-0');

      suspendPayload.__workflow_meta.iterationSuspendPayloads['0'].__streamState.messageList.memoryInfo.resourceId =
        'caller-mutated';
      const reloaded = await fixture.workflows.loadWorkflowSnapshot(fixture.parent);
      expect(
        (reloaded?.context.each as any).suspendPayload.__workflow_meta.iterationSuspendPayloads['0'],
      ).toMatchObject({
        __streamState: { messageList: { memoryInfo: { resourceId: 'resource-0' } } },
      });
    },
  );

  it('commits a parallel wait patch without inventing a dispatch action', async () => {
    const fixture = await setupGraphBoundParentApplication('success', 'parallel-wait');
    const result = await fixture.workflows.applyWorkflowTerminalParentEffect({
      ...fixture.fence,
      contract: fixture.contract,
    });
    expect(result).toMatchObject({
      status: 'applied',
      plan: { contract: { action: { kind: 'wait', reason: 'parallel-aggregation' } } },
    });
    if (result.status !== 'applied') throw new Error('Expected applied wait');
    expect(result.plan.frameworkActionKey).toBeUndefined();
    await expect(fixture.workflows.getWorkflowTerminalContinuationPlan(fixture.fence)).resolves.toMatchObject({
      status: 'found',
      applicationState: 'applied',
      dispatchState: 'none',
    });
    await expect(fixture.workflows.getWorkflowTerminalization(fixture.fence)).resolves.toMatchObject({
      status: 'found',
      record: { phase: 'parent_effect_recorded' },
    });
    await expect(fixture.workflows.loadWorkflowSnapshot(fixture.parent)).resolves.toMatchObject({
      status: 'running',
      context: { left: { status: 'success' }, right: { status: 'running' } },
    });
  });

  it('records an already-terminal noop without rewriting or revising the parent', async () => {
    const fixture = await setupGraphBoundParentApplication('success', 'noop');
    const before = await fixture.workflows.loadWorkflowSnapshot(fixture.parent);
    const parentKey = JSON.stringify([fixture.parent.workflowName, fixture.parent.runId]);
    const revisionBefore = fixture.db.workflowTerminalParentRevisions.get(parentKey);
    const result = await fixture.workflows.applyWorkflowTerminalParentEffect({
      ...fixture.fence,
      contract: fixture.contract,
    });
    expect(result).toMatchObject({
      status: 'applied',
      plan: { contract: { action: { kind: 'noop', reason: 'already-terminal' } } },
    });
    if (result.status !== 'applied') throw new Error('Expected applied noop');
    expect(result.plan.frameworkActionKey).toBeUndefined();
    await expect(fixture.workflows.loadWorkflowSnapshot(fixture.parent)).resolves.toEqual(before);
    expect(fixture.db.workflowTerminalParentRevisions.get(parentKey)).toBe(revisionBefore);
    await expect(fixture.workflows.getWorkflowTerminalContinuationPlan(fixture.fence)).resolves.toMatchObject({
      status: 'found',
      applicationState: 'applied',
      dispatchState: 'none',
    });
  });

  it.each([
    ['failed', 'fail-parent', 'failed'],
    ['canceled', 'cancel-parent', 'canceled'],
  ] as const)(
    'applies %s terminal semantics without admitting a next-step plan',
    async (terminalStatus, actionKind, parentStatus) => {
      const fixture = await setupGraphBoundParentApplication(terminalStatus);
      await expect(
        fixture.workflows.applyWorkflowTerminalParentEffect({ ...fixture.fence, contract: fixture.contract }),
      ).resolves.toMatchObject({
        status: 'applied',
        plan: { contract: { action: { kind: actionKind } }, frameworkActionKey: expect.any(String) },
      });
      await expect(fixture.workflows.loadWorkflowSnapshot(fixture.parent)).resolves.toMatchObject({
        status: parentStatus,
        result: { status: terminalStatus },
      });
    },
  );

  it('replays only the exact contract and redacts a changed revision conflict', async () => {
    const fixture = await setupGraphBoundParentApplication();
    await expect(
      fixture.workflows.applyWorkflowTerminalParentEffect({ ...fixture.fence, contract: fixture.contract }),
    ).resolves.toMatchObject({ status: 'applied' });
    await expect(
      fixture.workflows.applyWorkflowTerminalParentEffect({ ...fixture.fence, contract: fixture.contract }),
    ).resolves.toMatchObject({ status: 'already_applied' });

    fixture.db.workflowTerminalSnapshots.delete(JSON.stringify([fixture.fence.workflowName, fixture.fence.runId]));
    await fixture.workflows.deleteWorkflowRunById(fixture.parent);
    await expect(
      fixture.workflows.applyWorkflowTerminalParentEffect({ ...fixture.fence, contract: fixture.contract }),
    ).resolves.toMatchObject({ status: 'already_applied' });

    const { contractHash: _contractHash, ...contractSpec } = fixture.contract;
    const changedRevision = createWorkflowTerminalParentContinuationContract({
      ...contractSpec,
      expectedParentRevision: 'mem:v1:999',
    });
    await expect(
      fixture.workflows.applyWorkflowTerminalParentEffect({ ...fixture.fence, contract: changedRevision }),
    ).resolves.toMatchObject({
      status: 'contract_conflict',
      plan: {
        contractHash: fixture.contract.contractHash,
        actionKind: 'run-entry',
        actionReason: 'next-step',
      },
    });
  });

  it('rejects a forged contract hash before creating receipt or plan evidence', async () => {
    const fixture = await setupGraphBoundParentApplication();
    const before = await fixture.workflows.loadWorkflowSnapshot(fixture.parent);
    const forged = { ...fixture.contract, contractHash: `sha256:${'0'.repeat(64)}` } as typeof fixture.contract;
    await expect(
      fixture.workflows.applyWorkflowTerminalParentEffect({ ...fixture.fence, contract: forged }),
    ).resolves.toEqual({ status: 'invalid_contract' });
    await expect(fixture.workflows.loadWorkflowSnapshot(fixture.parent)).resolves.toEqual(before);
    expect(fixture.db.workflowTerminalDestinationReceipts).toHaveLength(0);
    expect(fixture.db.workflowTerminalContinuationPlans).toHaveLength(0);
  });

  it('keeps a well-formed but unbound source contract in the caller-error category', async () => {
    const fixture = await setupGraphBoundParentApplication();
    const { contractHash: _contractHash, source: _source, ...contractSpec } = fixture.contract;
    const unbound = createWorkflowTerminalParentContinuationContract({
      ...contractSpec,
      source: { kind: 'step', stepId: 'missing', executionPath: [0] },
    });

    await expect(
      fixture.workflows.applyWorkflowTerminalParentEffect({ ...fixture.fence, contract: unbound }),
    ).resolves.toEqual({ status: 'invalid_contract' });
    expect(fixture.db.workflowTerminalDestinationReceipts).toHaveLength(0);
    expect(fixture.db.workflowTerminalContinuationPlans).toHaveLength(0);
  });

  it('isolates stored parent context and continuation evidence from caller aliases', async () => {
    const fixture = await setupGraphBoundParentApplication();
    const originalContract = structuredClone(fixture.contract);
    const originalParent = await fixture.workflows.loadWorkflowSnapshot(fixture.parent);

    fixture.context.snapshot.context.nested = { status: 'failed' } as never;
    await expect(fixture.workflows.loadWorkflowSnapshot(fixture.parent)).resolves.toEqual(originalParent);

    const result = await fixture.workflows.applyWorkflowTerminalParentEffect({
      ...fixture.fence,
      contract: fixture.contract,
    });
    if (result.status !== 'applied') throw new Error('Expected applied continuation');
    (fixture.contract.action as { reason: string }).reason = 'caller-mutated-input';
    (result.plan.contract.action as { reason: string }).reason = 'caller-mutated-result';

    await expect(fixture.workflows.getWorkflowTerminalContinuationPlan(fixture.fence)).resolves.toMatchObject({
      status: 'found',
      plan: { contract: originalContract },
    });
  });

  it('isolates every in-memory parent snapshot mutation boundary from caller aliases', async () => {
    const fixture = await setupGraphBoundParentApplication();
    const persisted = structuredClone(fixture.context.snapshot);
    await fixture.workflows.persistWorkflowSnapshot({ ...fixture.parent, snapshot: persisted });
    persisted.status = 'failed';
    persisted.context.nested = { status: 'failed' } as never;
    await expect(fixture.workflows.loadWorkflowSnapshot(fixture.parent)).resolves.toMatchObject({
      status: 'running',
      context: { nested: { status: 'running' } },
    });

    const updated = await fixture.workflows.updateWorkflowState({
      ...fixture.parent,
      opts: { value: { stored: true } },
    });
    if (!updated) throw new Error('Expected updated parent snapshot');
    updated.status = 'canceled';
    updated.value = { caller: 'mutated' };
    await expect(fixture.workflows.loadWorkflowSnapshot(fixture.parent)).resolves.toMatchObject({
      status: 'running',
      value: { stored: true },
    });

    const result = { status: 'success', output: { stored: true } } as const;
    const context = await fixture.workflows.updateWorkflowResults({
      ...fixture.parent,
      stepId: 'nested',
      result,
      requestContext: {},
    });
    context.nested.output = { caller: 'mutated' };
    await expect(fixture.workflows.loadWorkflowSnapshot(fixture.parent)).resolves.toMatchObject({
      context: { nested: { output: { stored: true } } },
    });
  });

  it('rejects future parent timestamps instead of fabricating a storage clock', async () => {
    const fixture = await setupGraphBoundParentApplication();
    const parentKey = JSON.stringify([fixture.parent.workflowName, fixture.parent.runId]);
    const parent = fixture.db.workflows.get(parentKey);
    if (!parent?.snapshot || typeof parent.snapshot === 'string') throw new Error('Expected in-memory parent snapshot');
    parent.snapshot.timestamp = Date.now() + 60_000;

    await expect(
      fixture.workflows.applyWorkflowTerminalParentEffect({ ...fixture.fence, contract: fixture.contract }),
    ).resolves.toEqual({ status: 'corrupt_parent_state' });
    expect(fixture.db.workflowTerminalDestinationReceipts).toHaveLength(0);
    expect(fixture.db.workflowTerminalContinuationPlans).toHaveLength(0);
  });

  it('classifies corrupt retained child evidence separately from caller contracts', async () => {
    const fixture = await setupGraphBoundParentApplication();
    const journalKey = JSON.stringify([fixture.fence.workflowName, fixture.fence.runId]);
    const retained = fixture.db.workflowTerminalSnapshots.get(journalKey);
    if (!retained) throw new Error('Expected retained child snapshot');
    delete retained.snapshot.context.__state;

    await expect(
      fixture.workflows.applyWorkflowTerminalParentEffect({ ...fixture.fence, contract: fixture.contract }),
    ).resolves.toEqual({ status: 'corrupt_child_terminal_state' });
    expect(fixture.db.workflowTerminalDestinationReceipts).toHaveLength(0);
    expect(fixture.db.workflowTerminalContinuationPlans).toHaveLength(0);
  });

  it('classifies malformed retained child result envelopes as child corruption', async () => {
    const fixture = await setupGraphBoundParentApplication();
    const journalKey = JSON.stringify([fixture.fence.workflowName, fixture.fence.runId]);
    const retained = fixture.db.workflowTerminalSnapshots.get(journalKey);
    if (!retained) throw new Error('Expected retained child snapshot');
    retained.snapshot.result = { status: 'failed' } as never;

    await expect(
      fixture.workflows.applyWorkflowTerminalParentEffect({ ...fixture.fence, contract: fixture.contract }),
    ).resolves.toEqual({ status: 'corrupt_child_terminal_state' });
    expect(fixture.db.workflowTerminalDestinationReceipts).toHaveLength(0);
    expect(fixture.db.workflowTerminalContinuationPlans).toHaveLength(0);
  });

  it('classifies malformed stored parent context separately from caller contracts', async () => {
    const fixture = await setupGraphBoundParentApplication();
    const parentKey = JSON.stringify([fixture.parent.workflowName, fixture.parent.runId]);
    const parent = fixture.db.workflows.get(parentKey);
    if (!parent?.snapshot || typeof parent.snapshot === 'string') throw new Error('Expected in-memory parent snapshot');
    parent.snapshot.requestContext = [] as never;

    await expect(
      fixture.workflows.applyWorkflowTerminalParentEffect({ ...fixture.fence, contract: fixture.contract }),
    ).resolves.toEqual({ status: 'corrupt_parent_state' });
    expect(fixture.db.workflowTerminalDestinationReceipts).toHaveLength(0);
    expect(fixture.db.workflowTerminalContinuationPlans).toHaveLength(0);
  });

  it('classifies malformed parent graph and parent-derived result timing as parent corruption', async () => {
    const graphFixture = await setupGraphBoundParentApplication();
    const graphParentKey = JSON.stringify([graphFixture.parent.workflowName, graphFixture.parent.runId]);
    const graphParent = graphFixture.db.workflows.get(graphParentKey);
    if (!graphParent?.snapshot || typeof graphParent.snapshot === 'string') {
      throw new Error('Expected in-memory parent snapshot');
    }
    graphParent.snapshot.serializedStepGraph = [null] as never;
    await expect(
      graphFixture.workflows.applyWorkflowTerminalParentEffect({
        ...graphFixture.fence,
        contract: graphFixture.contract,
      }),
    ).resolves.toEqual({ status: 'corrupt_parent_state' });

    const timingFixture = await setupGraphBoundParentApplication();
    const timingParentKey = JSON.stringify([timingFixture.parent.workflowName, timingFixture.parent.runId]);
    const timingParent = timingFixture.db.workflows.get(timingParentKey);
    if (!timingParent?.snapshot || typeof timingParent.snapshot === 'string') {
      throw new Error('Expected in-memory parent snapshot');
    }
    (timingParent.snapshot.context.nested as { startedAt: number }).startedAt = -1;
    await expect(
      timingFixture.workflows.applyWorkflowTerminalParentEffect({
        ...timingFixture.fence,
        contract: timingFixture.contract,
      }),
    ).resolves.toEqual({ status: 'corrupt_parent_state' });
  });

  it('fails closed when persisted in-memory parent revision evidence disappears', async () => {
    const fixture = await setupGraphBoundParentApplication();
    const parentKey = JSON.stringify([fixture.parent.workflowName, fixture.parent.runId]);
    fixture.db.workflowTerminalParentRevisions.delete(parentKey);

    await expect(
      fixture.workflows.applyWorkflowTerminalParentEffect({ ...fixture.fence, contract: fixture.contract }),
    ).resolves.toEqual({ status: 'corrupt_parent_state' });
    expect(fixture.db.workflowTerminalDestinationReceipts).toHaveLength(0);
    expect(fixture.db.workflowTerminalContinuationPlans).toHaveLength(0);
  });

  it.each([
    ['applied quarantine', 'run-entry', 'quarantined'],
    ['quarantined non-quarantine', 'quarantine', 'applied'],
  ] as const)('fails closed for contradictory %s replay evidence', async (_label, initialAction, receiptState) => {
    const fixture = await setupGraphBoundParentApplication();
    let contract = fixture.contract;
    if (initialAction === 'quarantine') {
      const { contractHash: _contractHash, action: _action, patch: _patch, ...base } = contract;
      contract = createWorkflowTerminalParentContinuationContract({
        ...base,
        action: { kind: 'quarantine', reason: 'plan-conflict', conflictDigest: `sha256:${'f'.repeat(64)}` },
        patch: { kind: 'none' },
      });
    }
    await fixture.workflows.applyWorkflowTerminalParentEffect({ ...fixture.fence, contract });
    const receiptKey = JSON.stringify([fixture.effect.effectKey, 'mastra.parent-application.v1']);
    const receipt = fixture.db.workflowTerminalDestinationReceipts.get(receiptKey);
    if (!receipt) throw new Error('Expected stored parent receipt');
    const journalKey = JSON.stringify([fixture.fence.workflowName, fixture.fence.runId]);
    const journal = fixture.db.workflowTerminalizations.get(journalKey);
    if (!journal) throw new Error('Expected stored child journal');
    if (receiptState === 'quarantined') {
      fixture.db.workflowTerminalDestinationReceipts.set(receiptKey, {
        ...receipt,
        applicationState: 'quarantined',
        dispatchState: 'none',
        appliedAt: undefined,
        dispatchPendingAt: undefined,
        destinationAppliedAt: undefined,
        quarantinedAt: receipt.updatedAt,
      });
      fixture.db.workflowTerminalizations.set(journalKey, { ...journal, phase: 'parent_outbox_pending' });
    } else {
      fixture.db.workflowTerminalDestinationReceipts.set(receiptKey, {
        ...receipt,
        applicationState: 'applied',
        dispatchState: 'none',
        appliedAt: receipt.updatedAt,
        dispatchPendingAt: undefined,
        destinationAppliedAt: undefined,
        quarantinedAt: undefined,
      });
      fixture.db.workflowTerminalizations.set(journalKey, { ...journal, phase: 'parent_effect_recorded' });
    }
    await expect(fixture.workflows.applyWorkflowTerminalParentEffect({ ...fixture.fence, contract })).rejects.toThrow(
      'Contradictory workflow terminal parent application evidence',
    );
  });

  it('stores quarantine evidence without mutating the parent or creating a framework action', async () => {
    const fixture = await setupGraphBoundParentApplication();
    const before = await fixture.workflows.loadWorkflowSnapshot(fixture.parent);
    const { contractHash: _contractHash, action: _action, patch: _patch, ...base } = fixture.contract;
    const quarantine = createWorkflowTerminalParentContinuationContract({
      ...base,
      action: {
        kind: 'quarantine',
        reason: 'plan-conflict',
        conflictDigest: `sha256:${'f'.repeat(64)}`,
      },
      patch: { kind: 'none' },
    });
    const result = await fixture.workflows.applyWorkflowTerminalParentEffect({
      ...fixture.fence,
      contract: quarantine,
    });
    expect(result).toMatchObject({
      status: 'quarantined',
      plan: { contract: { action: { kind: 'quarantine' } } },
    });
    if (result.status !== 'quarantined') throw new Error('Expected quarantine');
    expect(result.plan.frameworkActionKey).toBeUndefined();
    await expect(fixture.workflows.loadWorkflowSnapshot(fixture.parent)).resolves.toEqual(before);
    await expect(fixture.workflows.getWorkflowTerminalContinuationPlan(fixture.fence)).resolves.toMatchObject({
      status: 'found',
      applicationState: 'quarantined',
      dispatchState: 'none',
    });
  });

  it('changes opaque parent revisions across writes and delete/recreate ABA cycles', async () => {
    const fixture = await setupGraphBoundParentApplication();
    const originalRevision = fixture.context.revision;
    await fixture.workflows.persistWorkflowSnapshot({
      ...fixture.parent,
      snapshot: structuredClone(fixture.context.snapshot),
    });
    const rewritten = await fixture.workflows.getWorkflowTerminalParentContext(fixture.fence);
    expect(rewritten).toMatchObject({ status: 'found' });
    if (rewritten.status !== 'found') throw new Error('Expected rewritten parent context');
    expect(rewritten.revision).not.toBe(originalRevision);

    await fixture.workflows.deleteWorkflowRunById(fixture.parent);
    await fixture.workflows.persistWorkflowSnapshot({
      ...fixture.parent,
      snapshot: structuredClone(fixture.context.snapshot),
    });
    const recreated = await fixture.workflows.getWorkflowTerminalParentContext(fixture.fence);
    expect(recreated).toMatchObject({ status: 'found' });
    if (recreated.status !== 'found') throw new Error('Expected recreated parent context');
    expect(recreated.revision).not.toBe(rewritten.revision);
    expect(recreated.revision).not.toBe(originalRevision);
  });

  it('allows only one child effect to consume a shared parent revision', async () => {
    const db = new InMemoryDB();
    const workflows = new WorkflowsInMemory({ db });
    const parent = { workflowName: 'race-parent', runId: 'parent-run' };
    const now = Date.now();
    const parentSnapshot: WorkflowRunState = {
      ...createEmptyWorkflowSnapshot(parent.runId),
      status: 'running',
      context: {
        left: {
          status: 'running',
          payload: {},
          startedAt: now - 20,
          metadata: { nestedRunId: 'child-left' },
        },
        right: {
          status: 'running',
          payload: {},
          startedAt: now - 20,
          metadata: { nestedRunId: 'child-right' },
        },
      } as WorkflowRunState['context'],
      serializedStepGraph: [
        {
          type: 'parallel',
          steps: [
            { type: 'step', step: { id: 'left', component: 'WORKFLOW' } },
            { type: 'step', step: { id: 'right', component: 'WORKFLOW' } },
          ],
        },
      ],
      activePaths: [0, 0],
      activeStepsPath: { left: [0, 0], right: [0, 1] },
      timestamp: now - 20,
    };
    await workflows.persistWorkflowSnapshot({ ...parent, snapshot: parentSnapshot });

    const prepareChild = async (runId: string, stepId: 'left' | 'right', path: [0, 0] | [0, 1]) => {
      const child = { workflowName: `race-${stepId}`, runId };
      await workflows.persistWorkflowSnapshot({ ...child, snapshot: createEmptyWorkflowSnapshot(runId) });
      const claim = await workflows.claimWorkflowTerminalization({
        ...child,
        eventKey: `event-${stepId}`,
        terminalStatus: 'success',
        ownerId,
        leaseMs: 10_000,
      });
      if (claim.status !== 'acquired') throw new Error('Expected race child claim');
      const childFence = {
        ...child,
        ownerId: claim.record.ownerId,
        claimToken: claim.record.claimToken,
        claimGeneration: claim.record.claimGeneration,
      };
      await workflows.persistWorkflowTerminalState({
        ...childFence,
        snapshot: {
          ...createEmptyWorkflowSnapshot(runId),
          status: 'success',
          result: { status: 'success', output: stepId, startedAt: now - 10, endedAt: now },
          context: { __state: { [stepId]: true } } as WorkflowRunState['context'],
          value: { [stepId]: true },
          timestamp: now,
        },
      });
      const effect = await workflows.prepareWorkflowTerminalEffect({
        ...childFence,
        expectedPhase: 'run_state_persisted',
        effect: {
          kind: 'parent-workflow-step-end',
          parentWorkflowName: parent.workflowName,
          parentRunId: parent.runId,
          parentStepId: stepId,
          parentExecutionPath: path,
        },
      });
      if (effect.status !== 'prepared' || effect.effect.kind !== 'parent-workflow-step-end') {
        throw new Error('Expected race parent effect');
      }
      return { fence: childFence, effect: effect.effect, stepId, path };
    };

    const [left, right] = await Promise.all([
      prepareChild('child-left', 'left', [0, 0]),
      prepareChild('child-right', 'right', [0, 1]),
    ]);
    const leftContext = await workflows.getWorkflowTerminalParentContext(left.fence);
    const rightContext = await workflows.getWorkflowTerminalParentContext(right.fence);
    if (leftContext.status !== 'found' || rightContext.status !== 'found') {
      throw new Error('Expected both race planning contexts');
    }
    expect(leftContext.revision).toBe(rightContext.revision);
    const contractFor = (candidate: typeof left | typeof right) =>
      createWorkflowTerminalParentContinuationContract({
        version: 1,
        terminalEffectKey: candidate.effect.effectKey,
        terminalEffectPayloadHash: candidate.effect.payloadHash,
        executionMode: 'continuous',
        expectedParentRevision: leftContext.revision,
        graphFingerprint: createWorkflowTerminalGraphFingerprint(parentSnapshot.serializedStepGraph),
        childTerminalStatus: 'success',
        observedParentStatus: 'running',
        source: { kind: 'step', stepId: candidate.stepId, executionPath: candidate.path },
        action: {
          kind: 'wait',
          reason: 'parallel-aggregation',
          coordinate: { kind: 'container', containerType: 'parallel', executionPath: [0] },
        },
        patch: mergePatch,
      });
    const outcomes = await Promise.all([
      workflows.applyWorkflowTerminalParentEffect({ ...left.fence, contract: contractFor(left) }),
      workflows.applyWorkflowTerminalParentEffect({ ...right.fence, contract: contractFor(right) }),
    ]);
    expect(outcomes.map(result => result.status).sort()).toEqual(['applied', 'parent_conflict']);
    expect(db.workflowTerminalDestinationReceipts).toHaveLength(1);
    expect(db.workflowTerminalContinuationPlans).toHaveLength(1);
  });

  it('serializes acquisition and requires the exact fence for renewal', async () => {
    const workflows = await setup();
    const [first, second] = await Promise.all([
      workflows.claimWorkflowTerminalization({
        ...run,
        eventKey,
        terminalStatus: 'failed',
        ownerId,
        leaseMs: 1_000,
      }),
      workflows.claimWorkflowTerminalization({
        ...run,
        eventKey,
        terminalStatus: 'failed',
        ownerId: 'worker-b',
        leaseMs: 1_000,
      }),
    ]);
    expect([first.status, second.status].sort()).toEqual(['acquired', 'leased']);
    const acquired = first.status === 'acquired' ? first : second;
    const leased = first.status === 'leased' ? first : second;
    if (acquired.status !== 'acquired') throw new Error('Acquired claim not found');
    if (leased.status !== 'leased') throw new Error('Leased result not found');
    expectTypeOf(acquired.record.ownerId).toEqualTypeOf<string>();
    expectTypeOf(acquired.record.claimToken).toEqualTypeOf<string>();
    expectTypeOf(acquired.record.leaseExpiresAt).toEqualTypeOf<number>();
    expect(leased.record).not.toHaveProperty('ownerId');
    expect(leased.record).not.toHaveProperty('claimToken');
    expect(leased.record).not.toHaveProperty('claimGeneration');

    await expect(
      workflows.claimWorkflowTerminalization({
        ...run,
        eventKey,
        terminalStatus: 'failed',
        ownerId: acquired.record.ownerId!,
        leaseMs: 2_000,
      }),
    ).resolves.toMatchObject({ status: 'leased' });
    await expect(
      workflows.claimWorkflowTerminalization({
        ...run,
        eventKey,
        terminalStatus: 'failed',
        ownerId: acquired.record.ownerId!,
        claimToken: acquired.record.claimToken,
        claimGeneration: acquired.record.claimGeneration,
        leaseMs: 2_000,
      }),
    ).resolves.toMatchObject({ status: 'renewed', record: { leaseExpiresAt: Date.now() + 2_000 } });
  });

  it('increments the generation on expiry and permanently fences the stale worker', async () => {
    const workflows = await setup();
    const first = await acquire(workflows, { leaseMs: 1_000 });
    vi.advanceTimersByTime(1_001);
    const second = await acquire(workflows, { ownerId: 'worker-b', leaseMs: 1_000 });
    expect(second.claimGeneration).toBe(first.claimGeneration + 1);
    expect(second.claimToken).not.toBe(first.claimToken);

    await expect(
      workflows.persistWorkflowTerminalState({
        ...run,
        ownerId,
        claimToken: first.claimToken!,
        claimGeneration: first.claimGeneration,
        snapshot: { ...createEmptyWorkflowSnapshot(runId), status: 'failed' },
      }),
    ).resolves.toMatchObject({ status: 'not_owner' });
  });

  it('enforces the root and nested monotonic phase graph', async () => {
    const workflows = await setup();
    const claim = await acquire(workflows);
    const fenced = {
      ...run,
      ownerId,
      claimToken: claim.claimToken!,
      claimGeneration: claim.claimGeneration,
    };

    await expect(
      workflows.advanceWorkflowTerminalization({
        ...fenced,
        expectedPhase: 'terminalization_pending',
        nextPhase: 'finish_outbox_pending',
      }),
    ).resolves.toEqual({ status: 'invalid_transition' });
    for (const expectedPhase of ['__proto__', 'constructor', 'toString']) {
      await expect(
        workflows.advanceWorkflowTerminalization({
          ...fenced,
          expectedPhase: expectedPhase as never,
          nextPhase: 'run_state_persisted',
        }),
      ).resolves.toEqual({ status: 'invalid_transition' });
    }
    await expect(
      workflows.advanceWorkflowTerminalization({
        ...fenced,
        expectedPhase: 'terminalization_pending',
        nextPhase: 'run_state_persisted',
      }),
    ).resolves.toEqual({ status: 'invalid_transition' });
    await expect(
      workflows.persistWorkflowTerminalState({
        ...fenced,
        snapshot: { ...createEmptyWorkflowSnapshot(runId), status: 'failed' },
      }),
    ).resolves.toMatchObject({ status: 'persisted', record: { phase: 'run_state_persisted' } });
    await expect(
      workflows.advanceWorkflowTerminalization({
        ...fenced,
        expectedPhase: 'run_state_persisted',
        nextPhase: 'finish_outbox_pending',
      }),
    ).resolves.toEqual({ status: 'invalid_transition' });
  });

  it('atomically persists an isolated terminal snapshot before certifying the journal phase', async () => {
    const workflows = await setup();
    const claim = await acquire(workflows);
    const fenced = fence(claim);

    await expect(
      workflows.persistWorkflowTerminalState({
        ...fenced,
        snapshot: { ...createEmptyWorkflowSnapshot('other-run'), status: 'failed' },
      }),
    ).resolves.toEqual({ status: 'invalid_snapshot' });
    await expect(
      workflows.persistWorkflowTerminalState({
        ...fenced,
        snapshot: { ...createEmptyWorkflowSnapshot(runId), status: 'paused' } as never,
      }),
    ).resolves.toEqual({ status: 'invalid_snapshot' });
    for (const snapshot of [null, undefined, 'invalid', 1, []]) {
      await expect(workflows.persistWorkflowTerminalState({ ...fenced, snapshot: snapshot as never })).resolves.toEqual(
        { status: 'invalid_snapshot' },
      );
    }
    await expect(workflows.getWorkflowTerminalization(run)).resolves.toMatchObject({
      status: 'found',
      record: { phase: 'terminalization_pending' },
    });

    const terminalSnapshot = {
      ...createEmptyWorkflowSnapshot(runId),
      status: 'failed' as const,
      context: { marker: { status: 'success' as const, output: { value: 'retained' } } },
    };
    await expect(
      workflows.persistWorkflowTerminalState({ ...fenced, snapshot: terminalSnapshot }),
    ).resolves.toMatchObject({ status: 'persisted', record: { phase: 'run_state_persisted' } });

    terminalSnapshot.context.marker.output.value = 'caller-mutated';
    terminalSnapshot.status = 'success' as never;
    await expect(workflows.loadWorkflowSnapshot(run)).resolves.toMatchObject({
      status: 'failed',
      context: { marker: { output: { value: 'retained' } } },
    });

    await workflows.persistWorkflowSnapshot({ ...run, snapshot: createEmptyWorkflowSnapshot(runId) });
    await expect(workflows.loadWorkflowSnapshot(run)).resolves.toMatchObject({ status: 'pending' });
    await expect(workflows.getWorkflowTerminalization(run)).resolves.toMatchObject({
      status: 'found',
      record: { phase: 'run_state_persisted' },
    });
  });

  it('normalizes inherited stateful identity fields before validating and storing a snapshot', async () => {
    const workflows = await setup();
    const claim = await acquire(workflows);
    let statusReads = 0;
    const snapshot = { ...createEmptyWorkflowSnapshot(runId) };
    delete (snapshot as Partial<typeof snapshot>).status;
    const prototype = {};
    Object.defineProperty(prototype, 'status', {
      configurable: true,
      enumerable: true,
      get: () => (statusReads++ === 0 ? 'failed' : 'success'),
    });
    Object.setPrototypeOf(snapshot, prototype);

    await expect(workflows.persistWorkflowTerminalState({ ...fence(claim), snapshot })).resolves.toMatchObject({
      status: 'persisted',
      record: { phase: 'run_state_persisted' },
    });
    expect(statusReads).toBe(1);
    await expect(workflows.loadWorkflowSnapshot(run)).resolves.toMatchObject({ status: 'failed' });
  });

  it('materializes accessor-backed operation identity once before journal and snapshot lookup', async () => {
    const workflows = await setup();
    const claim = await acquire(workflows);
    const alternate = { workflowName: 'alternate-workflow', runId: 'alternate-run' };
    await workflows.persistWorkflowSnapshot({
      ...alternate,
      snapshot: createEmptyWorkflowSnapshot(alternate.runId),
    });
    const operation = {
      ...fence(claim),
      snapshot: { ...createEmptyWorkflowSnapshot(runId), status: 'failed' as const },
    };
    let workflowNameReads = 0;
    let runIdReads = 0;
    Object.defineProperties(operation, {
      workflowName: {
        enumerable: true,
        get: () => (workflowNameReads++ === 0 ? workflowName : alternate.workflowName),
      },
      runId: { enumerable: true, get: () => (runIdReads++ === 0 ? runId : alternate.runId) },
    });

    await expect(workflows.persistWorkflowTerminalState(operation)).resolves.toMatchObject({
      status: 'persisted',
      record: { phase: 'run_state_persisted' },
    });
    expect({ workflowNameReads, runIdReads }).toEqual({ workflowNameReads: 1, runIdReads: 1 });
    await expect(workflows.loadWorkflowSnapshot(run)).resolves.toMatchObject({ status: 'failed' });
    await expect(workflows.loadWorkflowSnapshot(alternate)).resolves.toMatchObject({ status: 'pending' });
    await expect(workflows.getWorkflowTerminalization(run)).resolves.toMatchObject({
      status: 'found',
      record: { phase: 'run_state_persisted' },
    });
    await expect(workflows.getWorkflowTerminalization(alternate)).resolves.toEqual({ status: 'missing_record' });
  });

  it('materializes every in-memory journal operation before selecting or mutating a run', async () => {
    const { db, workflows } = await setupWithDb();
    let pairIndex = 0;
    const makePair = async (label: string) => {
      const index = pairIndex++;
      const intended = { workflowName: `${label}-${index}`, runId: `${label}-${index}-run` };
      const alternate = {
        workflowName: `${label}-${index}-alternate`,
        runId: `${label}-${index}-alternate-run`,
      };
      await workflows.persistWorkflowSnapshot({
        ...intended,
        snapshot: createEmptyWorkflowSnapshot(intended.runId),
      });
      await workflows.persistWorkflowSnapshot({
        ...alternate,
        snapshot: createEmptyWorkflowSnapshot(alternate.runId),
      });
      return { intended, alternate };
    };
    const claim = async (run: { workflowName: string; runId: string }, eventKey: string) => {
      const result = await workflows.claimWorkflowTerminalization({
        ...run,
        eventKey,
        terminalStatus: 'failed',
        ownerId: `${eventKey}-owner`,
        leaseMs: 10_000,
      });
      if (result.status !== 'acquired') throw new Error(`claim failed for ${eventKey}`);
      return result.record;
    };

    const claimPair = await makePair('claim');
    const claimOperation = withStatefulIdentity(
      {
        ...claimPair.intended,
        eventKey: 'claim-event',
        terminalStatus: 'failed' as const,
        ownerId: 'claim-owner',
        leaseMs: 10_000,
      },
      claimPair.alternate,
    );
    await expect(workflows.claimWorkflowTerminalization(claimOperation.operation)).resolves.toMatchObject({
      status: 'acquired',
      record: { eventKey: 'claim-event' },
    });
    expect(claimOperation.reads()).toEqual({ workflowName: 1, runId: 1 });
    const getOperation = withStatefulIdentity({ ...claimPair.intended }, claimPair.alternate);
    await expect(workflows.getWorkflowTerminalization(getOperation.operation)).resolves.toMatchObject({
      status: 'found',
      record: { eventKey: 'claim-event' },
    });
    expect(getOperation.reads()).toEqual({ workflowName: 1, runId: 1 });
    await expect(workflows.getWorkflowTerminalization(claimPair.alternate)).resolves.toEqual({
      status: 'missing_record',
    });

    const advancePair = await makePair('advance');
    const advanceClaim = await claim(advancePair.intended, 'advance-intended');
    await claim(advancePair.alternate, 'advance-alternate');
    const advanceKey = JSON.stringify([advancePair.intended.workflowName, advancePair.intended.runId]);
    const advanceRecord = db.workflowTerminalizations.get(advanceKey);
    if (!advanceRecord) throw new Error('advance record missing');
    db.workflowTerminalizations.set(advanceKey, { ...advanceRecord, phase: 'finish_effect_recorded' });
    const advanceOperation = withStatefulIdentity(
      {
        ...advancePair.intended,
        ownerId: advanceClaim.ownerId,
        claimToken: advanceClaim.claimToken,
        claimGeneration: advanceClaim.claimGeneration,
        expectedPhase: 'finish_effect_recorded' as const,
        nextPhase: 'complete' as const,
      },
      advancePair.alternate,
    );
    await expect(workflows.advanceWorkflowTerminalization(advanceOperation.operation)).resolves.toMatchObject({
      status: 'advanced',
      record: { phase: 'complete' },
    });
    expect(advanceOperation.reads()).toEqual({ workflowName: 1, runId: 1 });
    await expect(workflows.getWorkflowTerminalization(advancePair.alternate)).resolves.toMatchObject({
      status: 'found',
      record: { phase: 'terminalization_pending' },
    });

    const releasePair = await makePair('release');
    const releaseClaim = await claim(releasePair.intended, 'release-intended');
    const alternateReleaseClaim = await claim(releasePair.alternate, 'release-alternate');
    const releaseOperation = withStatefulIdentity(
      {
        ...releasePair.intended,
        ownerId: releaseClaim.ownerId,
        claimToken: releaseClaim.claimToken,
        claimGeneration: releaseClaim.claimGeneration,
      },
      releasePair.alternate,
    );
    await expect(workflows.releaseWorkflowTerminalization(releaseOperation.operation)).resolves.toMatchObject({
      status: 'released',
    });
    expect(releaseOperation.reads()).toEqual({ workflowName: 1, runId: 1 });
    expect(
      db.workflowTerminalizations.get(JSON.stringify([releasePair.intended.workflowName, releasePair.intended.runId]))
        ?.ownerId,
    ).toBeUndefined();
    expect(
      db.workflowTerminalizations.get(JSON.stringify([releasePair.alternate.workflowName, releasePair.alternate.runId]))
        ?.ownerId,
    ).toBe(alternateReleaseClaim.ownerId);

    const cleanupPair = await makePair('cleanup');
    await claim(cleanupPair.intended, 'cleanup-intended');
    await claim(cleanupPair.alternate, 'cleanup-alternate');
    for (const run of [cleanupPair.intended, cleanupPair.alternate]) {
      const key = JSON.stringify([run.workflowName, run.runId]);
      const record = db.workflowTerminalizations.get(key);
      if (!record) throw new Error('cleanup record missing');
      db.workflowTerminalizations.set(key, {
        ...record,
        phase: 'complete',
        ownerId: undefined,
        claimToken: undefined,
        leaseExpiresAt: undefined,
        completedAt: record.updatedAt,
      });
    }
    const cleanupOperation = withStatefulIdentity(
      { ...cleanupPair.intended, olderThan: new Date(Date.now() + 60_000) },
      cleanupPair.alternate,
    );
    await expect(workflows.deleteCompletedWorkflowTerminalizations(cleanupOperation.operation)).resolves.toEqual({
      status: 'deleted',
      count: 1,
    });
    expect(cleanupOperation.reads()).toEqual({ workflowName: 1, runId: 1 });
    await expect(workflows.getWorkflowTerminalization(cleanupPair.intended)).resolves.toEqual({
      status: 'missing_record',
    });
    await expect(workflows.getWorkflowTerminalization(cleanupPair.alternate)).resolves.toMatchObject({
      status: 'found',
      record: { phase: 'complete' },
    });
  });

  it('validates claim and fence envelopes before reporting a missing in-memory run', async () => {
    const workflows = new WorkflowsInMemory({ db: new InMemoryDB() });
    const missing = { workflowName: 'missing-workflow', runId: 'missing-run' };
    await expect(
      workflows.claimWorkflowTerminalization({
        ...missing,
        eventKey: 'event',
        terminalStatus: 'failed',
        ownerId: 'owner',
        leaseMs: 0,
      }),
    ).rejects.toThrow(TypeError);
    const invalidFence = {
      ...missing,
      ownerId: 'owner',
      claimToken: '',
      claimGeneration: 0,
    };
    await expect(
      workflows.advanceWorkflowTerminalization({
        ...invalidFence,
        expectedPhase: 'finish_effect_recorded',
        nextPhase: 'complete',
      }),
    ).rejects.toThrow(TypeError);
    await expect(workflows.releaseWorkflowTerminalization(invalidFence)).rejects.toThrow(TypeError);
    await expect(
      workflows.persistWorkflowTerminalState({
        ...invalidFence,
        snapshot: { ...createEmptyWorkflowSnapshot(missing.runId), status: 'failed' },
      }),
    ).rejects.toThrow(TypeError);
  });

  it('uses one first-terminal-wins slot before and after completion', async () => {
    const { db, workflows } = await setupWithDb();
    const claim = await acquire(workflows);
    await expect(
      workflows.claimWorkflowTerminalization({
        ...run,
        eventKey: 'competing-success',
        terminalStatus: 'success',
        ownerId: 'worker-b',
        leaseMs: 1_000,
      }),
    ).resolves.toMatchObject({ status: 'terminal_conflict', record: { terminalStatus: 'failed', eventKey } });

    await expect(completeAfterDownstreamEvidence(db, workflows, claim)).resolves.toMatchObject({
      status: 'advanced',
      record: { phase: 'complete' },
    });
    await expect(
      workflows.claimWorkflowTerminalization({
        ...run,
        eventKey: 'competing-success',
        terminalStatus: 'success',
        ownerId: 'worker-b',
        leaseMs: 1_000,
      }),
    ).resolves.toMatchObject({ status: 'terminal_conflict' });
  });

  it('isolates the journal from snapshot replacement and retains it after run deletion', async () => {
    const workflows = await setup();
    await acquire(workflows);
    await workflows.persistWorkflowSnapshot({
      ...run,
      snapshot: createEmptyWorkflowSnapshot(runId),
    });
    await expect(workflows.getWorkflowTerminalization(run)).resolves.toMatchObject({
      status: 'found',
      record: { eventKey, phase: 'terminalization_pending' },
    });

    await workflows.deleteWorkflowRunById(run);
    const observed = await workflows.getWorkflowTerminalization(run);
    expect(observed).toMatchObject({
      status: 'found',
      record: { eventKey, phase: 'terminalization_pending' },
    });
    if (observed.status !== 'found') throw new Error('Expected retained terminalization record');
    expect(observed.record).not.toHaveProperty('ownerId');
    expect(observed.record).not.toHaveProperty('claimToken');
    expect(observed.record).not.toHaveProperty('claimGeneration');
  });

  it('does not expose mutable aliases to stored journal records', async () => {
    const workflows = await setup();
    const claim = await acquire(workflows);
    const originalToken = claim.claimToken;
    const originalGeneration = claim.claimGeneration;

    claim.phase = 'complete';
    claim.claimToken = 'caller-mutated-token';
    claim.claimGeneration = 999;

    await expect(workflows.getWorkflowTerminalization(run)).resolves.toMatchObject({
      status: 'found',
      record: {
        phase: 'terminalization_pending',
      },
    });
    await expect(
      workflows.persistWorkflowTerminalState({
        ...run,
        ownerId,
        claimToken: originalToken!,
        claimGeneration: originalGeneration,
        snapshot: { ...createEmptyWorkflowSnapshot(runId), status: 'failed' },
      }),
    ).resolves.toMatchObject({ status: 'persisted' });
  });

  it('requires a live fence for release', async () => {
    const workflows = await setup();
    const claim = await acquire(workflows, { leaseMs: 1_000 });
    await expect(
      workflows.releaseWorkflowTerminalization({
        ...run,
        ownerId,
        claimToken: 'wrong-token',
        claimGeneration: claim.claimGeneration,
      }),
    ).resolves.toSatisfy(result => {
      expect(result).toMatchObject({ status: 'fence_conflict' });
      if (result.status !== 'fence_conflict') return false;
      expect(result.record).not.toHaveProperty('ownerId');
      expect(result.record).not.toHaveProperty('claimToken');
      expect(result.record).not.toHaveProperty('claimGeneration');
      return true;
    });
    vi.advanceTimersByTime(1_001);
    await expect(
      workflows.releaseWorkflowTerminalization({
        ...run,
        ownerId,
        claimToken: claim.claimToken!,
        claimGeneration: claim.claimGeneration,
      }),
    ).resolves.toMatchObject({ status: 'lease_expired' });
  });

  it('deletes only completed records older than the caller retention horizon', async () => {
    const { db, workflows } = await setupWithDb();
    const claim = await acquire(workflows);
    await completeAfterDownstreamEvidence(db, workflows, claim);

    await expect(workflows.deleteCompletedWorkflowTerminalizations({ ...run, olderThan: new Date() })).resolves.toEqual(
      { status: 'deleted', count: 0 },
    );
    vi.advanceTimersByTime(1);
    await expect(workflows.deleteCompletedWorkflowTerminalizations({ ...run, olderThan: new Date() })).resolves.toEqual(
      { status: 'deleted', count: 1 },
    );
  });

  it('returns explicit missing outcomes and validates bounded inputs', async () => {
    const store = new InMemoryStore();
    const workflows = (await store.getStore('workflows'))!;
    await expect(
      workflows.claimWorkflowTerminalization({
        ...run,
        eventKey,
        terminalStatus: 'failed',
        ownerId,
        leaseMs: 1_000,
      }),
    ).resolves.toEqual({ status: 'missing_run' });

    await workflows.persistWorkflowSnapshot({ ...run, snapshot: createEmptyWorkflowSnapshot(runId) });
    await expect(
      workflows.claimWorkflowTerminalization({
        ...run,
        eventKey,
        terminalStatus: 'failed',
        ownerId,
        leaseMs: 86_400_001,
      }),
    ).rejects.toThrow('leaseMs must be a positive safe integer no greater than 86400000');
    await expect(
      workflows.claimWorkflowTerminalization({
        ...run,
        eventKey,
        terminalStatus: 'failed',
        ownerId,
        claimToken: 'token-without-generation',
        leaseMs: 1_000,
      }),
    ).rejects.toThrow('claimToken and claimGeneration must be provided together');
    await expect(
      workflows.claimWorkflowTerminalization({
        ...run,
        eventKey,
        terminalStatus: 'unknown' as never,
        ownerId,
        leaseMs: 1_000,
      }),
    ).rejects.toThrow('terminalStatus must be success, failed, or canceled');

    expect(() =>
      claimWorkflowTerminalizationRecord(
        undefined,
        {
          ...run,
          eventKey,
          terminalStatus: 'failed',
          ownerId,
          leaseMs: 1_000,
        },
        Number.MAX_SAFE_INTEGER,
        'token',
      ),
    ).toThrow('terminalization lease expiry exceeds the safe integer range');
  });

  it('keeps workflow/run tuple identities distinct when names contain delimiters', async () => {
    const store = new InMemoryStore();
    const workflows = (await store.getStore('workflows'))!;
    const first = { workflowName: 'a-b', runId: 'c' };
    const second = { workflowName: 'a', runId: 'b-c' };
    await workflows.persistWorkflowSnapshot({
      ...first,
      snapshot: createEmptyWorkflowSnapshot('c'),
    });
    await workflows.persistWorkflowSnapshot({
      ...second,
      snapshot: createEmptyWorkflowSnapshot('b-c'),
    });

    const [firstClaim, secondClaim] = await Promise.all([
      workflows.claimWorkflowTerminalization({
        ...first,
        eventKey,
        terminalStatus: 'failed',
        ownerId: 'worker-first',
        leaseMs: 1_000,
      }),
      workflows.claimWorkflowTerminalization({
        ...second,
        eventKey: 'event-second',
        terminalStatus: 'failed',
        ownerId: 'worker-second',
        leaseMs: 1_000,
      }),
    ]);
    expect(firstClaim.status).toBe('acquired');
    expect(secondClaim.status).toBe('acquired');

    await workflows.deleteWorkflowRunById(first);
    await expect(workflows.loadWorkflowSnapshot(first)).resolves.toBeNull();
    await expect(workflows.loadWorkflowSnapshot(second)).resolves.toMatchObject({ runId: 'b-c' });
    await expect(workflows.getWorkflowTerminalization(first)).resolves.toMatchObject({
      status: 'found',
      record: { eventKey },
    });
    await expect(workflows.getWorkflowTerminalization(second)).resolves.toMatchObject({
      status: 'found',
      record: { eventKey: 'event-second' },
    });
  });
});
