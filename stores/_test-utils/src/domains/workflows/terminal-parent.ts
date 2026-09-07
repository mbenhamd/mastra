import {
  createEmptyWorkflowSnapshot,
  createWorkflowTerminalGraphFingerprint,
  createWorkflowTerminalParentContinuationContract,
} from '@mastra/core/storage';
import type { WorkflowsStorage } from '@mastra/core/storage';
import type { WorkflowRunState, WorkflowTerminalRecoveryAncestryV1 } from '@mastra/core/workflows';
import { expect } from 'vitest';

/** Shared adapter contract: a child result and its continuation receipt commit once. */
export async function expectWorkflowTerminalParentStorageContract(options: {
  primary: WorkflowsStorage;
  concurrent: WorkflowsStorage;
  workflowName: string;
}): Promise<void> {
  const { primary, concurrent, workflowName } = options;
  const parent = { workflowName, runId: 'terminal-parent' };
  const child = { workflowName, runId: 'terminal-child' };
  const now = Date.now();
  const parentSnapshot: WorkflowRunState = {
    ...createEmptyWorkflowSnapshot(parent.runId),
    status: 'running',
    context: {
      nested: {
        status: 'running',
        payload: { retained: true },
        startedAt: now,
        metadata: { nestedRunId: child.runId },
      },
    },
    serializedStepGraph: [
      { type: 'step', step: { id: 'nested', component: 'WORKFLOW' } },
      { type: 'sleep', id: 'after-child', duration: 1 },
    ],
    activePaths: [0],
    activeStepsPath: { nested: [0] },
    requestContext: { parent: true },
  };
  await primary.persistWorkflowSnapshot({ ...parent, snapshot: parentSnapshot });
  await primary.persistWorkflowSnapshot({ ...child, snapshot: createEmptyWorkflowSnapshot(child.runId) });
  const ancestry: WorkflowTerminalRecoveryAncestryV1 = [
    {
      version: 1,
      childWorkflowName: child.workflowName,
      childRunId: child.runId,
      parentWorkflowName: parent.workflowName,
      parentRunId: parent.runId,
      parentGraphFingerprint: createWorkflowTerminalGraphFingerprint(parentSnapshot.serializedStepGraph),
      source: { kind: 'step', stepId: 'nested', executionPath: [0] },
      inputPointer: { kind: 'parent-source-payload', stepId: 'nested' },
      resultPointer: { kind: 'retained-terminal-result', ...child },
      resumeMetadata: { wasResume: false, resumeSteps: [] },
    },
  ];
  await expect(primary.persistWorkflowTerminalRecoveryAncestry({ ...child, ancestry })).resolves.toMatchObject({
    status: 'persisted',
  });
  await expect(concurrent.persistWorkflowTerminalRecoveryAncestry({ ...child, ancestry })).resolves.toMatchObject({
    status: 'already_persisted',
  });
  const claim = await primary.claimWorkflowTerminalization({
    ...child,
    eventKey: 'terminal-parent-contract',
    terminalStatus: 'success',
    ownerId: 'parent-worker',
    leaseMs: 60_000,
  });
  if (claim.status !== 'acquired') throw new Error(`Expected child claim, received ${claim.status}`);
  const fence = {
    ...child,
    ownerId: claim.record.ownerId,
    claimToken: claim.record.claimToken,
    claimGeneration: claim.record.claimGeneration,
  };
  const terminalResult = { status: 'success' as const, output: { answer: 42 }, startedAt: now, endedAt: now + 1 };
  const snapshot: WorkflowRunState = {
    ...createEmptyWorkflowSnapshot(child.runId),
    status: 'success',
    result: terminalResult,
    value: { final: 'true' },
  };
  await expect(
    primary.persistWorkflowTerminalState({
      ...fence,
      snapshot,
      recoveryEnvelope: {
        version: 1,
        ...child,
        terminalStatus: 'success',
        executionMode: 'continuous',
        terminalResult,
        finalState: { final: true },
        requestContextPatch: { child: true },
        childGraphFingerprint: createWorkflowTerminalGraphFingerprint(snapshot.serializedStepGraph),
        ancestry,
      },
    }),
  ).resolves.toMatchObject({ status: 'persisted' });
  const prepared = await primary.prepareWorkflowTerminalEffect({
    ...fence,
    expectedPhase: 'run_state_persisted',
    effect: {
      kind: 'parent-workflow-step-end',
      parentWorkflowName: parent.workflowName,
      parentRunId: parent.runId,
      parentStepId: 'nested',
      parentExecutionPath: [0],
    },
  });
  if (prepared.status !== 'prepared') throw new Error(`Expected parent effect, received ${prepared.status}`);
  const context = await concurrent.getWorkflowTerminalParentContext(fence);
  if (context.status !== 'found') throw new Error(`Expected parent context, received ${context.status}`);
  let contract = createWorkflowTerminalParentContinuationContract({
    version: 1,
    terminalEffectKey: prepared.effect.effectKey,
    terminalEffectPayloadHash: prepared.effect.payloadHash,
    executionMode: 'continuous',
    expectedParentRevision: context.revision,
    graphFingerprint: createWorkflowTerminalGraphFingerprint(context.snapshot.serializedStepGraph),
    childTerminalStatus: 'success',
    observedParentStatus: 'running',
    source: { kind: 'step', stepId: 'nested', executionPath: [0] },
    action: {
      kind: 'run-entry',
      reason: 'next-step',
      target: { kind: 'entry', entryType: 'sleep', entryId: 'after-child', executionPath: [1] },
    },
    patch: {
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
    },
  });
  // A concurrent parent write invalidates a plan before it can apply any child output.
  await concurrent.persistWorkflowSnapshot({ ...parent, snapshot: parentSnapshot });
  await expect(primary.applyWorkflowTerminalParentEffect({ ...fence, contract })).resolves.toEqual({
    status: 'parent_conflict',
  });
  await expect(primary.loadWorkflowSnapshot(parent)).resolves.toMatchObject({
    context: { nested: { status: 'running', payload: { retained: true } } },
  });
  const refreshed = await primary.getWorkflowTerminalParentContext(fence);
  if (refreshed.status !== 'found') throw new Error(`Expected refreshed parent context, received ${refreshed.status}`);
  expect(refreshed.revision).not.toBe(context.revision);
  const { contractHash: _staleHash, ...staleSpec } = contract;
  contract = createWorkflowTerminalParentContinuationContract({
    ...staleSpec,
    expectedParentRevision: refreshed.revision,
  });
  const results = await Promise.all([
    primary.applyWorkflowTerminalParentEffect({ ...fence, contract }),
    concurrent.applyWorkflowTerminalParentEffect({ ...fence, contract }),
  ]);
  expect(results.map(result => result.status).sort()).toEqual(['already_applied', 'applied']);
  const applied = results.find(result => result.status === 'applied');
  if (!applied || applied.status !== 'applied') throw new Error('Expected one parent application');
  const after = await primary.getWorkflowTerminalParentContext(fence);
  if (after.status !== 'found') throw new Error(`Expected applied parent context, received ${after.status}`);
  expect(after.revision).not.toBe(refreshed.revision);
  expect(after.snapshot).toMatchObject({
    status: 'running',
    value: { final: true },
    context: {
      nested: { status: 'success', output: { answer: 42 }, payload: { retained: true } },
      __state: { final: true },
    },
    requestContext: { parent: true, child: true },
  });
  // Simulate a committed application whose response was lost: a fresh handle retries the original contract.
  await expect(concurrent.applyWorkflowTerminalParentEffect({ ...fence, contract })).resolves.toEqual({
    status: 'already_applied',
    plan: applied.plan,
  });
  await expect(concurrent.getWorkflowTerminalContinuationPlan(fence)).resolves.toMatchObject({
    status: 'found',
    plan: applied.plan,
    applicationState: 'applied',
    dispatchState: 'pending',
  });
  await expect(concurrent.getWorkflowTerminalization(child)).resolves.toMatchObject({
    status: 'found',
    record: { phase: 'parent_outbox_pending' },
  });
  const afterRetry = await concurrent.getWorkflowTerminalParentContext(fence);
  expect(afterRetry).toEqual(after);
  const { contractHash: _contractHash, ...spec } = contract;
  const conflictingContract = createWorkflowTerminalParentContinuationContract({
    ...spec,
    expectedParentRevision: after.revision,
  });
  await expect(
    concurrent.applyWorkflowTerminalParentEffect({ ...fence, contract: conflictingContract }),
  ).resolves.toMatchObject({ status: 'contract_conflict', plan: { contractHash: contract.contractHash } });
  await expect(primary.getWorkflowTerminalParentContext(fence)).resolves.toEqual(after);
}
