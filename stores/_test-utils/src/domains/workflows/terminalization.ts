import { createEmptyWorkflowSnapshot, createWorkflowTerminalGraphFingerprint } from '@mastra/core/storage';
import type {
  ClaimWorkflowTerminalizationResult,
  PersistWorkflowTerminalStateResult,
  PrepareWorkflowTerminalEffectResult,
  ReserveWorkflowTerminalDestinationReceiptResult,
  WorkflowsStorage,
} from '@mastra/core/storage';
import type { WorkflowRunState } from '@mastra/core/workflows';
import { randomUUID } from 'node:crypto';
import { expect } from 'vitest';

const CAPABILITIES = {
  journalVersion: 1,
  producerOutboxVersion: 1,
  destinationReceiptVersion: 1,
  parentApplicationVersion: 1,
  recoveryVersion: 1,
} as const;

function recoveryEnvelope(workflowName: string, runId: string, snapshot: WorkflowRunState) {
  return {
    version: 1 as const,
    workflowName,
    runId,
    terminalStatus: 'failed' as const,
    executionMode: 'continuous' as const,
    terminalResult: { status: 'failed' as const, error: { name: 'Error', message: 'contract failure' } },
    finalState: snapshot.value ?? {},
    requestContextPatch: snapshot.requestContext ?? {},
    childGraphFingerprint: createWorkflowTerminalGraphFingerprint(snapshot.serializedStepGraph),
    ancestry: [],
  };
}

function acquired(result: ClaimWorkflowTerminalizationResult) {
  if (result.status !== 'acquired') throw new Error(`Expected acquired claim, received ${result.status}`);
  return result.record;
}

function persisted(result: PersistWorkflowTerminalStateResult) {
  if (result.status !== 'persisted') throw new Error(`Expected persisted terminal state, received ${result.status}`);
  return result.record;
}

function prepared(result: PrepareWorkflowTerminalEffectResult) {
  if (result.status !== 'prepared' && result.status !== 'already_prepared') {
    throw new Error(`Expected prepared effect, received ${result.status}`);
  }
  return result.effect;
}

function reserved(result: ReserveWorkflowTerminalDestinationReceiptResult) {
  if (result.status !== 'reserved' && result.status !== 'already_exists') {
    throw new Error(`Expected reserved receipt, received ${result.status}`);
  }
  return result.receipt;
}

/** Runs the canonical terminalization journal protocol against two independent handles. */
export async function expectWorkflowTerminalStorageContract(options: {
  primary: WorkflowsStorage;
  concurrent: WorkflowsStorage;
  workflowName: string;
}): Promise<void> {
  const { primary, concurrent, workflowName } = options;
  expect(primary.getWorkflowTerminalizationCapabilities()).toEqual(CAPABILITIES);
  expect(concurrent.getWorkflowTerminalizationCapabilities()).toEqual(CAPABILITIES);
  expect(primary.supportsWorkflowTerminalizationJournal()).toBe(true);
  expect(concurrent.supportsWorkflowTerminalizationJournal()).toBe(true);

  const runId = `terminal-${randomUUID()}`;
  const run = { workflowName, runId };
  await expect(primary.getWorkflowTerminalization(run)).resolves.toEqual({ status: 'missing_run' });
  await expect(concurrent.getWorkflowRunTerminalStatus(run)).resolves.toEqual({ status: 'missing_run' });
  const initial = createEmptyWorkflowSnapshot(runId);
  await primary.persistWorkflowSnapshot({ ...run, snapshot: initial });

  await expect(concurrent.getWorkflowTerminalization(run)).resolves.toEqual({ status: 'missing_record' });
  await expect(concurrent.getWorkflowRunTerminalStatus(run)).resolves.toEqual({ status: 'nonterminal' });

  const claimInput = {
    ...run,
    eventKey: `event-${randomUUID()}`,
    terminalStatus: 'failed' as const,
    leaseMs: 60_000,
  };
  const [left, right] = await Promise.all([
    primary.claimWorkflowTerminalization({ ...claimInput, ownerId: 'contract-primary' }),
    concurrent.claimWorkflowTerminalization({ ...claimInput, ownerId: 'contract-concurrent' }),
  ]);
  expect([left.status, right.status].sort()).toEqual(['acquired', 'leased']);
  const liveClaim = acquired(left.status === 'acquired' ? left : right);
  const lease = left.status === 'leased' ? left : right;
  if (lease.status !== 'leased') throw new Error(`Expected leased claim, received ${lease.status}`);
  expect(lease.record).not.toHaveProperty('ownerId');
  expect(lease.record).not.toHaveProperty('claimToken');
  expect(lease.record).not.toHaveProperty('claimGeneration');

  await expect(
    concurrent.claimWorkflowTerminalization({
      ...claimInput,
      ownerId: 'different-terminal-owner',
      terminalStatus: 'success',
    }),
  ).resolves.toMatchObject({ status: 'terminal_conflict', record: { terminalStatus: 'failed' } });

  const fence = {
    ...run,
    ownerId: liveClaim.ownerId,
    claimToken: liveClaim.claimToken,
    claimGeneration: liveClaim.claimGeneration,
  };
  const terminalSnapshot = { ...initial, status: 'failed' as const, value: { contract: 'terminal' } };

  await expect(
    primary.advanceWorkflowTerminalization({
      ...fence,
      expectedPhase: 'terminalization_pending',
      nextPhase: 'finish_outbox_pending',
    }),
  ).resolves.toEqual({ status: 'invalid_transition' });

  const retained = persisted(
    await primary.persistWorkflowTerminalState({
      ...fence,
      snapshot: terminalSnapshot,
      recoveryEnvelope: recoveryEnvelope(workflowName, runId, terminalSnapshot),
    }),
  );
  expect(retained.phase).toBe('run_state_persisted');
  await expect(concurrent.getWorkflowRunTerminalStatus(run)).resolves.toEqual({
    status: 'terminal',
    terminalStatus: 'failed',
  });
  await expect(concurrent.loadWorkflowSnapshot(run)).resolves.toMatchObject(terminalSnapshot);

  await expect(
    concurrent.persistWorkflowTerminalState({
      ...fence,
      snapshot: terminalSnapshot,
      recoveryEnvelope: recoveryEnvelope(workflowName, runId, terminalSnapshot),
    }),
  ).resolves.toMatchObject({ status: 'phase_conflict', record: { phase: 'run_state_persisted' } });

  const effectResults = await Promise.all([
    primary.prepareWorkflowTerminalEffect({
      ...fence,
      expectedPhase: 'run_state_persisted',
      effect: { kind: 'workflow-finish' },
    }),
    concurrent.prepareWorkflowTerminalEffect({
      ...fence,
      expectedPhase: 'run_state_persisted',
      effect: { kind: 'workflow-finish' },
    }),
  ]);
  expect(effectResults.map(result => result.status).sort()).toEqual(['already_prepared', 'prepared']);
  const effect = prepared(effectResults.find(result => result.status === 'prepared') ?? effectResults[0]!);
  const concurrentEffect = prepared(
    effectResults.find(result => result.status === 'already_prepared') ?? effectResults[1]!,
  );
  expect(concurrentEffect).toEqual(effect);
  const retryEffect = prepared(
    await concurrent.prepareWorkflowTerminalEffect({
      ...fence,
      expectedPhase: 'run_state_persisted',
      effect: { kind: 'workflow-finish' },
    }),
  );
  expect(retryEffect).toEqual(effect);
  expect(retryEffect.effectKey).toBe(effect.effectKey);
  expect(retryEffect.payloadHash).toBe(effect.payloadHash);

  const receiptInput = { ...fence, effectKind: 'workflow-finish' as const, consumerId: 'contract-consumer' };
  const [receiptLeft, receiptRight] = await Promise.all([
    primary.reserveWorkflowTerminalDestinationReceipt(receiptInput),
    concurrent.reserveWorkflowTerminalDestinationReceipt(receiptInput),
  ]);
  expect([receiptLeft.status, receiptRight.status].sort()).toEqual(['already_exists', 'reserved']);
  const receipt = reserved(receiptLeft.status === 'reserved' ? receiptLeft : receiptRight);
  const duplicate = reserved(receiptLeft.status === 'already_exists' ? receiptLeft : receiptRight);
  expect(duplicate).toEqual(receipt);
  expect(receipt.effectKey).toBe(effect.effectKey);
  expect(receipt).toMatchObject({ applicationState: 'reserved', dispatchState: 'none' });
  await expect(concurrent.getWorkflowTerminalization(run)).resolves.toMatchObject({
    status: 'found',
    record: { phase: 'finish_outbox_pending' },
  });

  await expect(primary.releaseWorkflowTerminalization(fence)).resolves.toMatchObject({ status: 'released' });
  const reacquired = acquired(
    await concurrent.claimWorkflowTerminalization({ ...claimInput, ownerId: liveClaim.ownerId }),
  );
  expect(reacquired.claimGeneration).toBeGreaterThan(liveClaim.claimGeneration);
  expect(reacquired.claimToken).not.toBe(liveClaim.claimToken);

  const newFence = { ...run, ...reacquired };
  const beforeStale = await concurrent.getWorkflowTerminalization(run);
  await expect(
    primary.persistWorkflowTerminalState({
      ...fence,
      snapshot: terminalSnapshot,
      recoveryEnvelope: recoveryEnvelope(workflowName, runId, terminalSnapshot),
    }),
  ).resolves.toMatchObject({ status: 'fence_conflict' });

  await expect(
    primary.getWorkflowTerminalEffectForDispatch({ ...fence, kind: 'workflow-finish' }),
  ).resolves.toMatchObject({ status: 'fence_conflict' });
  await expect(primary.reserveWorkflowTerminalDestinationReceipt(receiptInput)).resolves.toMatchObject({
    status: 'fence_conflict',
  });
  await expect(
    concurrent.getWorkflowTerminalEffectForDispatch({ ...newFence, kind: 'workflow-finish' }),
  ).resolves.toMatchObject({
    status: 'found',
    effect: { effectKey: effect.effectKey, payloadHash: effect.payloadHash },
  });
  await expect(
    concurrent.getWorkflowTerminalDestinationReceipt({
      ...newFence,
      effectKind: 'workflow-finish',
      consumerId: 'contract-consumer',
    }),
  ).resolves.toMatchObject({ status: 'found', receipt });
  await expect(concurrent.getWorkflowTerminalization(run)).resolves.toEqual(beforeStale);
  // Reserving a destination is not proof that it applied the effect. Generic phase CAS cannot certify delivery.
  for (const nextPhase of ['finish_effect_recorded', 'complete'] as const) {
    await expect(
      concurrent.advanceWorkflowTerminalization({
        ...run,
        ...reacquired,
        expectedPhase: 'finish_outbox_pending',
        nextPhase,
      }),
    ).resolves.toEqual({ status: 'invalid_transition' });
  }
  await expect(
    concurrent.deleteCompletedWorkflowTerminalizations({ ...run, olderThan: new Date(Date.now() + 1) }),
  ).resolves.toEqual({ status: 'deleted', count: 0 });
  await expect(concurrent.getWorkflowTerminalization(run)).resolves.toEqual(beforeStale);
  await expect(
    concurrent.getWorkflowTerminalEffectForDispatch({ ...run, ...reacquired, kind: 'workflow-finish' }),
  ).resolves.toMatchObject({ status: 'found', effect, recovery: { envelope: { terminalStatus: 'failed' } } });
}
