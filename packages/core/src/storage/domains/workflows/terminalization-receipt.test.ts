import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { WorkflowRunState, WorkflowTerminalDestinationReceiptRecord } from '../../../workflows';
import { createWorkflowTerminalGraphFingerprint } from '../../../workflows/terminal-continuation';
import type {
  GetWorkflowTerminalDestinationReceiptInput,
  ReserveWorkflowTerminalDestinationReceiptInput,
} from '../../types';
import { createEmptyWorkflowSnapshot } from '../../workflow-snapshot';
import { InMemoryDB } from '../inmemory-db';
import { WorkflowsInMemory } from './inmemory';
import {
  MAX_WORKFLOW_TERMINAL_DESTINATION_RECEIPTS_PER_EFFECT,
  createWorkflowTerminalDestinationReceiptRecord,
  getWorkflowTerminalDestinationReceiptRecord,
  validateWorkflowTerminalDestinationReceiptIntegrity,
} from './terminalization';
import { createTerminalRecoveryEnvelope } from './terminalization-test-utils';

describe('WorkflowsInMemory terminal destination receipts', () => {
  const now = new Date('2026-01-01T00:00:00.000Z');

  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(now);
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  function runKey(run: { workflowName: string; runId: string }) {
    return JSON.stringify([run.workflowName, run.runId]);
  }

  async function createReadyRun(
    workflows: WorkflowsInMemory,
    run: { workflowName: string; runId: string },
    effect:
      | { kind: 'workflow-finish' }
      | {
          kind: 'parent-workflow-step-end';
          parentWorkflowName: string;
          parentRunId: string;
          parentStepId: string;
          parentExecutionPath: number[];
        } = { kind: 'workflow-finish' },
  ) {
    await workflows.persistWorkflowSnapshot({ ...run, snapshot: createEmptyWorkflowSnapshot(run.runId) });
    const claim = await workflows.claimWorkflowTerminalization({
      ...run,
      eventKey: `${run.runId}-event`,
      terminalStatus: 'failed',
      ownerId: `${run.runId}-owner`,
      leaseMs: 10_000,
    });
    if (claim.status !== 'acquired') throw new Error(`Expected acquired, received ${claim.status}`);
    const fence = {
      ...run,
      ownerId: claim.record.ownerId,
      claimToken: claim.record.claimToken,
      claimGeneration: claim.record.claimGeneration,
    };
    const snapshot = { ...createEmptyWorkflowSnapshot(run.runId), status: 'failed' as const };
    const parentGraph: WorkflowRunState['serializedStepGraph'] =
      effect.kind === 'parent-workflow-step-end'
        ? Array.from({ length: effect.parentExecutionPath[0]! + 1 }, (_, rootIndex) =>
            rootIndex === effect.parentExecutionPath[0]
              ? effect.parentExecutionPath.length === 1
                ? { type: 'step' as const, step: { id: effect.parentStepId, component: 'WORKFLOW' } }
                : {
                    type: 'parallel' as const,
                    steps: Array.from({ length: effect.parentExecutionPath[1]! + 1 }, (_, branchIndex) => ({
                      type: 'step' as const,
                      step: {
                        id:
                          branchIndex === effect.parentExecutionPath[1]
                            ? effect.parentStepId
                            : `filler-${rootIndex}-${branchIndex}`,
                        component: branchIndex === effect.parentExecutionPath[1] ? 'WORKFLOW' : 'STEP',
                      },
                    })),
                  }
              : { type: 'sleep' as const, id: `filler-${rootIndex}`, duration: 1 },
          )
        : [];
    const ancestry =
      effect.kind === 'parent-workflow-step-end'
        ? [
            {
              version: 1 as const,
              childWorkflowName: run.workflowName,
              childRunId: run.runId,
              parentWorkflowName: effect.parentWorkflowName,
              parentRunId: effect.parentRunId,
              parentGraphFingerprint: createWorkflowTerminalGraphFingerprint(parentGraph),
              source: {
                kind: 'step' as const,
                stepId: effect.parentStepId,
                executionPath: effect.parentExecutionPath,
              },
              inputPointer: { kind: 'parent-source-payload' as const, stepId: effect.parentStepId },
              resultPointer: {
                kind: 'retained-terminal-result' as const,
                workflowName: run.workflowName,
                runId: run.runId,
              },
              resumeMetadata: { wasResume: false, resumeSteps: [] },
            },
          ]
        : [];
    if (ancestry.length > 0) {
      const immediate = ancestry[0]!;
      await workflows.persistWorkflowSnapshot({
        workflowName: immediate.parentWorkflowName,
        runId: immediate.parentRunId,
        snapshot: { ...createEmptyWorkflowSnapshot(immediate.parentRunId), serializedStepGraph: parentGraph },
      });
      await workflows.persistWorkflowTerminalRecoveryAncestry({ ...run, ancestry });
    }
    await workflows.persistWorkflowTerminalState({
      ...fence,
      snapshot,
      recoveryEnvelope: createTerminalRecoveryEnvelope({ ...run, snapshot, terminalStatus: 'failed', ancestry }),
    });
    const prepared = await workflows.prepareWorkflowTerminalEffect({
      ...fence,
      expectedPhase: 'run_state_persisted',
      effect,
    });
    if (prepared.status !== 'prepared') throw new Error(`Expected prepared, received ${prepared.status}`);
    return { run, fence, effect: prepared.effect };
  }

  function receiptInput(
    ready: Awaited<ReturnType<typeof createReadyRun>>,
    consumerId = 'terminal-consumer',
  ): ReserveWorkflowTerminalDestinationReceiptInput {
    return { ...ready.fence, effectKind: ready.effect.kind, consumerId };
  }

  function withStatefulEnvelope<T extends ReserveWorkflowTerminalDestinationReceiptInput>(
    operation: T,
    alternate: T,
  ): { operation: T; reads: () => Record<keyof T, number> } {
    const fields = [
      'workflowName',
      'runId',
      'ownerId',
      'claimToken',
      'claimGeneration',
      'effectKind',
      'consumerId',
    ] as const;
    const reads = Object.fromEntries(fields.map(field => [field, 0])) as Record<keyof T, number>;
    for (const field of fields) {
      const intended = operation[field];
      Object.defineProperty(operation, field, {
        enumerable: true,
        get: () => (reads[field]++ === 0 ? intended : alternate[field]),
      });
    }
    return { operation, reads: () => ({ ...reads }) };
  }

  it('returns one stable receipt for concurrent same-consumer reservations', async () => {
    const workflows = new WorkflowsInMemory({ db: new InMemoryDB() });
    const ready = await createReadyRun(workflows, { workflowName: 'concurrent-receipt', runId: 'concurrent-run' });

    const [first, second] = await Promise.all([
      workflows.reserveWorkflowTerminalDestinationReceipt(receiptInput(ready, 'same-consumer')),
      workflows.reserveWorkflowTerminalDestinationReceipt(receiptInput(ready, 'same-consumer')),
    ]);

    expect(first.status).toBe('reserved');
    expect(second.status).toBe('already_exists');
    if (first.status !== 'reserved' || second.status !== 'already_exists') {
      throw new Error('Expected one stable receipt');
    }
    expect(second.receipt).toEqual(first.receipt);
  });

  it('atomically caps distinct consumers per effect while preserving idempotent retries', async () => {
    const db = new InMemoryDB();
    const workflows = new WorkflowsInMemory({ db });
    const ready = await createReadyRun(workflows, { workflowName: 'bounded-receipts', runId: 'bounded-run' });

    for (let index = 0; index < MAX_WORKFLOW_TERMINAL_DESTINATION_RECEIPTS_PER_EFFECT - 1; index += 1) {
      await expect(
        workflows.reserveWorkflowTerminalDestinationReceipt(receiptInput(ready, `consumer-${index}`)),
      ).resolves.toMatchObject({ status: 'reserved' });
    }

    const boundary = await Promise.all([
      workflows.reserveWorkflowTerminalDestinationReceipt(receiptInput(ready, 'boundary-a')),
      workflows.reserveWorkflowTerminalDestinationReceipt(receiptInput(ready, 'boundary-b')),
    ]);
    expect(boundary.map(result => result.status).sort()).toEqual(['consumer_limit_reached', 'reserved']);
    expect(db.workflowTerminalDestinationReceipts.size).toBe(MAX_WORKFLOW_TERMINAL_DESTINATION_RECEIPTS_PER_EFFECT);

    const winner = boundary.find(result => result.status === 'reserved');
    if (!winner || winner.status !== 'reserved') throw new Error('Expected one boundary reservation');
    await expect(
      workflows.reserveWorkflowTerminalDestinationReceipt(receiptInput(ready, winner.receipt.consumerId)),
    ).resolves.toMatchObject({ status: 'already_exists', receipt: { receiptKey: winner.receipt.receiptKey } });
    await expect(
      workflows.reserveWorkflowTerminalDestinationReceipt(receiptInput(ready, 'over-limit')),
    ).resolves.toEqual({ status: 'consumer_limit_reached' });
  });

  it('reserves isolated receipts, retains them after run deletion, and removes them with completed evidence', async () => {
    const db = new InMemoryDB();
    const workflows = new WorkflowsInMemory({ db });
    const ready = await createReadyRun(workflows, { workflowName: 'receipt-workflow', runId: 'receipt-run' });

    expect(workflows.getWorkflowTerminalizationCapabilities()).toEqual({
      journalVersion: 1,
      producerOutboxVersion: 1,
      destinationReceiptVersion: 1,
      parentApplicationVersion: 1,
      recoveryVersion: 1,
    });
    const first = await workflows.reserveWorkflowTerminalDestinationReceipt(receiptInput(ready, 'finish-dispatcher'));
    expect(first).toMatchObject({
      status: 'reserved',
      receipt: {
        version: 1,
        effectKind: 'workflow-finish',
        consumerId: 'finish-dispatcher',
        applicationState: 'reserved',
        dispatchState: 'none',
      },
    });
    if (first.status !== 'reserved') throw new Error('Expected reserved receipt');
    expect(first.receipt.receiptKey).toMatch(/^wtr:v1:[a-f0-9]{64}$/);
    expect(first.receipt.destinationHash).toMatch(/^sha256:[a-f0-9]{64}$/);
    await expect(
      workflows.reserveWorkflowTerminalDestinationReceipt(receiptInput(ready, 'finish-dispatcher')),
    ).resolves.toEqual({ status: 'already_exists', receipt: first.receipt });
    const second = await workflows.reserveWorkflowTerminalDestinationReceipt(receiptInput(ready, 'audit-projection'));
    expect(second).toMatchObject({ status: 'reserved', receipt: { consumerId: 'audit-projection' } });
    if (second.status !== 'reserved') throw new Error('Expected independent receipt');
    expect(second.receipt.receiptKey).not.toBe(first.receipt.receiptKey);

    first.receipt.receiptKey = 'caller-mutated';
    await workflows.deleteWorkflowRunById(ready.run);
    await expect(
      workflows.getWorkflowTerminalDestinationReceipt(receiptInput(ready, 'finish-dispatcher')),
    ).resolves.toMatchObject({
      status: 'found',
      receipt: { receiptKey: expect.stringMatching(/^wtr:v1:[a-f0-9]{64}$/) },
    });

    const journalKey = runKey(ready.run);
    const journal = db.workflowTerminalizations.get(journalKey);
    if (!journal) throw new Error('Expected terminalization journal');
    db.workflowTerminalizations.set(journalKey, {
      ...journal,
      phase: 'complete',
      ownerId: undefined,
      claimToken: undefined,
      leaseExpiresAt: undefined,
      completedAt: journal.updatedAt,
    });
    vi.advanceTimersByTime(1);
    let workflowNameReads = 0;
    let runIdReads = 0;
    const cleanupInput = {
      get workflowName() {
        return workflowNameReads++ === 0 ? ready.run.workflowName : 'alternate-workflow';
      },
      get runId() {
        return runIdReads++ === 0 ? ready.run.runId : 'alternate-run';
      },
      olderThan: new Date(),
    };
    await expect(workflows.deleteCompletedWorkflowTerminalizations(cleanupInput)).resolves.toEqual({
      status: 'deleted',
      count: 1,
    });
    expect({ workflowNameReads, runIdReads }).toEqual({ workflowNameReads: 1, runIdReads: 1 });
    expect(db.workflowTerminalDestinationReceipts.size).toBe(0);
  });

  it('materializes every reserve and get operation field once', async () => {
    const workflows = new WorkflowsInMemory({ db: new InMemoryDB() });
    const intended = await createReadyRun(workflows, { workflowName: 'intended-workflow', runId: 'intended-run' });
    const alternate = await createReadyRun(workflows, { workflowName: 'alternate-workflow', runId: 'alternate-run' });
    const intendedInput = receiptInput(intended, 'intended-consumer');
    const alternateInput = receiptInput(alternate, 'alternate-consumer');

    const reserve = withStatefulEnvelope({ ...intendedInput }, alternateInput);
    await expect(workflows.reserveWorkflowTerminalDestinationReceipt(reserve.operation)).resolves.toMatchObject({
      status: 'reserved',
      receipt: { workflowName: intended.run.workflowName, runId: intended.run.runId, consumerId: 'intended-consumer' },
    });
    expect(reserve.reads()).toEqual({
      workflowName: 1,
      runId: 1,
      ownerId: 1,
      claimToken: 1,
      claimGeneration: 1,
      effectKind: 1,
      consumerId: 1,
    });

    const get = withStatefulEnvelope({ ...intendedInput }, alternateInput);
    await expect(
      workflows.getWorkflowTerminalDestinationReceipt(get.operation as GetWorkflowTerminalDestinationReceiptInput),
    ).resolves.toMatchObject({
      status: 'found',
      receipt: { workflowName: intended.run.workflowName, runId: intended.run.runId, consumerId: 'intended-consumer' },
    });
    expect(get.reads()).toEqual({
      workflowName: 1,
      runId: 1,
      ownerId: 1,
      claimToken: 1,
      claimGeneration: 1,
      effectKind: 1,
      consumerId: 1,
    });
  });

  it('validates kinds, consumers, and fences before a missing-run lookup', async () => {
    const workflows = new WorkflowsInMemory({ db: new InMemoryDB() });
    const missing = {
      workflowName: 'missing-workflow',
      runId: 'missing-run',
      ownerId: 'owner',
      claimToken: 'token',
      claimGeneration: 1,
      effectKind: 'workflow-finish' as const,
      consumerId: 'consumer',
    };
    await expect(
      workflows.reserveWorkflowTerminalDestinationReceipt({ ...missing, effectKind: 'invalid' as never }),
    ).rejects.toThrow('kind must be parent-workflow-step-end or workflow-finish');
    for (const consumerId of ['', 'x'.repeat(257), `bad${String.fromCharCode(0xd800)}`]) {
      await expect(workflows.reserveWorkflowTerminalDestinationReceipt({ ...missing, consumerId })).rejects.toThrow(
        'consumerId must be a well-formed non-empty string',
      );
    }
    await expect(workflows.getWorkflowTerminalDestinationReceipt({ ...missing, claimToken: '' })).rejects.toThrow(
      'claimToken must be a well-formed non-empty string',
    );
    await expect(
      workflows.reserveWorkflowTerminalDestinationReceipt({
        ...missing,
        workflowName: 'w'.repeat(513),
      }),
    ).rejects.toThrow('workflowName must be a well-formed non-empty string no longer than 512 characters');
    await expect(
      workflows.getWorkflowTerminalDestinationReceipt({
        ...missing,
        runId: `run${String.fromCharCode(0xd800)}`,
      }),
    ).rejects.toThrow('runId must be a well-formed non-empty string no longer than 512 characters');
  });

  it('fails closed on corrupt present evidence before a stale fence but lets the fence mask missing evidence', async () => {
    const db = new InMemoryDB();
    const workflows = new WorkflowsInMemory({ db });
    const ready = await createReadyRun(workflows, { workflowName: 'corrupt-workflow', runId: 'corrupt-run' });
    const input = receiptInput(ready);
    const reserved = await workflows.reserveWorkflowTerminalDestinationReceipt(input);
    if (reserved.status !== 'reserved') throw new Error('Expected reserved receipt');
    const stale = { ...input, claimToken: 'stale-token' };

    const effectKey = JSON.stringify([ready.run.workflowName, ready.run.runId, ready.effect.kind]);
    const storedEffect = db.workflowTerminalEffects.get(effectKey);
    if (!storedEffect) throw new Error('Expected effect');
    db.workflowTerminalEffects.set(effectKey, { ...storedEffect, payloadHash: `sha256:${'0'.repeat(64)}` });
    await expect(workflows.getWorkflowTerminalDestinationReceipt(stale)).rejects.toThrow(
      'Invalid workflow terminal effect integrity',
    );
    db.workflowTerminalEffects.set(effectKey, storedEffect);
    db.workflowTerminalEffects.set(effectKey, { ...storedEffect, createdAt: Number.NaN });
    await expect(workflows.getWorkflowTerminalDestinationReceipt(stale)).rejects.toThrow(
      'Invalid workflow terminal effect journal link',
    );
    db.workflowTerminalEffects.set(effectKey, storedEffect);

    const retained = db.workflowTerminalSnapshots.get(runKey(ready.run));
    if (!retained) throw new Error('Expected retained state');
    db.workflowTerminalSnapshots.set(runKey(ready.run), { ...retained, createdAt: Number.NaN });
    await expect(workflows.getWorkflowTerminalDestinationReceipt(stale)).resolves.toMatchObject({
      status: 'fence_conflict',
    });
    await expect(workflows.getWorkflowTerminalDestinationReceipt(input)).rejects.toThrow(
      'Invalid workflow terminal snapshot journal link',
    );
    db.workflowTerminalSnapshots.set(runKey(ready.run), retained);

    const storedReceipt = [...db.workflowTerminalDestinationReceipts.values()].find(
      receipt => receipt.receiptKey === reserved.receipt.receiptKey,
    );
    if (!storedReceipt) throw new Error('Expected receipt');
    const journal = db.workflowTerminalizations.get(runKey(ready.run));
    if (!journal) throw new Error('Expected journal');
    expect(() =>
      getWorkflowTerminalDestinationReceiptRecord(
        journal,
        storedEffect,
        createWorkflowTerminalDestinationReceiptRecord(storedEffect, 'other-consumer', Date.now()),
        input,
        Date.now(),
      ),
    ).toThrow('Conflicting workflow terminal destination receipt identity');
    const physicalReceiptKey = JSON.stringify([storedReceipt.effectKey, storedReceipt.consumerId]);
    db.workflowTerminalDestinationReceipts.set(physicalReceiptKey, {
      ...storedReceipt,
      destinationHash: `sha256:${'0'.repeat(64)}`,
    });
    await expect(workflows.getWorkflowTerminalDestinationReceipt(stale)).rejects.toThrow(
      'Invalid workflow terminal destination receipt integrity',
    );
    db.workflowTerminalDestinationReceipts.set(physicalReceiptKey, {
      ...storedReceipt,
      receiptKey: `wtr:v1:${'0'.repeat(64)}`,
      workflowName: 'corrupt-workflow-name',
      runId: 'corrupt-run-id',
      effectKind: 'corrupt-kind' as never,
    });
    await expect(workflows.getWorkflowTerminalDestinationReceipt(stale)).rejects.toThrow(
      'Invalid workflow terminal destination receipt integrity',
    );
    db.workflowTerminalDestinationReceipts.set(
      physicalReceiptKey,
      createWorkflowTerminalDestinationReceiptRecord(storedEffect, 'other-consumer', Date.now()),
    );
    await expect(workflows.getWorkflowTerminalDestinationReceipt(stale)).rejects.toThrow(
      'Conflicting workflow terminal destination receipt storage',
    );
    db.workflowTerminalDestinationReceipts.delete(physicalReceiptKey);
    const aliasedReceipt = { ...storedReceipt, effectKey: `wte:v1:${'0'.repeat(64)}` };
    db.workflowTerminalDestinationReceipts.set('corrupt-logical-alias', aliasedReceipt);
    await expect(workflows.getWorkflowTerminalDestinationReceipt(input)).rejects.toThrow(
      'Invalid workflow terminal destination receipt integrity',
    );
    await expect(workflows.reserveWorkflowTerminalDestinationReceipt(input)).rejects.toThrow(
      'Invalid workflow terminal destination receipt integrity',
    );
    db.workflowTerminalDestinationReceipts.delete('corrupt-logical-alias');
    await expect(workflows.getWorkflowTerminalDestinationReceipt(stale)).resolves.toMatchObject({
      status: 'fence_conflict',
    });
    db.workflowTerminalEffects.delete(effectKey);
    await expect(workflows.reserveWorkflowTerminalDestinationReceipt(stale)).resolves.toMatchObject({
      status: 'fence_conflict',
    });
  });

  it('requires retained terminal state before insertion', async () => {
    const db = new InMemoryDB();
    const workflows = new WorkflowsInMemory({ db });
    const ready = await createReadyRun(workflows, { workflowName: 'missing-state', runId: 'missing-state-run' });
    db.workflowTerminalSnapshots.delete(runKey(ready.run));

    await expect(workflows.reserveWorkflowTerminalDestinationReceipt(receiptInput(ready))).resolves.toEqual({
      status: 'missing_terminal_state',
    });
    expect(db.workflowTerminalDestinationReceipts.size).toBe(0);
  });

  it('validates the closed state matrix and rejects impossible timestamps', async () => {
    const workflows = new WorkflowsInMemory({ db: new InMemoryDB() });
    const ready = await createReadyRun(workflows, { workflowName: 'matrix-workflow', runId: 'matrix-run' });
    const reserved = createWorkflowTerminalDestinationReceiptRecord(ready.effect, 'matrix-consumer', Date.now());
    const legal: WorkflowTerminalDestinationReceiptRecord[] = [
      reserved,
      { ...reserved, applicationState: 'applied', dispatchState: 'none', appliedAt: reserved.createdAt },
      {
        ...reserved,
        applicationState: 'applied',
        dispatchState: 'pending',
        appliedAt: reserved.createdAt,
        dispatchPendingAt: reserved.createdAt,
      },
      {
        ...reserved,
        applicationState: 'applied',
        dispatchState: 'destination_applied',
        appliedAt: reserved.createdAt,
        dispatchPendingAt: reserved.createdAt,
        destinationAppliedAt: reserved.createdAt,
      },
      { ...reserved, applicationState: 'quarantined', dispatchState: 'none', quarantinedAt: reserved.createdAt },
    ];
    for (const receipt of legal) {
      expect(() =>
        validateWorkflowTerminalDestinationReceiptIntegrity(receipt, ready.effect, Date.now()),
      ).not.toThrow();
    }
    for (const receipt of [
      { ...reserved, createdAt: Number.NaN },
      { ...reserved, createdAt: '0' },
      { ...reserved, applicationState: 'reserved', dispatchState: 'pending' },
      { ...reserved, applicationState: 'applied', dispatchState: 'none' },
      {
        ...reserved,
        applicationState: 'applied',
        dispatchState: 'pending',
        appliedAt: reserved.createdAt - 1,
        dispatchPendingAt: reserved.createdAt,
      },
    ]) {
      expect(() =>
        validateWorkflowTerminalDestinationReceiptIntegrity(receipt as never, ready.effect, Date.now()),
      ).toThrow('Invalid workflow terminal destination receipt integrity');
    }
  });

  it('binds parent destination paths without advancing application phases', async () => {
    const workflows = new WorkflowsInMemory({ db: new InMemoryDB() });
    const first = await createReadyRun(
      workflows,
      { workflowName: 'parent-child-a', runId: 'parent-child-run-a' },
      {
        kind: 'parent-workflow-step-end',
        parentWorkflowName: 'parent',
        parentRunId: 'parent-run',
        parentStepId: 'nested',
        parentExecutionPath: [1, 3],
      },
    );
    const second = await createReadyRun(
      workflows,
      { workflowName: 'parent-child-b', runId: 'parent-child-run-b' },
      {
        kind: 'parent-workflow-step-end',
        parentWorkflowName: 'parent',
        parentRunId: 'parent-run',
        parentStepId: 'nested',
        parentExecutionPath: [1, 4],
      },
    );
    const firstReceipt = await workflows.reserveWorkflowTerminalDestinationReceipt(
      receiptInput(first, 'parent-application'),
    );
    const secondReceipt = await workflows.reserveWorkflowTerminalDestinationReceipt(
      receiptInput(second, 'parent-application'),
    );
    if (firstReceipt.status !== 'reserved' || secondReceipt.status !== 'reserved') {
      throw new Error('Expected parent receipts');
    }
    expect(firstReceipt.receipt.destinationHash).not.toBe(secondReceipt.receipt.destinationHash);
    await expect(workflows.getWorkflowTerminalization(first.run)).resolves.toMatchObject({
      status: 'found',
      record: { phase: 'parent_outbox_pending' },
    });
  });

  it('removes receipts only with completed evidence cleanup and global clear', async () => {
    const db = new InMemoryDB();
    const workflows = new WorkflowsInMemory({ db });
    const ready = await createReadyRun(workflows, { workflowName: 'cleanup-receipt', runId: 'cleanup-receipt-run' });
    await workflows.reserveWorkflowTerminalDestinationReceipt(receiptInput(ready));
    const key = runKey(ready.run);
    const journal = db.workflowTerminalizations.get(key);
    if (!journal) throw new Error('Expected journal');
    db.workflowTerminalizations.set(key, { ...journal, phase: 'finish_effect_recorded' });
    await workflows.advanceWorkflowTerminalization({
      ...ready.fence,
      expectedPhase: 'finish_effect_recorded',
      nextPhase: 'complete',
    });
    vi.advanceTimersByTime(1);
    await expect(
      workflows.deleteCompletedWorkflowTerminalizations({ ...ready.run, olderThan: new Date() }),
    ).resolves.toEqual({ status: 'deleted', count: 1 });
    expect(db.workflowTerminalDestinationReceipts.size).toBe(0);
    expect(db.workflowTerminalEffects.size).toBe(0);
    expect(db.workflowTerminalSnapshots.size).toBe(0);
    expect(db.workflowTerminalizations.size).toBe(0);

    const second = await createReadyRun(workflows, { workflowName: 'clear-receipt', runId: 'clear-receipt-run' });
    await workflows.reserveWorkflowTerminalDestinationReceipt(receiptInput(second));
    await workflows.dangerouslyClearAll();
    expect(db.workflowTerminalDestinationReceipts.size).toBe(0);
  });
});
