import { describe, expect, it } from 'vitest';
import type { WorkflowRunState } from '../../../workflows';
import type { AdmitWorkflowResumeInput } from '../../types';
import { cloneRunData } from './inmemory';
import {
  admitWorkflowResumeRecord,
  finalizeWorkflowResumeRecord,
  materializeWorkflowResumeOperationHash,
  persistWorkflowStepUpdateRecord,
  rollbackWorkflowResumeRecord,
  WORKFLOW_RESUME_RESULT_RECEIPT_MAX_BYTES,
} from './resume';

const materialize = <T>(value: T): T => JSON.parse(JSON.stringify(value)) as T;

function suspendedSnapshot(): WorkflowRunState {
  return {
    runId: 'run-1',
    resourceId: 'resource-1',
    status: 'suspended',
    value: { durable: 'state' },
    context: {
      input: { topic: 'resume' },
      wait: { status: 'suspended', payload: {}, startedAt: 1, suspendedAt: 2 },
    },
    activePaths: [],
    activeStepsPath: {},
    suspendedPaths: { wait: [0] },
    resumeLabels: { approval: { stepId: 'wait' } },
    waitingPaths: {},
    serializedStepGraph: [],
    requestContext: { tenant: 'tenant-1' },
    executionGeneration: 'wfeg:generation-1',
    lifecycleResumeAttempt: 2,
    lifecycleStepStates: { wait: { stepCallId: 'wfsc:wait-1', stepAttempt: 1 } },
    timestamp: 1,
    tracingContext: { traceId: 'trace-1' },
    runOptions: { disableScorers: true },
  } as WorkflowRunState;
}

function admissionInput(operation: unknown = { step: 'wait', payload: { approved: true } }): AdmitWorkflowResumeInput {
  const snapshot = suspendedSnapshot();
  return {
    workflowName: 'workflow-1',
    runId: snapshot.runId,
    resourceId: snapshot.resourceId,
    resumeOperationHash: materializeWorkflowResumeOperationHash(operation),
    operationReplayContext: { version: 1, steps: ['wait'], resumePath: [0] },
    executionGeneration: snapshot.executionGeneration!,
    lifecycleResumeAttempt: snapshot.lifecycleResumeAttempt!,
    lifecycleStepStates: snapshot.lifecycleStepStates!,
    nextLifecycleResumeAttempt: snapshot.lifecycleResumeAttempt! + 1,
    requestContext: snapshot.requestContext,
  };
}

describe('atomic workflow resume records', () => {
  it('persists exactly one completed result for the next ordinary resume attempt', () => {
    const existing = suspendedSnapshot();
    const nextAttempt = existing.lifecycleResumeAttempt! + 1;
    const ordinaryInput = {
      workflowName: 'workflow-1',
      runId: existing.runId,
      resourceId: existing.resourceId,
      expectedExecutionGeneration: existing.executionGeneration,
      expectedLifecycleResumeAttempt: nextAttempt,
    };

    expect(
      persistWorkflowStepUpdateRecord(
        existing,
        {
          ...ordinaryInput,
          snapshot: {
            ...existing,
            status: 'running',
            lifecycleResumeAttempt: nextAttempt,
          },
        },
        materialize,
      ),
    ).toEqual({ status: 'protected_state' });

    for (const status of ['waiting', 'pending'] as const) {
      expect(
        persistWorkflowStepUpdateRecord(
          existing,
          {
            ...ordinaryInput,
            snapshot: {
              ...existing,
              status,
              lifecycleResumeAttempt: nextAttempt,
            },
          },
          materialize,
        ),
      ).toEqual({ status: 'protected_state' });
    }

    expect(
      persistWorkflowStepUpdateRecord(
        existing,
        {
          ...ordinaryInput,
          snapshot: {
            ...existing,
            status: 'suspended',
            executionGeneration: 'wfeg:different-generation',
            lifecycleResumeAttempt: nextAttempt,
          },
        },
        materialize,
      ),
    ).toEqual({ status: 'stale_execution' });

    for (const lineage of [
      { expected: undefined, existing: existing.executionGeneration, proposed: undefined },
      { expected: '', existing: existing.executionGeneration, proposed: '' },
      { expected: existing.executionGeneration, existing: existing.executionGeneration, proposed: undefined },
      { expected: existing.executionGeneration, existing: undefined, proposed: existing.executionGeneration },
    ]) {
      expect(
        persistWorkflowStepUpdateRecord(
          { ...existing, executionGeneration: lineage.existing },
          {
            ...ordinaryInput,
            expectedExecutionGeneration: lineage.expected,
            snapshot: {
              ...existing,
              status: 'suspended',
              executionGeneration: lineage.proposed,
              lifecycleResumeAttempt: nextAttempt,
            },
          },
          materialize,
        ),
      ).toEqual({ status: 'stale_execution' });
    }

    for (const attempts of [
      { existing: existing.lifecycleResumeAttempt, expected: nextAttempt, proposed: nextAttempt + 1 },
      { existing: undefined, expected: 1, proposed: undefined },
    ]) {
      expect(
        persistWorkflowStepUpdateRecord(
          { ...existing, lifecycleResumeAttempt: attempts.existing },
          {
            ...ordinaryInput,
            expectedLifecycleResumeAttempt: attempts.expected,
            snapshot: {
              ...existing,
              status: 'suspended',
              lifecycleResumeAttempt: attempts.proposed,
            },
          },
          materialize,
        ),
      ).toEqual({ status: 'stale_execution' });
    }

    const persisted = persistWorkflowStepUpdateRecord(
      existing,
      {
        ...ordinaryInput,
        snapshot: {
          ...existing,
          status: 'suspended',
          value: { winner: true },
          lifecycleResumeAttempt: nextAttempt,
          lifecycleStepStates: { wait: { stepCallId: 'wfsc:wait-1', stepAttempt: 2 } },
        },
      },
      materialize,
    );
    expect(persisted).toMatchObject({
      status: 'persisted',
      snapshot: {
        status: 'suspended',
        value: { winner: true },
        lifecycleResumeAttempt: nextAttempt,
      },
    });

    expect(
      persistWorkflowStepUpdateRecord(
        persisted.snapshot,
        {
          ...ordinaryInput,
          snapshot: {
            ...existing,
            status: 'suspended',
            value: { stale: true },
            lifecycleResumeAttempt: nextAttempt,
          },
        },
        materialize,
      ),
    ).toEqual({ status: 'stale_execution' });

    expect(
      persistWorkflowStepUpdateRecord(
        existing,
        {
          ...ordinaryInput,
          expectedLifecycleResumeAttempt: nextAttempt + 1,
          snapshot: {
            ...existing,
            status: 'suspended',
            lifecycleResumeAttempt: nextAttempt + 1,
          },
        },
        materialize,
      ),
    ).toEqual({ status: 'stale_execution' });

    const admission = admissionInput();
    const admitted = admitWorkflowResumeRecord(existing, admission, 10, materialize);
    expect(
      persistWorkflowStepUpdateRecord(
        admitted.snapshot,
        {
          ...ordinaryInput,
          expectedLifecycleResumeAttempt: admission.nextLifecycleResumeAttempt + 1,
          snapshot: {
            ...admitted.snapshot!,
            status: 'suspended',
            lifecycleResumeAttempt: admission.nextLifecycleResumeAttempt + 1,
          },
        },
        materialize,
      ),
    ).toEqual({ status: 'stale_execution' });
  });

  it('keeps operation identity strict when storage snapshots support rich values', () => {
    expect(() => materializeWorkflowResumeOperationHash({ payload: new Map([['key', 'value']]) })).toThrow(
      'unsupported object prototype',
    );
    expect(() => materializeWorkflowResumeOperationHash({ payload: new (class Payload {})() })).toThrow(
      'unsupported object prototype',
    );
  });

  it('hashes values at the JSON transport boundary without Date or toJSON collisions', () => {
    const dateA = materializeWorkflowResumeOperationHash({ at: new Date('2026-01-01T00:00:00.000Z') });
    const dateB = materializeWorkflowResumeOperationHash({ at: new Date('2026-01-02T00:00:00.000Z') });
    const customA = materializeWorkflowResumeOperationHash({ value: { toJSON: () => ({ id: 'a' }) } });
    const customB = materializeWorkflowResumeOperationHash({ value: { toJSON: () => ({ id: 'b' }) } });

    expect(dateA).not.toBe(dateB);
    expect(customA).not.toBe(customB);
    expect(dateA).toBe(materializeWorkflowResumeOperationHash({ at: { toJSON: () => '2026-01-01T00:00:00.000Z' } }));
  });

  it('retains one storage-owned JSON checkpoint and restores it without an event snapshot', () => {
    const snapshot = suspendedSnapshot();
    snapshot.value.large = 's'.repeat(256_000);
    snapshot.context.large = { status: 'success', output: 'c'.repeat(256_000) } as never;
    const input = admissionInput();

    const admitted = admitWorkflowResumeRecord(materialize(snapshot), input, 10, materialize);
    expect(admitted.status).toBe('admitted');
    expect(admitted.snapshot?.resumeCheckpoint?.snapshot.context.large).toEqual(snapshot.context.large);
    expect(admitted.snapshot?.resumeCheckpoint?.snapshot).not.toHaveProperty('resumeCheckpoint');
    expect(admitted.snapshot?.resumeCheckpoint?.snapshot).not.toHaveProperty('resumeResultReceipt');

    const roundTripped = materialize(admitted.snapshot!);
    const rolledBack = rollbackWorkflowResumeRecord(
      roundTripped,
      {
        workflowName: input.workflowName,
        runId: input.runId,
        resourceId: input.resourceId,
        resumeOperationHash: input.resumeOperationHash,
        executionGeneration: input.executionGeneration,
        lifecycleResumeAttempt: input.nextLifecycleResumeAttempt,
        lifecycleStepStates: input.lifecycleStepStates,
      },
      11,
      materialize,
    );
    expect(rolledBack.status).toBe('rolled_back');
    expect(rolledBack.snapshot).toMatchObject({
      status: 'suspended',
      resourceId: 'resource-1',
      runOptions: { disableScorers: true },
      requestContext: { tenant: 'tenant-1' },
    });
    expect(rolledBack.snapshot?.value.large).toHaveLength(256_000);
  });

  it('is idempotent for the same operation and rejects a different concurrent payload', () => {
    const firstInput = admissionInput();
    const first = admitWorkflowResumeRecord(suspendedSnapshot(), firstInput, 10, materialize);
    expect(first.status).toBe('admitted');

    expect(admitWorkflowResumeRecord(first.snapshot, firstInput, 11, materialize)).toMatchObject({
      status: 'already_admitted',
    });
    expect(
      admitWorkflowResumeRecord(first.snapshot, admissionInput({ step: 'other', payload: false }), 11, materialize),
    ).toEqual({ status: 'operation_conflict' });
  });

  it('rejects a checkpoint whose embedded suspended snapshot is corrupt', () => {
    const input = admissionInput();
    const admitted = admitWorkflowResumeRecord(suspendedSnapshot(), input, 10, materialize);
    const corrupt = materialize(admitted.snapshot!);
    corrupt.resumeCheckpoint!.snapshot.status = 'running';

    expect(
      rollbackWorkflowResumeRecord(
        corrupt,
        {
          workflowName: input.workflowName,
          runId: input.runId,
          resumeOperationHash: input.resumeOperationHash,
          executionGeneration: input.executionGeneration,
          lifecycleResumeAttempt: input.nextLifecycleResumeAttempt,
          lifecycleStepStates: input.lifecycleStepStates,
        },
        11,
        materialize,
      ),
    ).toEqual({ status: 'checkpoint_conflict' });
  });

  it('does not let a stale rollback overwrite a newer terminal state', () => {
    const input = admissionInput();
    const admitted = admitWorkflowResumeRecord(suspendedSnapshot(), input, 10, materialize);
    const terminal = { ...admitted.snapshot!, status: 'success' as const, result: { done: true } };

    const rollback = rollbackWorkflowResumeRecord(
      terminal,
      {
        workflowName: input.workflowName,
        runId: input.runId,
        resumeOperationHash: input.resumeOperationHash,
        executionGeneration: input.executionGeneration,
        lifecycleResumeAttempt: input.nextLifecycleResumeAttempt,
        lifecycleStepStates: input.lifecycleStepStates,
      },
      11,
      materialize,
    );
    expect(rollback.status).toBe('fence_conflict');
    expect(terminal).toMatchObject({ status: 'success', result: { done: true } });
  });

  it('preserves rich in-memory checkpoint values and detects later tampering', () => {
    class DurableValue {
      constructor(readonly id: string) {}
    }
    const snapshot = suspendedSnapshot();
    const error = new Error('retained failure');
    snapshot.value = {
      map: new Map([['key', { value: 1 }]]),
      set: new Set(['one', 'two']),
      error,
      regexp: /resume/gi,
      bytes: new Uint8Array([1, 2, 3]),
      instance: new DurableValue('durable'),
    } as never;
    const input = admissionInput();

    const admitted = admitWorkflowResumeRecord(snapshot, input, 10, cloneRunData);
    expect(admitted.status).toBe('admitted');
    const checkpointValue = admitted.snapshot?.resumeCheckpoint?.snapshot.value as Record<string, unknown>;
    expect(checkpointValue.map).toBeInstanceOf(Map);
    expect(checkpointValue.set).toBeInstanceOf(Set);
    expect(checkpointValue.error).toBeInstanceOf(Error);
    expect(checkpointValue.regexp).toBeInstanceOf(RegExp);
    expect(checkpointValue.bytes).toBeInstanceOf(Uint8Array);
    expect(checkpointValue.instance).toBeInstanceOf(DurableValue);

    const rollbackInput = {
      workflowName: input.workflowName,
      runId: input.runId,
      resumeOperationHash: input.resumeOperationHash,
      executionGeneration: input.executionGeneration,
      lifecycleResumeAttempt: input.nextLifecycleResumeAttempt,
      lifecycleStepStates: input.lifecycleStepStates,
    };
    const rolledBack = rollbackWorkflowResumeRecord(admitted.snapshot, rollbackInput, 11, cloneRunData);
    expect(rolledBack.status).toBe('rolled_back');
    expect((rolledBack.snapshot?.value as Record<string, unknown>).instance).toBeInstanceOf(DurableValue);

    const repeated = rollbackWorkflowResumeRecord(rolledBack.snapshot, rollbackInput, 12, cloneRunData);
    expect(repeated.status).toBe('already_rolled_back');

    const tampered = cloneRunData(admitted.snapshot!);
    ((tampered.resumeCheckpoint!.snapshot.value as Record<string, unknown>).map as Map<string, unknown>).set(
      'tampered',
      true,
    );
    expect(rollbackWorkflowResumeRecord(tampered, rollbackInput, 11, cloneRunData)).toEqual({
      status: 'checkpoint_conflict',
    });
  });

  it('clears suspension and tracing evidence for skipped terminal finalization', () => {
    const input = admissionInput();
    const admitted = admitWorkflowResumeRecord(suspendedSnapshot(), input, 10, materialize);
    const finalStepStates = {
      ...input.lifecycleStepStates,
      skipped: { stepCallId: 'wfsc:skipped-1', stepAttempt: 1 },
    };
    const current = { ...admitted.snapshot!, lifecycleStepStates: finalStepStates };
    const finalized = finalizeWorkflowResumeRecord(
      current,
      {
        workflowName: input.workflowName,
        runId: input.runId,
        resourceId: input.resourceId,
        resumeOperationHash: input.resumeOperationHash,
        executionGeneration: input.executionGeneration,
        lifecycleResumeAttempt: input.nextLifecycleResumeAttempt,
        lifecycleStepStates: finalStepStates,
        shouldPersistSnapshot: true,
        receiptKey: 'receipt-1',
        snapshot: {
          ...current,
          status: 'skipped',
          suspendedPaths: { stale: [1] },
          resumeLabels: { stale: { stepId: 'stale' } },
          tracingContext: { traceId: 'stale-trace' },
        },
        result: { status: 'skipped', steps: {}, state: { durable: 'state' } },
      },
      20,
      materialize,
    );

    expect(finalized.status).toBe('finalized');
    expect(finalized.snapshot).toMatchObject({
      status: 'skipped',
      suspendedPaths: {},
      resumeLabels: {},
      resourceId: 'resource-1',
      runOptions: { disableScorers: true },
    });
    expect(finalized.snapshot).not.toHaveProperty('tracingContext');
    expect(finalized.snapshot?.resumeResultReceipt).toMatchObject({
      receiptKey: 'receipt-1',
      resumeOperationHash: input.resumeOperationHash,
      operationReplayContext: { version: 1, steps: ['wait'], resumePath: [0] },
      lifecycleStepStates: finalStepStates,
    });
  });

  it('projects an oversized successful result into a bounded explicit failure receipt', () => {
    const input = admissionInput();
    const admitted = admitWorkflowResumeRecord(suspendedSnapshot(), input, 10, materialize);
    const current = admitted.snapshot!;
    const finalized = finalizeWorkflowResumeRecord(
      current,
      {
        workflowName: input.workflowName,
        runId: input.runId,
        resumeOperationHash: input.resumeOperationHash,
        executionGeneration: input.executionGeneration,
        lifecycleResumeAttempt: input.nextLifecycleResumeAttempt,
        lifecycleStepStates: input.lifecycleStepStates,
        shouldPersistSnapshot: false,
        receiptKey: 'oversized-receipt',
        snapshot: { ...current, status: 'success' },
        result: {
          status: 'success',
          steps: {},
          result: 'x'.repeat(WORKFLOW_RESUME_RESULT_RECEIPT_MAX_BYTES),
        },
      },
      20,
      materialize,
    );

    expect(finalized).toMatchObject({
      status: 'finalized',
      receipt: {
        result: {
          status: 'failed',
          error: { name: 'WorkflowResumeResultTooLargeError' },
        },
      },
      snapshot: {
        status: 'suspended',
        resumeResultReceipt: {
          result: { status: 'failed' },
        },
      },
    });
    expect(Buffer.byteLength(JSON.stringify(finalized.receipt), 'utf8')).toBeLessThan(
      WORKFLOW_RESUME_RESULT_RECEIPT_MAX_BYTES,
    );
  });
});
