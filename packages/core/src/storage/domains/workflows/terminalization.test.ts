import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { InMemoryStore } from '../../mock';
import { createEmptyWorkflowSnapshot } from '../../workflow-snapshot';
import type { WorkflowsStorage } from './base';
import { WorkflowsStorage as WorkflowsStorageBase } from './base';
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

  async function setup(): Promise<WorkflowsStorage> {
    const store = new InMemoryStore();
    const workflows = (await store.getStore('workflows'))!;
    await workflows.persistWorkflowSnapshot({
      ...run,
      snapshot: createEmptyWorkflowSnapshot(runId),
    });
    return workflows;
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
      workflows.advanceWorkflowTerminalization({
        ...run,
        ownerId,
        claimToken: first.claimToken!,
        claimGeneration: first.claimGeneration,
        expectedPhase: 'terminalization_pending',
        nextPhase: 'run_state_persisted',
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
    await expect(
      workflows.advanceWorkflowTerminalization({
        ...fenced,
        expectedPhase: 'terminalization_pending',
        nextPhase: 'run_state_persisted',
      }),
    ).resolves.toMatchObject({ status: 'advanced', record: { phase: 'run_state_persisted' } });
    await expect(
      workflows.advanceWorkflowTerminalization({
        ...fenced,
        expectedPhase: 'run_state_persisted',
        nextPhase: 'finish_outbox_pending',
      }),
    ).resolves.toMatchObject({ status: 'advanced', record: { phase: 'finish_outbox_pending' } });
  });

  it('uses one first-terminal-wins slot before and after completion', async () => {
    const workflows = await setup();
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

    const fenced = {
      ...run,
      ownerId,
      claimToken: claim.claimToken!,
      claimGeneration: claim.claimGeneration,
    };
    for (const [expectedPhase, nextPhase] of [
      ['terminalization_pending', 'run_state_persisted'],
      ['run_state_persisted', 'finish_outbox_pending'],
      ['finish_outbox_pending', 'finish_effect_recorded'],
      ['finish_effect_recorded', 'complete'],
    ] as const) {
      await workflows.advanceWorkflowTerminalization({ ...fenced, expectedPhase, nextPhase });
    }
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
      workflows.advanceWorkflowTerminalization({
        ...run,
        ownerId,
        claimToken: originalToken!,
        claimGeneration: originalGeneration,
        expectedPhase: 'terminalization_pending',
        nextPhase: 'run_state_persisted',
      }),
    ).resolves.toMatchObject({ status: 'advanced' });
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
    const workflows = await setup();
    const claim = await acquire(workflows);
    const fenced = {
      ...run,
      ownerId,
      claimToken: claim.claimToken!,
      claimGeneration: claim.claimGeneration,
    };
    for (const [expectedPhase, nextPhase] of [
      ['terminalization_pending', 'run_state_persisted'],
      ['run_state_persisted', 'finish_outbox_pending'],
      ['finish_outbox_pending', 'finish_effect_recorded'],
      ['finish_effect_recorded', 'complete'],
    ] as const) {
      await workflows.advanceWorkflowTerminalization({ ...fenced, expectedPhase, nextPhase });
    }

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
});
