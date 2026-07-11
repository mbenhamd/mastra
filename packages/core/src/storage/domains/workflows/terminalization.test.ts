import { afterEach, beforeEach, describe, expect, expectTypeOf, it, vi } from 'vitest';
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
