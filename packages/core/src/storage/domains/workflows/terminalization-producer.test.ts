import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { WorkflowTerminalEffectRecord } from '../../../workflows';
import { createEmptyWorkflowSnapshot } from '../../workflow-snapshot';
import { InMemoryDB } from '../inmemory-db';
import type { WorkflowsStorage } from './base';
import { WorkflowsInMemory } from './inmemory';
import {
  MAX_WORKFLOW_TERMINAL_PARENT_EXECUTION_PATH_LENGTH,
  createWorkflowTerminalEffectRecord,
  validateWorkflowTerminalEffectIntegrity,
} from './terminalization';

describe('WorkflowsInMemory terminal producer outbox', () => {
  const now = new Date('2026-01-01T00:00:00.000Z');

  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(now);
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  function effectKey(run: { workflowName: string; runId: string }, kind: WorkflowTerminalEffectRecord['kind']) {
    return JSON.stringify([run.workflowName, run.runId, kind]);
  }

  function runKey(run: { workflowName: string; runId: string }) {
    return JSON.stringify([run.workflowName, run.runId]);
  }

  async function createTerminalRun(
    workflows: WorkflowsStorage,
    run: { workflowName: string; runId: string; resourceId?: string },
    eventKey = `${run.runId}-event`,
  ) {
    await workflows.persistWorkflowSnapshot({ ...run, snapshot: createEmptyWorkflowSnapshot(run.runId) });
    const claim = await workflows.claimWorkflowTerminalization({
      ...run,
      eventKey,
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
    const snapshot = {
      ...createEmptyWorkflowSnapshot(run.runId),
      status: 'failed' as const,
      context: { marker: { status: 'success' as const, output: { retained: true } } },
    };
    await expect(workflows.persistWorkflowTerminalState({ ...fence, snapshot })).resolves.toMatchObject({
      status: 'persisted',
      record: { phase: 'run_state_persisted' },
    });
    return { claim: claim.record, fence, snapshot };
  }

  function withStatefulIdentity<T extends { workflowName: string; runId: string }>(
    operation: T,
    alternate: { workflowName: string; runId: string },
  ) {
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

  it('retains an isolated immutable snapshot while the canonical run remains replaceable', async () => {
    const db = new InMemoryDB();
    const workflows = new WorkflowsInMemory({ db });
    const run = { workflowName: 'retained-workflow', runId: 'retained-run', resourceId: 'resource-retained' };
    const { snapshot } = await createTerminalRun(workflows, run);

    expect(workflows.getWorkflowTerminalizationCapabilities()).toEqual({
      journalVersion: 1,
      producerOutboxVersion: 1,
    });
    const canonical = db.workflows.get(runKey(run))?.snapshot;
    const retained = db.workflowTerminalSnapshots.get(runKey(run));
    expect(retained).toMatchObject({
      version: 1,
      workflowName: run.workflowName,
      runId: run.runId,
      resourceId: run.resourceId,
      terminalStatus: 'failed',
      snapshot: { status: 'failed', context: { marker: { output: { retained: true } } } },
    });
    expect(retained?.snapshot).not.toBe(canonical);

    snapshot.context.marker.output.retained = false;
    await expect(workflows.loadWorkflowSnapshot(run)).resolves.toMatchObject({
      context: { marker: { output: { retained: true } } },
    });
    expect(retained?.snapshot).toMatchObject({ context: { marker: { output: { retained: true } } } });

    await workflows.persistWorkflowSnapshot({ ...run, snapshot: createEmptyWorkflowSnapshot(run.runId) });
    await expect(workflows.loadWorkflowSnapshot(run)).resolves.toMatchObject({ status: 'pending' });
    expect(db.workflowTerminalSnapshots.get(runKey(run))).toMatchObject({
      snapshot: { status: 'failed', context: { marker: { output: { retained: true } } } },
    });
  });

  it('prepares one immutable finish intent and returns retained state only to the live fence', async () => {
    const db = new InMemoryDB();
    const workflows = new WorkflowsInMemory({ db });
    const run = { workflowName: 'finish-workflow', runId: 'finish-run', resourceId: 'resource-finish' };
    const { fence } = await createTerminalRun(workflows, run);
    const prepare = {
      ...fence,
      expectedPhase: 'run_state_persisted' as const,
      effect: { kind: 'workflow-finish' as const },
    };

    const prepared = await workflows.prepareWorkflowTerminalEffect(prepare);
    expect(prepared).toMatchObject({
      status: 'prepared',
      effect: { kind: 'workflow-finish', workflowName: run.workflowName, runId: run.runId },
    });
    if (prepared.status !== 'prepared') throw new Error('Expected prepared effect');
    expect(prepared.effect.effectKey).toMatch(/^wte:v1:[a-f0-9]{64}$/);
    expect(prepared.effect.payloadHash).toMatch(/^sha256:[a-f0-9]{64}$/);
    vi.advanceTimersByTime(9_000);
    await expect(workflows.prepareWorkflowTerminalEffect({ ...prepare, leaseMs: 10_000 })).resolves.toMatchObject({
      status: 'already_prepared',
      effect: { effectKey: prepared.effect.effectKey },
    });
    expect(db.workflowTerminalizations.get(runKey(run))?.leaseExpiresAt).toBe(now.getTime() + 19_000);
    vi.advanceTimersByTime(2_000);
    await expect(
      workflows.getWorkflowTerminalEffectForDispatch({ ...fence, claimToken: 'wrong-token', kind: 'workflow-finish' }),
    ).resolves.toMatchObject({ status: 'fence_conflict' });

    await workflows.deleteWorkflowRunById(run);
    const dispatch = await workflows.getWorkflowTerminalEffectForDispatch({ ...fence, kind: 'workflow-finish' });
    expect(dispatch).toMatchObject({
      status: 'found',
      effect: { effectKey: prepared.effect.effectKey },
      snapshot: { status: 'failed', context: { marker: { output: { retained: true } } } },
      resourceId: run.resourceId,
    });
    if (dispatch.status !== 'found') throw new Error('Expected dispatch evidence');
    dispatch.effect.effectKey = 'caller-mutated';
    dispatch.snapshot.status = 'success';
    await expect(
      workflows.getWorkflowTerminalEffectForDispatch({ ...fence, kind: 'workflow-finish' }),
    ).resolves.toMatchObject({
      status: 'found',
      effect: { effectKey: prepared.effect.effectKey },
      snapshot: { status: 'failed' },
      resourceId: run.resourceId,
    });
  });

  it('materializes prepare and dispatch identities once without cross-run mutation', async () => {
    const db = new InMemoryDB();
    const workflows = new WorkflowsInMemory({ db });
    const intended = { workflowName: 'identity-intended', runId: 'identity-intended-run' };
    const alternate = { workflowName: 'identity-alternate', runId: 'identity-alternate-run' };
    const intendedState = await createTerminalRun(workflows, intended);
    await createTerminalRun(workflows, alternate);

    const prepare = withStatefulIdentity(
      {
        ...intendedState.fence,
        expectedPhase: 'run_state_persisted' as const,
        effect: { kind: 'workflow-finish' as const },
      },
      alternate,
    );
    await expect(workflows.prepareWorkflowTerminalEffect(prepare.operation)).resolves.toMatchObject({
      status: 'prepared',
    });
    expect(prepare.reads()).toEqual({ workflowName: 1, runId: 1 });
    expect(db.workflowTerminalEffects.has(effectKey(intended, 'workflow-finish'))).toBe(true);
    expect(db.workflowTerminalEffects.has(effectKey(alternate, 'workflow-finish'))).toBe(false);
    await expect(workflows.getWorkflowTerminalization(alternate)).resolves.toMatchObject({
      status: 'found',
      record: { phase: 'run_state_persisted' },
    });

    const dispatch = withStatefulIdentity({ ...intendedState.fence, kind: 'workflow-finish' as const }, alternate);
    await expect(workflows.getWorkflowTerminalEffectForDispatch(dispatch.operation)).resolves.toMatchObject({
      status: 'found',
      snapshot: { runId: intended.runId },
    });
    expect(dispatch.reads()).toEqual({ workflowName: 1, runId: 1 });
  });

  it('rejects accessor and unknown descriptor fields before looking up a run', async () => {
    const workflows = new WorkflowsInMemory({ db: new InMemoryDB() });
    const missing = {
      workflowName: 'missing-workflow',
      runId: 'missing-run',
      ownerId: 'owner',
      claimToken: 'token',
      claimGeneration: 1,
      expectedPhase: 'run_state_persisted' as const,
    };
    const accessorEffect: Record<string, unknown> = {};
    Object.defineProperty(accessorEffect, 'kind', { enumerable: true, get: () => 'workflow-finish' });
    await expect(
      workflows.prepareWorkflowTerminalEffect({ ...missing, effect: accessorEffect as never }),
    ).rejects.toThrow('effect contains unknown or accessor fields');
    await expect(
      workflows.prepareWorkflowTerminalEffect({
        ...missing,
        effect: { kind: 'workflow-finish', extra: true } as never,
      }),
    ).rejects.toThrow('effect contains unknown or accessor fields');
    await expect(
      workflows.getWorkflowTerminalEffectForDispatch({ ...missing, kind: 'invalid' as never }),
    ).rejects.toThrow('kind must be parent-workflow-step-end or workflow-finish');
    await expect(
      workflows.prepareWorkflowTerminalEffect({ ...missing, claimToken: '', effect: { kind: 'workflow-finish' } }),
    ).rejects.toThrow('claimToken must be a well-formed non-empty string');
    await expect(
      workflows.getWorkflowTerminalEffectForDispatch({ ...missing, claimToken: '', kind: 'workflow-finish' }),
    ).rejects.toThrow('claimToken must be a well-formed non-empty string');
    await expect(
      workflows.getWorkflowTerminalEffectForDispatch({
        ...missing,
        workflowName: 'w'.repeat(513),
        kind: 'workflow-finish',
      }),
    ).rejects.toThrow('workflowName must be a well-formed non-empty string no longer than 512 characters');
    await expect(
      workflows.getWorkflowTerminalEffectForDispatch({
        ...missing,
        runId: `run${String.fromCharCode(0xd800)}`,
        kind: 'workflow-finish',
      }),
    ).rejects.toThrow('runId must be a well-formed non-empty string no longer than 512 characters');
    await expect(
      workflows.claimWorkflowTerminalization({
        workflowName: 'w'.repeat(513),
        runId: missing.runId,
        eventKey: 'event',
        terminalStatus: 'failed',
        ownerId: missing.ownerId,
        leaseMs: 10_000,
      }),
    ).rejects.toThrow('workflowName must be a well-formed non-empty string no longer than 512 characters');
    await expect(
      workflows.claimWorkflowTerminalization({
        workflowName: missing.workflowName,
        runId: 'r'.repeat(513),
        eventKey: 'event',
        terminalStatus: 'failed',
        ownerId: missing.ownerId,
        leaseMs: 10_000,
      }),
    ).rejects.toThrow('runId must be a well-formed non-empty string no longer than 512 characters');

    let propertyReads = 0;
    const proxyEffect = new Proxy(
      { kind: 'workflow-finish' as const },
      {
        get(target, property, receiver) {
          propertyReads += 1;
          return Reflect.get(target, property, receiver);
        },
      },
    );
    await expect(workflows.prepareWorkflowTerminalEffect({ ...missing, effect: proxyEffect })).resolves.toEqual({
      status: 'missing_run',
    });
    expect(propertyReads).toBe(0);
  });

  it('fails closed on forged effect and retained evidence before honoring a stale fence', async () => {
    const db = new InMemoryDB();
    const workflows = new WorkflowsInMemory({ db });
    const run = { workflowName: 'forged-workflow', runId: 'forged-run' };
    const { fence } = await createTerminalRun(workflows, run);
    const retainedKey = runKey(run);
    const originalRetained = db.workflowTerminalSnapshots.get(retainedKey);
    if (!originalRetained) throw new Error('Expected retained terminal state');
    vi.advanceTimersByTime(1_000);
    db.workflowTerminalSnapshots.set(retainedKey, { ...originalRetained, createdAt: originalRetained.createdAt + 500 });
    await expect(
      workflows.prepareWorkflowTerminalEffect({
        ...fence,
        expectedPhase: 'run_state_persisted',
        effect: { kind: 'workflow-finish' },
      }),
    ).rejects.toThrow('Invalid workflow terminal snapshot journal link');
    db.workflowTerminalSnapshots.set(retainedKey, originalRetained);
    await workflows.prepareWorkflowTerminalEffect({
      ...fence,
      expectedPhase: 'run_state_persisted',
      effect: { kind: 'workflow-finish' },
    });
    const key = effectKey(run, 'workflow-finish');
    const original = db.workflowTerminalEffects.get(key);
    if (!original) throw new Error('Expected effect evidence');

    for (const forged of [
      { ...original, payloadHash: `sha256:${'0'.repeat(64)}` },
      { ...original, sourceEventKey: 'forged-event' },
      { ...original, createdAt: originalRetained.createdAt - 1 },
    ]) {
      db.workflowTerminalEffects.set(key, forged);
      await expect(
        workflows.getWorkflowTerminalEffectForDispatch({
          ...fence,
          claimToken: 'stale-token',
          kind: 'workflow-finish',
        }),
      ).rejects.toThrow();
    }
    db.workflowTerminalEffects.set(key, original);

    const parentKey = effectKey(run, 'parent-workflow-step-end');
    db.workflowTerminalEffects.set(parentKey, original);
    await expect(
      workflows.getWorkflowTerminalEffectForDispatch({ ...fence, kind: 'parent-workflow-step-end' }),
    ).rejects.toThrow('Invalid workflow terminal effect kind');
    db.workflowTerminalEffects.delete(parentKey);

    const retained = db.workflowTerminalSnapshots.get(retainedKey);
    if (!retained) throw new Error('Expected retained terminal state');
    db.workflowTerminalSnapshots.set(retainedKey, { ...retained, terminalStatus: 'success' });
    await expect(workflows.getWorkflowTerminalEffectForDispatch({ ...fence, kind: 'workflow-finish' })).rejects.toThrow(
      'Invalid workflow terminal snapshot journal link',
    );
  });

  it('frames parent identity, rejects unsafe paths, and keeps generic CAS evidence-closed', async () => {
    const db = new InMemoryDB();
    const workflows = new WorkflowsInMemory({ db });
    const run = { workflowName: 'parent-workflow', runId: 'parent-run' };
    const { claim, fence } = await createTerminalRun(workflows, run);
    const base = {
      ...fence,
      expectedPhase: 'run_state_persisted' as const,
      effect: {
        kind: 'parent-workflow-step-end' as const,
        parentWorkflowName: 'root',
        parentRunId: 'root-run',
        parentStepId: 'nested',
        parentExecutionPath: [0, 2],
      },
    };
    const first = createWorkflowTerminalEffectRecord(claim, base, Date.now());
    const reordered = createWorkflowTerminalEffectRecord(
      claim,
      {
        ...fence,
        expectedPhase: 'run_state_persisted',
        effect: {
          parentExecutionPath: [0, 2],
          parentStepId: 'nested',
          parentRunId: 'root-run',
          parentWorkflowName: 'root',
          kind: 'parent-workflow-step-end',
        },
      },
      Date.now() + 1,
    );
    const changedPath = createWorkflowTerminalEffectRecord(
      claim,
      { ...base, effect: { ...base.effect, parentExecutionPath: [0, 3] } },
      Date.now() + 2,
    );
    expect(reordered.effectKey).toBe(first.effectKey);
    expect(changedPath.effectKey).not.toBe(first.effectKey);
    expect(() => validateWorkflowTerminalEffectIntegrity(first)).not.toThrow();

    const prepared = await workflows.prepareWorkflowTerminalEffect(base);
    expect(prepared).toMatchObject({ status: 'prepared', effect: { parentExecutionPath: [0, 2] } });
    const dispatch = await workflows.getWorkflowTerminalEffectForDispatch({
      ...fence,
      kind: 'parent-workflow-step-end',
    });
    if (dispatch.status !== 'found' || dispatch.effect.kind !== 'parent-workflow-step-end') {
      throw new Error('Expected parent dispatch evidence');
    }
    dispatch.effect.parentExecutionPath[0] = 99;
    await expect(
      workflows.getWorkflowTerminalEffectForDispatch({ ...fence, kind: 'parent-workflow-step-end' }),
    ).resolves.toMatchObject({ status: 'found', effect: { parentExecutionPath: [0, 2] } });

    const sparse = Array(2) as number[];
    sparse[1] = 1;
    for (const parentExecutionPath of [
      sparse,
      [],
      [-1],
      [Number.MAX_SAFE_INTEGER + 1],
      Array(MAX_WORKFLOW_TERMINAL_PARENT_EXECUTION_PATH_LENGTH + 1).fill(0),
    ]) {
      await expect(
        workflows.prepareWorkflowTerminalEffect({
          ...base,
          effect: { ...base.effect, parentExecutionPath },
        }),
      ).rejects.toThrow('parentExecutionPath must contain');
    }

    let lengthReads = 0;
    const statefulPath = new Proxy(Array(MAX_WORKFLOW_TERMINAL_PARENT_EXECUTION_PATH_LENGTH + 1).fill(0), {
      get(target, property, receiver) {
        if (property === 'length') {
          lengthReads += 1;
          return lengthReads < 3 ? 1 : target.length;
        }
        return Reflect.get(target, property, receiver);
      },
    });
    await expect(
      workflows.prepareWorkflowTerminalEffect({
        ...base,
        effect: { ...base.effect, parentExecutionPath: statefulPath },
      }),
    ).rejects.toThrow('parentExecutionPath must contain');
    expect(lengthReads).toBe(0);
    await expect(
      workflows.prepareWorkflowTerminalEffect({
        ...base,
        effect: { ...base.effect, parentWorkflowName: `root${String.fromCharCode(0xd800)}` },
      }),
    ).rejects.toThrow('parentWorkflowName must be a well-formed non-empty string');

    await expect(
      workflows.advanceWorkflowTerminalization({
        ...fence,
        expectedPhase: 'run_state_persisted',
        nextPhase: 'parent_outbox_pending',
      }),
    ).resolves.toEqual({ status: 'invalid_transition' });
  });

  it('removes journal, effect, and retained state only after completed retention', async () => {
    const db = new InMemoryDB();
    const workflows = new WorkflowsInMemory({ db });
    const run = { workflowName: 'cleanup-workflow', runId: 'cleanup-run' };
    const { fence } = await createTerminalRun(workflows, run);
    await workflows.prepareWorkflowTerminalEffect({
      ...fence,
      expectedPhase: 'run_state_persisted',
      effect: { kind: 'workflow-finish' },
    });
    const journal = db.workflowTerminalizations.get(runKey(run));
    if (!journal) throw new Error('Expected journal');
    db.workflowTerminalizations.set(runKey(run), {
      ...journal,
      phase: 'finish_effect_recorded',
    });
    await expect(
      workflows.advanceWorkflowTerminalization({
        ...fence,
        expectedPhase: 'finish_effect_recorded',
        nextPhase: 'complete',
      }),
    ).resolves.toMatchObject({ status: 'advanced', record: { phase: 'complete' } });
    vi.advanceTimersByTime(1);
    await expect(workflows.deleteCompletedWorkflowTerminalizations({ ...run, olderThan: new Date() })).resolves.toEqual(
      { status: 'deleted', count: 1 },
    );
    expect(db.workflowTerminalizations.has(runKey(run))).toBe(false);
    expect(db.workflowTerminalEffects.has(effectKey(run, 'workflow-finish'))).toBe(false);
    expect(db.workflowTerminalSnapshots.has(runKey(run))).toBe(false);
  });
});
