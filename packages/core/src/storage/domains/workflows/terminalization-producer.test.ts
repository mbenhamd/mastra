import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type {
  WorkflowRunState,
  WorkflowTerminalEffectRecord,
  WorkflowTerminalRecoveryAncestryV1,
} from '../../../workflows';
import { createWorkflowTerminalGraphFingerprint } from '../../../workflows/terminal-continuation';
import { createEmptyWorkflowSnapshot } from '../../workflow-snapshot';
import { InMemoryDB } from '../inmemory-db';
import type { WorkflowsStorage } from './base';
import { WorkflowsInMemory } from './inmemory';
import {
  MAX_WORKFLOW_TERMINAL_PARENT_EXECUTION_PATH_LENGTH,
  createWorkflowTerminalEffectRecord,
  getWorkflowTerminalSnapshotRecordHash,
  validateWorkflowTerminalEffectIntegrity,
} from './terminalization';
import { createTerminalRecoveryEnvelope } from './terminalization-test-utils';

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

  function parentGraph(stepId = 'nested', executionPath = [0]): WorkflowRunState['serializedStepGraph'] {
    return Array.from({ length: executionPath[0]! + 1 }, (_, rootIndex) =>
      rootIndex === executionPath[0]
        ? executionPath.length === 1
          ? { type: 'step' as const, step: { id: stepId, component: 'WORKFLOW' } }
          : {
              type: 'parallel' as const,
              steps: Array.from({ length: executionPath[1]! + 1 }, (_, branchIndex) => ({
                type: 'step' as const,
                step: {
                  id: branchIndex === executionPath[1] ? stepId : `filler-${rootIndex}-${branchIndex}`,
                  component: branchIndex === executionPath[1] ? 'WORKFLOW' : 'STEP',
                },
              })),
            }
        : { type: 'sleep' as const, id: `filler-${rootIndex}`, duration: 1 },
    );
  }

  async function createTerminalRun(
    workflows: WorkflowsStorage,
    run: { workflowName: string; runId: string; resourceId?: string },
    eventKey = `${run.runId}-event`,
    ancestry: WorkflowTerminalRecoveryAncestryV1 = [],
  ) {
    await workflows.persistWorkflowSnapshot({
      ...run,
      snapshot: { ...createEmptyWorkflowSnapshot(run.runId), serializedStepGraph: parentGraph() },
    });
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
      serializedStepGraph: parentGraph(),
      status: 'failed' as const,
      context: {
        __state: { retained: true },
        marker: { status: 'success' as const, output: { retained: true } },
      },
    };
    if (ancestry.length > 0) {
      await workflows.persistWorkflowTerminalRecoveryAncestry({ ...run, ancestry });
    }
    const recoveryEnvelope = createTerminalRecoveryEnvelope({
      ...run,
      snapshot,
      terminalStatus: 'failed',
      ancestry,
    });
    await expect(
      workflows.persistWorkflowTerminalState({ ...fence, snapshot, recoveryEnvelope }),
    ).resolves.toMatchObject({
      status: 'persisted',
      record: { phase: 'run_state_persisted' },
    });
    return { claim: claim.record, fence, snapshot };
  }

  function nestedAncestry(
    child: { workflowName: string; runId: string },
    parent: { workflowName: string; runId: string },
    stepId = 'nested',
    executionPath = [0],
  ): WorkflowTerminalRecoveryAncestryV1 {
    return [
      {
        version: 1,
        childWorkflowName: child.workflowName,
        childRunId: child.runId,
        parentWorkflowName: parent.workflowName,
        parentRunId: parent.runId,
        parentGraphFingerprint: createWorkflowTerminalGraphFingerprint(parentGraph(stepId, executionPath)),
        source: { kind: 'step', stepId, executionPath },
        inputPointer: { kind: 'parent-source-payload', stepId },
        resultPointer: {
          kind: 'retained-terminal-result',
          workflowName: child.workflowName,
          runId: child.runId,
        },
        resumeMetadata: { wasResume: false, resumeSteps: [] },
      },
    ];
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
      destinationReceiptVersion: 1,
      parentApplicationVersion: 1,
      recoveryVersion: 1,
    });
    const canonical = db.workflows.get(runKey(run))?.snapshot;
    const retained = db.workflowTerminalSnapshots.get(runKey(run));
    expect(retained).toMatchObject({
      version: 1,
      workflowName: run.workflowName,
      runId: run.runId,
      resourceId: run.resourceId,
      terminalStatus: 'failed',
      envelopeHash: expect.stringMatching(/^sha256:[a-f0-9]{64}$/),
      recordHash: expect.stringMatching(/^sha256:[a-f0-9]{64}$/),
      envelope: { terminalStatus: 'failed', finalState: { retained: true } },
    });
    expect(retained?.envelope).not.toBe(canonical);

    snapshot.context.marker.output.retained = false;
    await expect(workflows.loadWorkflowSnapshot(run)).resolves.toMatchObject({
      context: { marker: { output: { retained: true } } },
    });
    expect(retained?.envelope).toMatchObject({ finalState: { retained: true } });

    await workflows.persistWorkflowSnapshot({ ...run, snapshot: createEmptyWorkflowSnapshot(run.runId) });
    await expect(workflows.loadWorkflowSnapshot(run)).resolves.toMatchObject({ status: 'pending' });
    expect(db.workflowTerminalSnapshots.get(runKey(run))).toMatchObject({
      envelope: { terminalStatus: 'failed', finalState: { retained: true } },
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

    const retainedBeforePrepare = db.workflowTerminalSnapshots.get(runKey(run));
    if (!retainedBeforePrepare) throw new Error('Expected retained terminal state');
    retainedBeforePrepare.resourceId = 'redirected-before-prepare';
    await expect(workflows.prepareWorkflowTerminalEffect(prepare)).rejects.toThrow(
      'Invalid workflow terminal snapshot record integrity',
    );
    retainedBeforePrepare.resourceId = run.resourceId;

    const prepared = await workflows.prepareWorkflowTerminalEffect(prepare);
    expect(prepared).toMatchObject({
      status: 'prepared',
      effect: { kind: 'workflow-finish', workflowName: run.workflowName, runId: run.runId },
    });
    if (prepared.status !== 'prepared') throw new Error('Expected prepared effect');
    expect(prepared.effect.effectKey).toMatch(/^wte:v1:[a-f0-9]{64}$/);
    expect(prepared.effect.payloadHash).toMatch(/^sha256:[a-f0-9]{64}$/);
    expect(prepared.effect.resourceId).toBe(run.resourceId);
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

    const retained = db.workflowTerminalSnapshots.get(runKey(run));
    if (!retained) throw new Error('Expected retained terminal state');
    retained.resourceId = 'redirected-resource';
    await expect(workflows.getWorkflowTerminalEffectForDispatch({ ...fence, kind: 'workflow-finish' })).rejects.toThrow(
      'Invalid workflow terminal snapshot record integrity',
    );
    retained.resourceId = run.resourceId;

    await workflows.deleteWorkflowRunById(run);
    const dispatch = await workflows.getWorkflowTerminalEffectForDispatch({ ...fence, kind: 'workflow-finish' });
    expect(dispatch).toMatchObject({
      status: 'found',
      effect: { effectKey: prepared.effect.effectKey },
      recovery: {
        resourceId: run.resourceId,
        envelope: { terminalStatus: 'failed', finalState: { retained: true } },
      },
    });
    if (dispatch.status !== 'found') throw new Error('Expected dispatch evidence');
    dispatch.effect.effectKey = 'caller-mutated';
    dispatch.recovery.envelope.finalState.retained = false;
    await expect(
      workflows.getWorkflowTerminalEffectForDispatch({ ...fence, kind: 'workflow-finish' }),
    ).resolves.toMatchObject({
      status: 'found',
      effect: { effectKey: prepared.effect.effectKey },
      recovery: {
        resourceId: run.resourceId,
        envelope: { terminalStatus: 'failed', finalState: { retained: true } },
      },
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
      recovery: { envelope: { runId: intended.runId } },
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
    const forgedRetained = { ...originalRetained, createdAt: originalRetained.createdAt + 500 };
    db.workflowTerminalSnapshots.set(retainedKey, {
      ...forgedRetained,
      recordHash: getWorkflowTerminalSnapshotRecordHash(forgedRetained),
    });
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
      'Invalid workflow terminal snapshot record integrity',
    );
  });

  it('frames parent identity, rejects unsafe paths, and keeps generic CAS evidence-closed', async () => {
    const db = new InMemoryDB();
    const workflows = new WorkflowsInMemory({ db });
    const run = { workflowName: 'parent-workflow', runId: 'parent-run' };
    const parent = { workflowName: 'root', runId: 'root-run' };
    await workflows.persistWorkflowSnapshot({
      ...parent,
      snapshot: {
        ...createEmptyWorkflowSnapshot(parent.runId),
        serializedStepGraph: parentGraph('nested', [0, 2]),
      },
    });
    const { claim, fence } = await createTerminalRun(
      workflows,
      run,
      `${run.runId}-event`,
      nestedAncestry(run, parent, 'nested', [0, 2]),
    );
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
    const retained = db.workflowTerminalSnapshots.get(runKey(run));
    if (!retained) throw new Error('Expected retained recovery envelope');
    const first = createWorkflowTerminalEffectRecord(claim, retained, base, Date.now());
    const reordered = createWorkflowTerminalEffectRecord(
      claim,
      retained,
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
      retained,
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

  it('retains a completed ancestor while a recursively linked child remains pending', async () => {
    const db = new InMemoryDB();
    const workflows = new WorkflowsInMemory({ db });
    const parent = { workflowName: 'cleanup-parent', runId: 'parent-run' };
    const child = { workflowName: 'cleanup-child', runId: 'child-run' };
    await workflows.persistWorkflowSnapshot({
      ...parent,
      snapshot: { ...createEmptyWorkflowSnapshot(parent.runId), serializedStepGraph: parentGraph() },
    });
    const childState = await createTerminalRun(workflows, child, `${child.runId}-event`, nestedAncestry(child, parent));
    const parentState = await createTerminalRun(workflows, parent);

    await workflows.prepareWorkflowTerminalEffect({
      ...parentState.fence,
      expectedPhase: 'run_state_persisted',
      effect: { kind: 'workflow-finish' },
    });
    await workflows.prepareWorkflowTerminalEffect({
      ...childState.fence,
      expectedPhase: 'run_state_persisted',
      effect: {
        kind: 'parent-workflow-step-end',
        parentWorkflowName: parent.workflowName,
        parentRunId: parent.runId,
        parentStepId: 'nested',
        parentExecutionPath: [0],
      },
    });
    const parentJournal = db.workflowTerminalizations.get(runKey(parent));
    const childJournal = db.workflowTerminalizations.get(runKey(child));
    if (!parentJournal || !childJournal) throw new Error('Expected cleanup journals');
    db.workflowTerminalizations.set(runKey(parent), {
      ...parentJournal,
      phase: 'complete',
      completedAt: Date.now(),
    });
    vi.advanceTimersByTime(1);

    await expect(
      workflows.deleteCompletedWorkflowTerminalizations({ ...parent, olderThan: new Date() }),
    ).resolves.toEqual({ status: 'deleted', count: 0 });
    db.workflowTerminalizations.set(runKey(child), {
      ...childJournal,
      phase: 'complete',
      completedAt: Date.now(),
    });
    vi.advanceTimersByTime(1);
    await expect(
      workflows.deleteCompletedWorkflowTerminalizations({ ...parent, olderThan: new Date() }),
    ).resolves.toEqual({ status: 'deleted', count: 1 });
  });

  it('traverses wide and deep dependency graphs once, isolates unrelated trees, and fails closed at 100k', () => {
    const completedJournal = { phase: 'complete' } as never;
    const addEdge = (
      db: InMemoryDB,
      parent: { workflowName: string; runId: string },
      child: { workflowName: string; runId: string },
    ) => {
      db.workflowTerminalEffects.set(JSON.stringify([child.workflowName, child.runId, 'parent-workflow-step-end']), {
        kind: 'parent-workflow-step-end',
        workflowName: child.workflowName,
        runId: child.runId,
        parentWorkflowName: parent.workflowName,
        parentRunId: parent.runId,
      } as never);
    };
    const hasPending = (workflows: WorkflowsInMemory, root: { workflowName: string; runId: string }) =>
      (workflows as any).hasPendingTerminalDependents(root.workflowName, root.runId) as boolean;

    const wideDb = new InMemoryDB();
    const wide = new WorkflowsInMemory({ db: wideDb });
    const wideRoot = { workflowName: 'wide-root', runId: 'root' };
    for (let index = 0; index < 5_000; index++) {
      const child = { workflowName: 'wide-child', runId: String(index) };
      addEdge(wideDb, wideRoot, child);
      wideDb.workflowTerminalizations.set(runKey(child), completedJournal);
    }
    addEdge(
      wideDb,
      { workflowName: 'unrelated-root', runId: 'root' },
      {
        workflowName: 'unrelated-pending',
        runId: 'child',
      },
    );
    expect(hasPending(wide, wideRoot)).toBe(false);

    const deepDb = new InMemoryDB();
    const deep = new WorkflowsInMemory({ db: deepDb });
    const deepRoot = { workflowName: 'deep', runId: '0' };
    let parent = deepRoot;
    for (let index = 1; index <= 5_000; index++) {
      const child = { workflowName: 'deep', runId: String(index) };
      addEdge(deepDb, parent, child);
      if (index < 5_000) deepDb.workflowTerminalizations.set(runKey(child), completedJournal);
      parent = child;
    }
    expect(hasPending(deep, deepRoot)).toBe(true);
    deepDb.workflowTerminalizations.set(runKey(parent), completedJournal);
    expect(hasPending(deep, deepRoot)).toBe(false);

    const boundedDb = new InMemoryDB();
    const bounded = new WorkflowsInMemory({ db: boundedDb });
    const boundedRoot = { workflowName: 'bounded-root', runId: 'root' };
    for (let index = 0; index < 100_000; index++) {
      addEdge(boundedDb, boundedRoot, { workflowName: 'bounded-child', runId: String(index) });
    }
    expect(hasPending(bounded, boundedRoot)).toBe(true);
  });
});
