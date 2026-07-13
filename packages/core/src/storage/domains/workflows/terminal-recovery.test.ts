import { describe, expect, it } from 'vitest';
import type { WorkflowRunState, WorkflowTerminalRecoveryAncestryV1 } from '../../../workflows';
import { createWorkflowTerminalGraphFingerprint } from '../../../workflows/terminal-continuation';
import { createEmptyWorkflowSnapshot } from '../../workflow-snapshot';
import { InMemoryDB } from '../inmemory-db';
import { WorkflowsInMemory } from './inmemory';
import { createTerminalRecoveryEnvelope } from './terminalization-test-utils';

const NESTED_PARENT_GRAPH: WorkflowRunState['serializedStepGraph'] = [
  { type: 'step', step: { id: 'nested', component: 'WORKFLOW' } },
];
const EMPTY_CHILD_GRAPH_FINGERPRINT = createWorkflowTerminalGraphFingerprint([]);

function createParentSnapshot(runId: string): WorkflowRunState {
  return { ...createEmptyWorkflowSnapshot(runId), serializedStepGraph: NESTED_PARENT_GRAPH };
}

function ancestry(
  child: { workflowName: string; runId: string },
  parent: { workflowName: string; runId: string },
): WorkflowTerminalRecoveryAncestryV1 {
  return [
    {
      version: 1,
      childWorkflowName: child.workflowName,
      childRunId: child.runId,
      parentWorkflowName: parent.workflowName,
      parentRunId: parent.runId,
      parentGraphFingerprint: createWorkflowTerminalGraphFingerprint(NESTED_PARENT_GRAPH),
      source: { kind: 'step', stepId: 'nested', executionPath: [0] },
      inputPointer: { kind: 'parent-source-payload', stepId: 'nested' },
      resultPointer: { kind: 'retained-terminal-result', workflowName: child.workflowName, runId: child.runId },
      resumeMetadata: { wasResume: false, resumeSteps: [] },
    },
  ];
}

async function completeRoot(
  workflows: WorkflowsInMemory,
  db: InMemoryDB,
  run: { workflowName: string; runId: string },
  terminalStatus: 'success' | 'failed' | 'canceled' = 'failed',
) {
  await workflows.persistWorkflowSnapshot({ ...run, snapshot: createParentSnapshot(run.runId) });
  const claim = await workflows.claimWorkflowTerminalization({
    ...run,
    eventKey: `${run.runId}-terminal`,
    terminalStatus,
    ownerId: 'owner',
    leaseMs: 10_000,
  });
  if (claim.status !== 'acquired') throw new Error('Expected acquired root');
  const snapshot: WorkflowRunState = {
    ...createParentSnapshot(run.runId),
    status: terminalStatus,
    context: { __state: { terminal: true } } as WorkflowRunState['context'],
    value: { terminal: true },
  };
  await workflows.persistWorkflowTerminalState({
    ...run,
    ownerId: claim.record.ownerId,
    claimToken: claim.record.claimToken,
    claimGeneration: claim.record.claimGeneration,
    snapshot,
    recoveryEnvelope: createTerminalRecoveryEnvelope({ ...run, snapshot, terminalStatus }),
  });
  const key = JSON.stringify([run.workflowName, run.runId]);
  const journal = db.workflowTerminalizations.get(key);
  if (!journal) throw new Error('Expected root journal');
  db.workflowTerminalizations.set(key, {
    ...journal,
    phase: 'complete',
    ownerId: undefined,
    claimToken: undefined,
    leaseExpiresAt: undefined,
    completedAt: journal.updatedAt,
  });
}

function nestedAdmission(
  parent: { workflowName: string; runId: string },
  child: { workflowName: string; runId: string },
) {
  return {
    ...parent,
    stepId: 'nested',
    nestedWorkflowName: child.workflowName,
    nestedRunId: child.runId,
    expectedChildGraphFingerprint: EMPTY_CHILD_GRAPH_FINGERPRINT,
    result: { status: 'running' as const, payload: {} },
    requestContext: {},
    recoveryAncestry: ancestry(child, parent),
  };
}

describe('WorkflowsInMemory terminal recovery storage', () => {
  it('observes missing, nonterminal, terminal, and corrupt durable run status without executing identity accessors', async () => {
    const db = new InMemoryDB();
    const workflows = new WorkflowsInMemory({ db });
    const run = { workflowName: 'status-run', runId: 'run' };
    await expect(workflows.getWorkflowRunTerminalStatus(run)).resolves.toEqual({ status: 'missing_run' });
    await workflows.persistWorkflowSnapshot({ ...run, snapshot: createEmptyWorkflowSnapshot(run.runId) });
    await expect(workflows.getWorkflowRunTerminalStatus(run)).resolves.toEqual({ status: 'nonterminal' });
    await expect(
      workflows.claimWorkflowTerminalization({
        ...run,
        eventKey: 'status-terminal',
        terminalStatus: 'failed',
        ownerId: 'status-owner',
        leaseMs: 10_000,
      }),
    ).resolves.toMatchObject({ status: 'acquired' });
    await expect(workflows.getWorkflowRunTerminalStatus(run)).resolves.toEqual({
      status: 'terminal',
      terminalStatus: 'failed',
    });

    let accessorReads = 0;
    const accessorIdentity = { runId: 'alternate-run' } as { workflowName: string; runId: string };
    Object.defineProperty(accessorIdentity, 'workflowName', {
      enumerable: true,
      get() {
        accessorReads += 1;
        return accessorReads === 1 ? run.workflowName : 'alternate-workflow';
      },
    });
    await expect(workflows.getWorkflowRunTerminalStatus(accessorIdentity)).rejects.toThrow(
      'Workflow identity workflowName must be own data',
    );
    expect(accessorReads).toBe(0);

    const key = JSON.stringify([run.workflowName, run.runId]);
    const revision = db.workflowTerminalParentRevisions.get(key);
    if (!revision) throw new Error('Expected durable status revision');
    db.workflowTerminalParentRevisions.set(key, { ...revision, terminalStatus: 'unknown' as never });
    await expect(workflows.getWorkflowRunTerminalStatus(run)).rejects.toThrow(
      'Invalid workflow parent terminal status',
    );
  });

  it('atomically admits ownership plus graph-bound ancestry and makes exact replay read-only', async () => {
    const db = new InMemoryDB();
    const workflows = new WorkflowsInMemory({ db });
    const parent = { workflowName: 'atomic-parent', runId: 'parent-run' };
    const child = { workflowName: 'atomic-child', runId: 'child-run' };
    const parentGraph: WorkflowRunState['serializedStepGraph'] = [
      { type: 'step', step: { id: 'nested', component: 'WORKFLOW' } },
    ];
    await workflows.persistWorkflowSnapshot({
      ...parent,
      snapshot: {
        ...createEmptyWorkflowSnapshot(parent.runId),
        serializedStepGraph: parentGraph,
        context: {
          nested: { status: 'running', payload: {}, metadata: {} },
        } as WorkflowRunState['context'],
      },
    });
    const childAncestry = ancestry(child, parent);
    childAncestry[0]!.parentGraphFingerprint = createWorkflowTerminalGraphFingerprint(parentGraph);
    const admission = {
      ...parent,
      stepId: 'nested',
      nestedWorkflowName: child.workflowName,
      nestedRunId: child.runId,
      expectedChildGraphFingerprint: EMPTY_CHILD_GRAPH_FINGERPRINT,
      result: { status: 'running' as const, payload: {} },
      requestContext: {},
      recoveryAncestry: childAncestry,
    };
    const initializedAdmission = {
      ...admission,
      initialChildSnapshot: {
        resourceId: 'child-resource',
        snapshot: {
          ...createEmptyWorkflowSnapshot(child.runId),
          status: 'running' as const,
          context: { __state: { winner: 'first' } } as WorkflowRunState['context'],
          value: { winner: 'first' },
        },
      },
    };

    await expect(workflows.admitWorkflowNestedRun(initializedAdmission)).resolves.toMatchObject({
      status: 'admitted',
      childSnapshotState: 'initialized',
    });
    const revisionKey = JSON.stringify([parent.workflowName, parent.runId]);
    const revision = db.workflowTerminalParentRevisions.get(revisionKey);
    await expect(
      workflows.admitWorkflowNestedRun({
        ...initializedAdmission,
        initialChildSnapshot: {
          snapshot: {
            ...initializedAdmission.initialChildSnapshot.snapshot,
            context: { __state: { winner: 'replay' } } as WorkflowRunState['context'],
            value: { winner: 'replay' },
          },
        },
      }),
    ).resolves.toMatchObject({
      status: 'already_admitted',
      childSnapshotState: 'retained',
    });
    expect(db.workflowTerminalParentRevisions.get(revisionKey)).toBe(revision);
    await expect(workflows.getWorkflowRunById(child)).resolves.toMatchObject({
      resourceId: 'child-resource',
      snapshot: { context: { __state: { winner: 'first' } }, value: { winner: 'first' } },
    });

    const conflictingChild = { workflowName: 'other-child', runId: 'other-run' };
    const conflictingAncestry = ancestry(conflictingChild, parent);
    conflictingAncestry[0]!.parentGraphFingerprint = createWorkflowTerminalGraphFingerprint(parentGraph);
    await expect(
      workflows.admitWorkflowNestedRun({
        ...admission,
        nestedWorkflowName: conflictingChild.workflowName,
        nestedRunId: conflictingChild.runId,
        recoveryAncestry: conflictingAncestry,
      }),
    ).resolves.toEqual({ status: 'ownership_conflict' });
    await expect(workflows.getWorkflowTerminalRecoveryAncestry(conflictingChild)).resolves.toEqual({
      status: 'missing_ancestry',
    });

    const forged = structuredClone(childAncestry);
    forged[0]!.parentGraphFingerprint = createWorkflowTerminalGraphFingerprint([]);
    await expect(
      workflows.admitWorkflowNestedRun({
        ...admission,
        nestedWorkflowName: 'forged-child',
        nestedRunId: 'forged-run',
        recoveryAncestry: forged.map(frame => ({
          ...frame,
          childWorkflowName: 'forged-child',
          childRunId: 'forged-run',
          resultPointer: {
            kind: 'retained-terminal-result' as const,
            workflowName: 'forged-child',
            runId: 'forged-run',
          },
        })),
      }),
    ).rejects.toThrow('does not match serialized parent graph');
    await expect(
      workflows.getWorkflowTerminalRecoveryAncestry({ workflowName: 'forged-child', runId: 'forged-run' }),
    ).resolves.toEqual({ status: 'missing_ancestry' });
  });

  it('repairs an admitted child missing its initial snapshot without permitting later replacement', async () => {
    const workflows = new WorkflowsInMemory({ db: new InMemoryDB() });
    const parent = { workflowName: 'repair-parent', runId: 'parent-run' };
    const child = { workflowName: 'repair-child', runId: 'child-run' };
    const parentGraph: WorkflowRunState['serializedStepGraph'] = [
      { type: 'step', step: { id: 'nested', component: 'WORKFLOW' } },
    ];
    await workflows.persistWorkflowSnapshot({
      ...parent,
      snapshot: { ...createEmptyWorkflowSnapshot(parent.runId), serializedStepGraph: parentGraph },
    });
    const childAncestry = ancestry(child, parent);
    childAncestry[0]!.parentGraphFingerprint = createWorkflowTerminalGraphFingerprint(parentGraph);
    const admission = {
      ...parent,
      stepId: 'nested',
      nestedWorkflowName: child.workflowName,
      nestedRunId: child.runId,
      expectedChildGraphFingerprint: EMPTY_CHILD_GRAPH_FINGERPRINT,
      result: { status: 'running' as const, payload: {} },
      requestContext: {},
      recoveryAncestry: childAncestry,
    };

    await expect(workflows.admitWorkflowNestedRun(admission)).resolves.toMatchObject({
      status: 'admitted',
      childSnapshotState: 'not_requested',
    });
    await expect(workflows.loadWorkflowSnapshot(child)).resolves.toBeNull();
    const repair = {
      ...admission,
      initialChildSnapshot: {
        snapshot: {
          ...createEmptyWorkflowSnapshot(child.runId),
          status: 'running' as const,
          context: { __state: { checkpoint: 'repaired' } } as WorkflowRunState['context'],
          value: { checkpoint: 'repaired' },
        },
      },
    };
    await expect(workflows.admitWorkflowNestedRun(repair)).resolves.toMatchObject({
      status: 'already_admitted',
      childSnapshotState: 'initialized',
    });
    await expect(
      workflows.admitWorkflowNestedRun({
        ...repair,
        initialChildSnapshot: {
          snapshot: {
            ...repair.initialChildSnapshot.snapshot,
            value: { checkpoint: 'must-not-replace' },
          },
        },
      }),
    ).resolves.toMatchObject({ status: 'already_admitted', childSnapshotState: 'retained' });
    await expect(workflows.loadWorkflowSnapshot(child)).resolves.toMatchObject({
      context: { __state: { checkpoint: 'repaired' } },
      value: { checkpoint: 'repaired' },
    });
  });

  it('does not resurrect a terminal child whose canonical snapshot was deleted', async () => {
    const db = new InMemoryDB();
    const workflows = new WorkflowsInMemory({ db });
    const parent = { workflowName: 'terminal-child-parent', runId: 'parent-run' };
    const child = { workflowName: 'terminal-child', runId: 'child-run' };
    await workflows.persistWorkflowSnapshot({ ...parent, snapshot: createParentSnapshot(parent.runId) });
    await workflows.persistWorkflowSnapshot({ ...child, snapshot: createEmptyWorkflowSnapshot(child.runId) });
    await expect(
      workflows.claimWorkflowTerminalization({
        ...child,
        eventKey: 'terminal-child-event',
        terminalStatus: 'failed',
        ownerId: 'terminal-child-owner',
        leaseMs: 10_000,
      }),
    ).resolves.toMatchObject({ status: 'acquired' });
    const childKey = JSON.stringify([child.workflowName, child.runId]);
    db.workflows.delete(childKey);
    const parentBefore = await workflows.loadWorkflowSnapshot(parent);
    await expect(workflows.getWorkflowRunTerminalStatus(child)).resolves.toEqual({
      status: 'terminal',
      terminalStatus: 'failed',
    });

    await expect(
      workflows.admitWorkflowNestedRun({
        ...nestedAdmission(parent, child),
        initialChildSnapshot: { snapshot: { ...createEmptyWorkflowSnapshot(child.runId), status: 'running' } },
      }),
    ).resolves.toEqual({ status: 'child_terminal' });

    await expect(workflows.loadWorkflowSnapshot(child)).resolves.toBeNull();
    await expect(workflows.getWorkflowTerminalRecoveryAncestry(child)).resolves.toEqual({
      status: 'missing_ancestry',
    });
    await expect(workflows.loadWorkflowSnapshot(parent)).resolves.toEqual(parentBefore);
  });

  it.each([
    ['wrong run identity', { runId: 'forged-run' }],
    ['unknown status', { status: 'unknown' }],
    ['graph drift', { serializedStepGraph: [{ type: 'step', step: { id: 'drifted', component: 'WORKFLOW' } }] }],
  ] as const)('rejects retained child %s without partial admission writes', async (_label, patch) => {
    const db = new InMemoryDB();
    const workflows = new WorkflowsInMemory({ db });
    const parent = { workflowName: `conflict-parent-${_label}`, runId: 'parent-run' };
    const child = { workflowName: `conflict-child-${_label}`, runId: 'child-run' };
    await workflows.persistWorkflowSnapshot({ ...parent, snapshot: createParentSnapshot(parent.runId) });
    await workflows.persistWorkflowSnapshot({
      ...child,
      snapshot: { ...createEmptyWorkflowSnapshot(child.runId), ...patch } as WorkflowRunState,
    });
    const childKey = JSON.stringify([child.workflowName, child.runId]);
    db.workflowTerminalParentRevisions.delete(childKey);
    const parentBefore = await workflows.loadWorkflowSnapshot(parent);

    await expect(workflows.admitWorkflowNestedRun(nestedAdmission(parent, child))).resolves.toEqual({
      status: 'child_snapshot_conflict',
    });

    expect(db.workflowTerminalParentRevisions.has(childKey)).toBe(false);
    await expect(workflows.getWorkflowTerminalRecoveryAncestry(child)).resolves.toEqual({
      status: 'missing_ancestry',
    });
    await expect(workflows.loadWorkflowSnapshot(parent)).resolves.toEqual(parentBefore);
  });

  it.each(['own', 'inherited'] as const)(
    'rejects an initial child snapshot with a nested %s toJSON hook without executing or writing it',
    async placement => {
      const db = new InMemoryDB();
      const workflows = new WorkflowsInMemory({ db });
      const parent = { workflowName: `tojson-parent-${placement}`, runId: 'parent-run' };
      const child = { workflowName: `tojson-child-${placement}`, runId: 'child-run' };
      await workflows.persistWorkflowSnapshot({ ...parent, snapshot: createParentSnapshot(parent.runId) });
      const parentBefore = await workflows.loadWorkflowSnapshot(parent);
      let calls = 0;
      const toJSON = () => {
        calls += 1;
        return { runId: 'forged-run' };
      };
      const payload = {};
      if (placement === 'own') {
        Object.defineProperty(payload, 'toJSON', { configurable: true, value: toJSON });
      } else {
        Object.setPrototypeOf(payload, { toJSON });
      }
      const snapshot = {
        ...createEmptyWorkflowSnapshot(child.runId),
        status: 'running' as const,
        context: { nested: { status: 'success', output: payload } } as WorkflowRunState['context'],
        value: { payload },
      };

      await expect(
        workflows.admitWorkflowNestedRun({
          ...nestedAdmission(parent, child),
          initialChildSnapshot: { snapshot },
        }),
      ).rejects.toThrow('Invalid workflow terminal recovery data at initialChildSnapshot.snapshot');
      expect(calls).toBe(0);
      await expect(workflows.loadWorkflowSnapshot(child)).resolves.toBeNull();
      await expect(workflows.getWorkflowTerminalRecoveryAncestry(child)).resolves.toEqual({
        status: 'missing_ancestry',
      });
      await expect(workflows.loadWorkflowSnapshot(parent)).resolves.toEqual(parentBefore);
    },
  );

  it('persists immutable ancestry without requiring a canonical child run', async () => {
    const workflows = new WorkflowsInMemory({ db: new InMemoryDB() });
    const parent = { workflowName: 'parent', runId: 'parent-run' };
    const child = { workflowName: 'child', runId: 'child-run' };
    await workflows.persistWorkflowSnapshot({ ...parent, snapshot: createParentSnapshot(parent.runId) });
    const input = ancestry(child, parent);
    await expect(
      workflows.persistWorkflowTerminalRecoveryAncestry({ ...child, ancestry: input }),
    ).resolves.toMatchObject({ status: 'persisted' });
    input[0]!.source = { kind: 'step', stepId: 'forged', executionPath: [9] };
    await expect(workflows.getWorkflowTerminalRecoveryAncestry(child)).resolves.toMatchObject({
      status: 'found',
      record: { ancestry: [{ source: { stepId: 'nested', executionPath: [0] } }] },
    });
    await expect(
      workflows.persistWorkflowTerminalRecoveryAncestry({ ...child, ancestry: ancestry(child, parent) }),
    ).resolves.toMatchObject({ status: 'already_persisted' });
  });

  it('preserves a completed root when ancestry wins and rejects orphan admission after cleanup wins', async () => {
    const db = new InMemoryDB();
    const workflows = new WorkflowsInMemory({ db });
    const protectedRoot = { workflowName: 'protected-root', runId: 'root-run' };
    const protectedChild = { workflowName: 'protected-child', runId: 'child-run' };
    await workflows.persistWorkflowSnapshot({
      ...protectedRoot,
      snapshot: createParentSnapshot(protectedRoot.runId),
    });
    await workflows.persistWorkflowTerminalRecoveryAncestry({
      ...protectedChild,
      ancestry: ancestry(protectedChild, protectedRoot),
    });
    await completeRoot(workflows, db, protectedRoot);
    await expect(
      workflows.deleteCompletedWorkflowTerminalizations({ ...protectedRoot, olderThan: new Date(Date.now() + 1) }),
    ).resolves.toEqual({ status: 'deleted', count: 0 });

    const deletedRoot = { workflowName: 'deleted-root', runId: 'root-run' };
    const orphanChild = { workflowName: 'orphan-child', runId: 'child-run' };
    await completeRoot(workflows, db, deletedRoot);
    await expect(
      workflows.deleteCompletedWorkflowTerminalizations({ ...deletedRoot, olderThan: new Date(Date.now() + 1) }),
    ).resolves.toEqual({ status: 'deleted', count: 1 });
    await expect(
      workflows.persistWorkflowTerminalRecoveryAncestry({
        ...orphanChild,
        ancestry: ancestry(orphanChild, deletedRoot),
      }),
    ).rejects.toThrow('parent evidence is unavailable');
  });

  it.each(['success', 'failed', 'canceled'] as const)(
    'rejects late atomic admission after cleanup of a %s parent without mutating canonical state',
    async terminalStatus => {
      const db = new InMemoryDB();
      const workflows = new WorkflowsInMemory({ db });
      const parent = { workflowName: `late-${terminalStatus}-parent`, runId: 'parent-run' };
      const child = { workflowName: `late-${terminalStatus}-child`, runId: 'child-run' };
      await completeRoot(workflows, db, parent, terminalStatus);
      await expect(
        workflows.deleteCompletedWorkflowTerminalizations({ ...parent, olderThan: new Date(Date.now() + 1) }),
      ).resolves.toEqual({ status: 'deleted', count: 1 });
      const before = JSON.stringify(await workflows.loadWorkflowSnapshot(parent));

      await expect(workflows.admitWorkflowNestedRun(nestedAdmission(parent, child))).resolves.toEqual({
        status: 'parent_terminal',
      });
      expect(JSON.stringify(await workflows.loadWorkflowSnapshot(parent))).toBe(before);
      await expect(workflows.getWorkflowTerminalRecoveryAncestry(child)).resolves.toEqual({
        status: 'missing_ancestry',
      });
    },
  );

  it.each(['tripwire', 'bailed'] as const)(
    'rejects ancestry and atomic admission against a directly persisted %s parent',
    async terminalStatus => {
      const db = new InMemoryDB();
      const workflows = new WorkflowsInMemory({ db });
      const parent = { workflowName: `direct-${terminalStatus}-parent`, runId: 'parent-run' };
      const ancestryChild = { workflowName: `direct-${terminalStatus}-ancestry`, runId: 'child-run' };
      const admissionChild = { workflowName: `direct-${terminalStatus}-admission`, runId: 'child-run' };
      await workflows.persistWorkflowSnapshot({
        ...parent,
        snapshot: { ...createParentSnapshot(parent.runId), status: terminalStatus },
      });

      await expect(
        workflows.persistWorkflowTerminalRecoveryAncestry({
          ...ancestryChild,
          ancestry: ancestry(ancestryChild, parent),
        }),
      ).rejects.toThrow('parent evidence is unavailable');
      await expect(workflows.admitWorkflowNestedRun(nestedAdmission(parent, admissionChild))).resolves.toEqual({
        status: 'parent_terminal',
      });
      expect(db.workflowTerminalParentRevisions.get(JSON.stringify([parent.workflowName, parent.runId]))).toMatchObject(
        {
          terminalStatus,
        },
      );
    },
  );

  it('makes the first terminal claim authoritative before terminal snapshot persistence', async () => {
    const db = new InMemoryDB();
    const workflows = new WorkflowsInMemory({ db });
    const parent = { workflowName: 'claimed-terminal-parent', runId: 'parent-run' };
    const ancestryChild = { workflowName: 'claimed-terminal-ancestry', runId: 'child-run' };
    const admissionChild = { workflowName: 'claimed-terminal-admission', runId: 'child-run' };
    await workflows.persistWorkflowSnapshot({ ...parent, snapshot: createParentSnapshot(parent.runId) });

    await expect(
      workflows.claimWorkflowTerminalization({
        ...parent,
        eventKey: 'parent-terminal',
        terminalStatus: 'failed',
        ownerId: 'owner',
        leaseMs: 10_000,
      }),
    ).resolves.toMatchObject({ status: 'acquired' });

    expect(db.workflowTerminalParentRevisions.get(JSON.stringify([parent.workflowName, parent.runId]))).toMatchObject({
      terminalStatus: 'failed',
    });
    await expect(
      workflows.persistWorkflowTerminalRecoveryAncestry({
        ...ancestryChild,
        ancestry: ancestry(ancestryChild, parent),
      }),
    ).rejects.toThrow('parent evidence is unavailable');
    await expect(workflows.admitWorkflowNestedRun(nestedAdmission(parent, admissionChild))).resolves.toEqual({
      status: 'parent_terminal',
    });
  });

  it('retains terminal ancestry exclusion after journal cleanup and a running snapshot rewrite', async () => {
    const db = new InMemoryDB();
    const workflows = new WorkflowsInMemory({ db });
    const parent = { workflowName: 'terminal-marker-parent', runId: 'parent-run' };
    const ancestryChild = { workflowName: 'terminal-marker-ancestry', runId: 'child-run' };
    const admissionChild = { workflowName: 'terminal-marker-admission', runId: 'child-run' };
    await completeRoot(workflows, db, parent, 'failed');
    await expect(
      workflows.deleteCompletedWorkflowTerminalizations({ ...parent, olderThan: new Date(Date.now() + 1) }),
    ).resolves.toEqual({ status: 'deleted', count: 1 });

    await workflows.persistWorkflowSnapshot({
      ...parent,
      snapshot: { ...createParentSnapshot(parent.runId), status: 'running' },
    });
    await expect(workflows.loadWorkflowSnapshot(parent)).resolves.toMatchObject({ status: 'running' });
    await expect(
      workflows.persistWorkflowTerminalRecoveryAncestry({
        ...ancestryChild,
        ancestry: ancestry(ancestryChild, parent),
      }),
    ).rejects.toThrow('parent evidence is unavailable');
    await expect(workflows.admitWorkflowNestedRun(nestedAdmission(parent, admissionChild))).resolves.toEqual({
      status: 'parent_terminal',
    });
    expect(db.workflowTerminalParentRevisions.get(JSON.stringify([parent.workflowName, parent.runId]))).toMatchObject({
      terminalStatus: 'failed',
    });
  });

  it('fails cleanup before deleting evidence when the terminal marker contradicts the journal', async () => {
    const db = new InMemoryDB();
    const workflows = new WorkflowsInMemory({ db });
    const parent = { workflowName: 'conflicting-terminal-marker-parent', runId: 'parent-run' };
    await completeRoot(workflows, db, parent, 'failed');
    const key = JSON.stringify([parent.workflowName, parent.runId]);
    const revision = db.workflowTerminalParentRevisions.get(key);
    if (!revision) throw new Error('Expected parent revision');
    db.workflowTerminalParentRevisions.set(key, { ...revision, terminalStatus: 'success' });

    await expect(
      workflows.deleteCompletedWorkflowTerminalizations({ ...parent, olderThan: new Date(Date.now() + 1) }),
    ).rejects.toThrow('terminal marker conflicts with authoritative terminal status');

    expect(db.workflowTerminalizations.has(key)).toBe(true);
    expect(db.workflowTerminalSnapshots.has(key)).toBe(true);
    expect(db.workflowTerminalParentRevisions.get(key)).toMatchObject({ terminalStatus: 'success' });
  });

  it('retains a completed parent recovery root when atomic admission wins first', async () => {
    const db = new InMemoryDB();
    const workflows = new WorkflowsInMemory({ db });
    const parent = { workflowName: 'admission-first-parent', runId: 'parent-run' };
    const child = { workflowName: 'admission-first-child', runId: 'child-run' };
    await workflows.persistWorkflowSnapshot({ ...parent, snapshot: createParentSnapshot(parent.runId) });
    await expect(workflows.admitWorkflowNestedRun(nestedAdmission(parent, child))).resolves.toMatchObject({
      status: 'admitted',
    });
    const claim = await workflows.claimWorkflowTerminalization({
      ...parent,
      eventKey: 'admission-first-terminal',
      terminalStatus: 'failed',
      ownerId: 'owner',
      leaseMs: 10_000,
    });
    if (claim.status !== 'acquired') throw new Error('Expected acquired parent');
    const current = await workflows.loadWorkflowSnapshot(parent);
    if (!current) throw new Error('Expected admitted parent snapshot');
    const terminal: WorkflowRunState = {
      ...current,
      status: 'failed',
      context: { ...current.context, __state: { terminal: true } },
      value: { terminal: true },
    };
    await workflows.persistWorkflowTerminalState({
      ...parent,
      ownerId: claim.record.ownerId,
      claimToken: claim.record.claimToken,
      claimGeneration: claim.record.claimGeneration,
      snapshot: terminal,
      recoveryEnvelope: createTerminalRecoveryEnvelope({ ...parent, snapshot: terminal, terminalStatus: 'failed' }),
    });
    const key = JSON.stringify([parent.workflowName, parent.runId]);
    const journal = db.workflowTerminalizations.get(key);
    if (!journal) throw new Error('Expected parent journal');
    db.workflowTerminalizations.set(key, {
      ...journal,
      phase: 'complete',
      ownerId: undefined,
      claimToken: undefined,
      leaseExpiresAt: undefined,
      completedAt: journal.updatedAt,
    });

    await expect(
      workflows.deleteCompletedWorkflowTerminalizations({ ...parent, olderThan: new Date(Date.now() + 1) }),
    ).resolves.toEqual({ status: 'deleted', count: 0 });
    await expect(workflows.getWorkflowTerminalRecoveryAncestry(child)).resolves.toMatchObject({ status: 'found' });
  });

  it('atomically preserves foreach ownership assigned by concurrent sibling calls', async () => {
    const workflows = new WorkflowsInMemory({ db: new InMemoryDB() });
    const run = { workflowName: 'owner-parent', runId: 'parent-run' };
    const graph: WorkflowRunState['serializedStepGraph'] = [
      { type: 'foreach', step: { id: 'each', component: 'WORKFLOW' }, opts: { concurrency: 2 } },
    ];
    const snapshot: WorkflowRunState = {
      ...createEmptyWorkflowSnapshot(run.runId),
      status: 'running',
      serializedStepGraph: graph,
      context: {
        each: { status: 'running', payload: ['a', 'b'], output: [null, null], metadata: {} },
      } as WorkflowRunState['context'],
    };
    await workflows.persistWorkflowSnapshot({ ...run, snapshot });
    const admission = (forEachIndex: number, nestedRunId: string) => {
      const child = { workflowName: 'each', runId: nestedRunId };
      return {
        ...run,
        stepId: 'each',
        nestedWorkflowName: child.workflowName,
        nestedRunId,
        expectedChildGraphFingerprint: EMPTY_CHILD_GRAPH_FINGERPRINT,
        forEachIndex,
        result: { status: 'running' as const, payload: ['a', 'b'], output: [null, null] },
        requestContext: {},
        recoveryAncestry: [
          {
            version: 1 as const,
            childWorkflowName: child.workflowName,
            childRunId: child.runId,
            parentWorkflowName: run.workflowName,
            parentRunId: run.runId,
            parentGraphFingerprint: createWorkflowTerminalGraphFingerprint(graph),
            source: {
              kind: 'foreach-iteration' as const,
              stepId: 'each',
              containerPath: [0],
              iterationIndex: forEachIndex,
            },
            inputPointer: { kind: 'parent-source-payload' as const, stepId: 'each' },
            resultPointer: {
              kind: 'retained-terminal-result' as const,
              workflowName: child.workflowName,
              runId: child.runId,
            },
            resumeMetadata: { wasResume: false, resumeSteps: [] },
          },
        ],
      };
    };
    await expect(
      Promise.all([
        workflows.admitWorkflowNestedRun(admission(0, 'child-a')),
        workflows.admitWorkflowNestedRun(admission(1, 'child-b')),
      ]),
    ).resolves.toMatchObject([{ status: 'admitted' }, { status: 'admitted' }]);
    await expect(workflows.admitWorkflowNestedRun(admission(0, 'child-c'))).resolves.toEqual({
      status: 'ownership_conflict',
    });
    await expect(
      workflows.getWorkflowTerminalRecoveryAncestry({ workflowName: 'each', runId: 'child-c' }),
    ).resolves.toEqual({ status: 'missing_ancestry' });
    await expect(workflows.loadWorkflowSnapshot(run)).resolves.toMatchObject({
      context: {
        each: { metadata: { __workflow_meta: { iterationRunIds: { '0': 'child-a', '1': 'child-b' } } } },
      },
    });
  });
});
