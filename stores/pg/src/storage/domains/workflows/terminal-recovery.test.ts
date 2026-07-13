import { randomUUID } from 'node:crypto';
import {
  InMemoryDB,
  WORKFLOW_TERMINAL_FOREACH_RUN_KEY,
  WorkflowsInMemory,
  createEmptyWorkflowSnapshot,
  createWorkflowTerminalGraphFingerprint,
  validateWorkflowTerminalEffectIntegrity,
} from '@mastra/core/storage';
import type { WorkflowRunState, WorkflowTerminalRecoveryAncestryV1 } from '@mastra/core/workflows';
import { getWorkflowTerminalRecoveryEnvelopeHash } from '@mastra/core/workflows';
import { Pool } from 'pg';
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';
import { createTerminalRecoveryEnvelope } from './terminalization-test-utils';
import { WorkflowsPG } from '.';

const NESTED_PARENT_GRAPH: WorkflowRunState['serializedStepGraph'] = [
  { type: 'step', step: { id: 'nested', component: 'WORKFLOW' } },
];
const EMPTY_CHILD_GRAPH_FINGERPRINT = createWorkflowTerminalGraphFingerprint([]);

function createParentSnapshot(runId: string): WorkflowRunState {
  return { ...createEmptyWorkflowSnapshot(runId), serializedStepGraph: NESTED_PARENT_GRAPH };
}

describe('WorkflowsPG terminal recovery envelope parity', () => {
  const pool = new Pool({
    host: process.env.POSTGRES_HOST || '127.0.0.1',
    port: Number(process.env.POSTGRES_PORT) || 5434,
    database: process.env.POSTGRES_DB || 'postgres',
    user: process.env.POSTGRES_USER || 'postgres',
    password: process.env.POSTGRES_PASSWORD || 'postgres',
  });
  const workflowsA = new WorkflowsPG({ pool });
  const workflowsB = new WorkflowsPG({ pool });

  beforeAll(async () => {
    await workflowsA.init();
  });

  afterAll(async () => {
    await pool.end();
  });

  async function cleanup(workflowNames: string[]): Promise<void> {
    for (const workflowName of workflowNames) {
      await pool.query(`DELETE FROM mastra_workflow_terminal_continuation_plans_v2 WHERE workflow_name = $1`, [
        workflowName,
      ]);
      await pool.query(`DELETE FROM mastra_workflow_terminal_destination_receipts_v2 WHERE workflow_name = $1`, [
        workflowName,
      ]);
      await pool.query(`DELETE FROM mastra_workflow_terminal_effects_v2 WHERE workflow_name = $1`, [workflowName]);
      await pool.query(`DELETE FROM mastra_workflow_terminal_snapshots_v2 WHERE workflow_name = $1`, [workflowName]);
      await pool.query(`DELETE FROM mastra_workflow_terminal_recovery_ancestries WHERE workflow_name = $1`, [
        workflowName,
      ]);
      await pool.query(`DELETE FROM mastra_workflow_terminalizations WHERE workflow_name = $1`, [workflowName]);
      await pool.query(`DELETE FROM mastra_workflow_snapshot WHERE workflow_name = $1`, [workflowName]);
      await pool.query(`DELETE FROM mastra_workflow_parent_revisions WHERE workflow_name = $1`, [workflowName]);
    }
  }

  function ancestry(
    child: { workflowName: string; runId: string },
    parent: { workflowName: string; runId: string },
    source: WorkflowTerminalRecoveryAncestryV1[0]['source'] = {
      kind: 'step',
      stepId: 'nested',
      executionPath: [0],
    },
  ): WorkflowTerminalRecoveryAncestryV1 {
    return [
      {
        version: 1,
        childWorkflowName: child.workflowName,
        childRunId: child.runId,
        parentWorkflowName: parent.workflowName,
        parentRunId: parent.runId,
        parentGraphFingerprint: createWorkflowTerminalGraphFingerprint(NESTED_PARENT_GRAPH),
        source,
        inputPointer: { kind: 'parent-source-payload', stepId: source.stepId },
        resultPointer: { kind: 'retained-terminal-result', workflowName: child.workflowName, runId: child.runId },
        resumeMetadata: { wasResume: false, resumeSteps: [] },
      },
    ];
  }

  async function claim(
    workflows: WorkflowsPG,
    run: { workflowName: string; runId: string },
    terminalStatus: 'success' | 'failed' | 'canceled' = 'failed',
  ) {
    const result = await workflows.claimWorkflowTerminalization({
      ...run,
      eventKey: `${run.runId}-terminal`,
      terminalStatus,
      ownerId: `${run.runId}-owner`,
      leaseMs: 10_000,
    });
    if (result.status !== 'acquired') throw new Error(`Expected acquired, received ${result.status}`);
    return {
      ...run,
      ownerId: result.record.ownerId,
      claimToken: result.record.claimToken,
      claimGeneration: result.record.claimGeneration,
    };
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

  it('observes missing, nonterminal, and terminal durable run status after canonical row deletion', async () => {
    const suffix = randomUUID();
    const run = { workflowName: `status-run-${suffix}`, runId: 'run' };
    try {
      await expect(workflowsA.getWorkflowRunTerminalStatus(run)).resolves.toEqual({ status: 'missing_run' });
      await workflowsA.persistWorkflowSnapshot({ ...run, snapshot: createEmptyWorkflowSnapshot(run.runId) });
      await expect(workflowsA.getWorkflowRunTerminalStatus(run)).resolves.toEqual({ status: 'nonterminal' });
      await expect(
        workflowsA.claimWorkflowTerminalization({
          ...run,
          eventKey: 'status-terminal',
          terminalStatus: 'failed',
          ownerId: 'status-owner',
          leaseMs: 10_000,
        }),
      ).resolves.toMatchObject({ status: 'acquired' });
      await pool.query(`DELETE FROM mastra_workflow_snapshot WHERE workflow_name = $1 AND run_id = $2`, [
        run.workflowName,
        run.runId,
      ]);
      await expect(workflowsB.getWorkflowRunTerminalStatus(run)).resolves.toEqual({
        status: 'terminal',
        terminalStatus: 'failed',
      });
    } finally {
      await cleanup([run.workflowName]);
    }
  });

  it('atomically admits graph-bound ownership and ancestry with read-only exact replay', async () => {
    const suffix = randomUUID();
    const parent = { workflowName: `admit-parent-${suffix}`, runId: 'parent-run' };
    const child = { workflowName: `admit-child-${suffix}`, runId: 'child-run' };
    const parentGraph: WorkflowRunState['serializedStepGraph'] = [
      { type: 'step', step: { id: 'nested', component: 'WORKFLOW' } },
    ];
    await workflowsA.persistWorkflowSnapshot({
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
    try {
      await expect(workflowsA.admitWorkflowNestedRun(admission)).resolves.toMatchObject({
        status: 'admitted',
        childSnapshotState: 'not_requested',
      });
      const before = await pool.query(
        `SELECT generation FROM mastra_workflow_parent_revisions WHERE workflow_name = $1 AND run_id = $2`,
        [parent.workflowName, parent.runId],
      );
      const repairSnapshot = {
        ...createEmptyWorkflowSnapshot(child.runId),
        status: 'running' as const,
        context: { __state: { checkpoint: 'repaired' } } as WorkflowRunState['context'],
        value: { checkpoint: 'repaired' },
      };
      await expect(
        workflowsB.admitWorkflowNestedRun({
          ...admission,
          initialChildSnapshot: { resourceId: 'repaired-resource', snapshot: repairSnapshot },
        }),
      ).resolves.toMatchObject({
        status: 'already_admitted',
        childSnapshotState: 'initialized',
      });
      const after = await pool.query(
        `SELECT generation FROM mastra_workflow_parent_revisions WHERE workflow_name = $1 AND run_id = $2`,
        [parent.workflowName, parent.runId],
      );
      expect(after.rows[0]?.generation).toBe(before.rows[0]?.generation);
      await expect(
        workflowsA.admitWorkflowNestedRun({
          ...admission,
          initialChildSnapshot: {
            snapshot: { ...repairSnapshot, value: { checkpoint: 'must-not-replace' } },
          },
        }),
      ).resolves.toMatchObject({ status: 'already_admitted', childSnapshotState: 'retained' });
      await expect(workflowsA.getWorkflowRunById(child)).resolves.toMatchObject({
        resourceId: 'repaired-resource',
        snapshot: {
          context: { __state: { checkpoint: 'repaired' } },
          value: { checkpoint: 'repaired' },
        },
      });

      const conflictingChild = { workflowName: `other-child-${suffix}`, runId: 'other-run' };
      const conflictingAncestry = ancestry(conflictingChild, parent);
      conflictingAncestry[0]!.parentGraphFingerprint = createWorkflowTerminalGraphFingerprint(parentGraph);
      await expect(
        workflowsB.admitWorkflowNestedRun({
          ...admission,
          nestedWorkflowName: conflictingChild.workflowName,
          nestedRunId: conflictingChild.runId,
          recoveryAncestry: conflictingAncestry,
        }),
      ).resolves.toEqual({ status: 'ownership_conflict' });
      await expect(workflowsA.getWorkflowTerminalRecoveryAncestry(conflictingChild)).resolves.toEqual({
        status: 'missing_ancestry',
      });

      const forgedChild = { workflowName: `forged-child-${suffix}`, runId: 'forged-run' };
      const forgedAncestry = ancestry(forgedChild, parent);
      forgedAncestry[0]!.parentGraphFingerprint = createWorkflowTerminalGraphFingerprint([]);
      await expect(
        workflowsA.admitWorkflowNestedRun({
          ...admission,
          nestedWorkflowName: forgedChild.workflowName,
          nestedRunId: forgedChild.runId,
          recoveryAncestry: forgedAncestry,
        }),
      ).rejects.toThrow('does not match serialized parent graph');
      await expect(workflowsA.getWorkflowTerminalRecoveryAncestry(forgedChild)).resolves.toEqual({
        status: 'missing_ancestry',
      });
    } finally {
      await cleanup([parent.workflowName, child.workflowName, `other-child-${suffix}`, `forged-child-${suffix}`]);
    }
  });

  it('atomically converges concurrent child initialization without replacing the winner', async () => {
    const suffix = randomUUID();
    const parent = { workflowName: `init-race-parent-${suffix}`, runId: 'parent-run' };
    const child = { workflowName: `init-race-child-${suffix}`, runId: 'child-run' };
    const parentGraph: WorkflowRunState['serializedStepGraph'] = [
      { type: 'step', step: { id: 'nested', component: 'WORKFLOW' } },
    ];
    await workflowsA.persistWorkflowSnapshot({
      ...parent,
      snapshot: {
        ...createEmptyWorkflowSnapshot(parent.runId),
        serializedStepGraph: parentGraph,
        context: { nested: { status: 'running', payload: {}, metadata: {} } } as WorkflowRunState['context'],
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
    const withInitial = (winner: string) => ({
      ...admission,
      initialChildSnapshot: {
        resourceId: `resource-${winner}`,
        snapshot: {
          ...createEmptyWorkflowSnapshot(child.runId),
          status: 'running' as const,
          context: { __state: { winner } } as WorkflowRunState['context'],
          value: { winner },
        },
      },
    });

    try {
      const results = await Promise.all([
        workflowsA.admitWorkflowNestedRun(withInitial('a')),
        workflowsB.admitWorkflowNestedRun(withInitial('b')),
      ]);
      expect(results.map(result => result.status).sort()).toEqual(['admitted', 'already_admitted']);
      expect(
        results
          .flatMap(result =>
            result.status === 'admitted' || result.status === 'already_admitted' ? [result.childSnapshotState] : [],
          )
          .sort(),
      ).toEqual(['initialized', 'retained']);
      const retained = await workflowsA.getWorkflowRunById(child);
      expect(retained).toMatchObject({
        resourceId: expect.stringMatching(/^resource-[ab]$/),
        snapshot: { context: { __state: { winner: expect.stringMatching(/^[ab]$/) } } },
      });
      const winner = (retained?.snapshot as WorkflowRunState).value as { winner: string };

      await expect(workflowsB.admitWorkflowNestedRun(withInitial('replay'))).resolves.toMatchObject({
        status: 'already_admitted',
        childSnapshotState: 'retained',
      });
      await expect(workflowsA.getWorkflowRunById(child)).resolves.toMatchObject({
        resourceId: `resource-${winner.winner}`,
        snapshot: { context: { __state: { winner: winner.winner } }, value: { winner: winner.winner } },
      });
    } finally {
      await cleanup([parent.workflowName, child.workflowName]);
    }
  });

  it('does not resurrect a terminal child whose canonical snapshot was deleted', async () => {
    const suffix = randomUUID();
    const parent = { workflowName: `terminal-child-parent-${suffix}`, runId: 'parent-run' };
    const child = { workflowName: `terminal-child-${suffix}`, runId: 'child-run' };
    try {
      await workflowsA.persistWorkflowSnapshot({ ...parent, snapshot: createParentSnapshot(parent.runId) });
      await workflowsA.persistWorkflowSnapshot({ ...child, snapshot: createEmptyWorkflowSnapshot(child.runId) });
      await expect(
        workflowsA.claimWorkflowTerminalization({
          ...child,
          eventKey: 'terminal-child-event',
          terminalStatus: 'failed',
          ownerId: 'terminal-child-owner',
          leaseMs: 10_000,
        }),
      ).resolves.toMatchObject({ status: 'acquired' });
      await pool.query(`DELETE FROM mastra_workflow_snapshot WHERE workflow_name = $1 AND run_id = $2`, [
        child.workflowName,
        child.runId,
      ]);
      const parentBefore = await workflowsA.loadWorkflowSnapshot(parent);
      await expect(workflowsA.getWorkflowRunTerminalStatus(child)).resolves.toEqual({
        status: 'terminal',
        terminalStatus: 'failed',
      });

      await expect(
        workflowsB.admitWorkflowNestedRun({
          ...nestedAdmission(parent, child),
          initialChildSnapshot: { snapshot: { ...createEmptyWorkflowSnapshot(child.runId), status: 'running' } },
        }),
      ).resolves.toEqual({ status: 'child_terminal' });

      await expect(workflowsA.loadWorkflowSnapshot(child)).resolves.toBeNull();
      await expect(workflowsA.getWorkflowTerminalRecoveryAncestry(child)).resolves.toEqual({
        status: 'missing_ancestry',
      });
      await expect(workflowsA.loadWorkflowSnapshot(parent)).resolves.toEqual(parentBefore);
    } finally {
      await cleanup([parent.workflowName, child.workflowName]);
    }
  });

  it.each([
    ['wrong-run', { runId: 'forged-run' }],
    ['unknown-status', { status: 'unknown' }],
    ['graph-drift', { serializedStepGraph: [{ type: 'step', step: { id: 'drifted', component: 'WORKFLOW' } }] }],
  ] as const)('rejects retained child %s without partial admission writes', async (label, patch) => {
    const suffix = randomUUID();
    const parent = { workflowName: `conflict-parent-${label}-${suffix}`, runId: 'parent-run' };
    const child = { workflowName: `conflict-child-${label}-${suffix}`, runId: 'child-run' };
    try {
      await workflowsA.persistWorkflowSnapshot({ ...parent, snapshot: createParentSnapshot(parent.runId) });
      await workflowsA.persistWorkflowSnapshot({
        ...child,
        snapshot: { ...createEmptyWorkflowSnapshot(child.runId), ...patch } as WorkflowRunState,
      });
      await pool.query(`DELETE FROM mastra_workflow_parent_revisions WHERE workflow_name = $1 AND run_id = $2`, [
        child.workflowName,
        child.runId,
      ]);
      const parentBefore = await workflowsA.loadWorkflowSnapshot(parent);

      await expect(workflowsB.admitWorkflowNestedRun(nestedAdmission(parent, child))).resolves.toEqual({
        status: 'child_snapshot_conflict',
      });

      const revision = await pool.query(
        `SELECT generation FROM mastra_workflow_parent_revisions WHERE workflow_name = $1 AND run_id = $2`,
        [child.workflowName, child.runId],
      );
      expect(revision.rowCount).toBe(0);
      await expect(workflowsA.getWorkflowTerminalRecoveryAncestry(child)).resolves.toEqual({
        status: 'missing_ancestry',
      });
      await expect(workflowsA.loadWorkflowSnapshot(parent)).resolves.toEqual(parentBefore);
    } finally {
      await cleanup([parent.workflowName, child.workflowName]);
    }
  });

  it.each(['own', 'inherited'] as const)(
    'rejects an initial child snapshot with a nested %s toJSON hook without executing or writing it',
    async placement => {
      const suffix = randomUUID();
      const parent = { workflowName: `tojson-parent-${placement}-${suffix}`, runId: 'parent-run' };
      const child = { workflowName: `tojson-child-${placement}-${suffix}`, runId: 'child-run' };
      try {
        await workflowsA.persistWorkflowSnapshot({ ...parent, snapshot: createParentSnapshot(parent.runId) });
        const parentBefore = await workflowsA.loadWorkflowSnapshot(parent);
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
          workflowsB.admitWorkflowNestedRun({
            ...nestedAdmission(parent, child),
            initialChildSnapshot: { snapshot },
          }),
        ).rejects.toThrow('Invalid workflow terminal recovery data at initialChildSnapshot.snapshot');
        expect(calls).toBe(0);
        await expect(workflowsA.loadWorkflowSnapshot(child)).resolves.toBeNull();
        await expect(workflowsA.getWorkflowTerminalRecoveryAncestry(child)).resolves.toEqual({
          status: 'missing_ancestry',
        });
        await expect(workflowsA.loadWorkflowSnapshot(parent)).resolves.toEqual(parentBefore);
      } finally {
        await cleanup([parent.workflowName, child.workflowName]);
      }
    },
  );

  it.each([
    ['scalar', undefined],
    ['foreach', 2],
  ] as const)('binds an own __proto__ %s child slot identically across adapters', async (_label, forEachIndex) => {
    const suffix = randomUUID();
    const memory = new WorkflowsInMemory({ db: new InMemoryDB() });
    const parent = { workflowName: `magic-owner-${suffix}`, runId: 'parent-run' };
    const stores = [memory, workflowsA] as const;
    const observations: unknown[] = [];
    try {
      for (const store of stores) {
        await store.persistWorkflowSnapshot({ ...parent, snapshot: createEmptyWorkflowSnapshot(parent.runId) });
        const operation = {
          ...parent,
          stepId: '__proto__',
          nestedRunId: 'durable-child',
          ...(forEachIndex === undefined ? {} : { forEachIndex }),
          result: {
            status: 'running' as const,
            payload: { safe: true },
            metadata: { caller: 'preserved', __workflow_meta: { caller: 'preserved' } },
          },
          requestContext: { admitted: true },
        };
        const bound = await store.bindWorkflowNestedRunOwnership(operation);
        expect(bound.status).toBe('bound');
        if (bound.status !== 'bound') throw new Error(`Expected bound, received ${bound.status}`);
        const replay = await store.bindWorkflowNestedRunOwnership(operation);
        expect(replay.status).toBe('already_bound');
        const conflict = await store.bindWorkflowNestedRunOwnership({ ...operation, nestedRunId: 'other-child' });
        expect(conflict).toEqual({ status: 'ownership_conflict' });

        const snapshot = await store.loadWorkflowSnapshot(parent);
        const ownStep = Object.getOwnPropertyDescriptor(snapshot?.context ?? {}, '__proto__')?.value;
        expect(Object.hasOwn(snapshot?.context ?? {}, '__proto__')).toBe(true);
        expect(Object.getPrototypeOf(snapshot?.context ?? {})).toBe(Object.prototype);
        observations.push({ ownStep, requestContext: snapshot?.requestContext });
        if (forEachIndex === undefined) {
          expect(ownStep?.metadata?.nestedRunId).toBe('durable-child');
        } else {
          expect(ownStep?.metadata?.__workflow_meta?.[WORKFLOW_TERMINAL_FOREACH_RUN_KEY]?.[String(forEachIndex)]).toBe(
            'durable-child',
          );
        }
      }
      expect(observations[1]).toEqual(observations[0]);
    } finally {
      await cleanup([parent.workflowName]);
    }
  });

  it('rolls back ancestry when the atomic parent ownership update fails', async () => {
    const suffix = randomUUID();
    const sqlSuffix = suffix.replaceAll('-', '');
    const parent = { workflowName: `rollback-parent-${suffix}`, runId: 'parent-run' };
    const child = { workflowName: `rollback-child-${suffix}`, runId: 'child-run' };
    const triggerName = `pf1782_admission_${sqlSuffix}`;
    const functionName = `pf1782_admission_fail_${sqlSuffix}`;
    const parentGraph: WorkflowRunState['serializedStepGraph'] = [
      { type: 'step', step: { id: 'nested', component: 'WORKFLOW' } },
    ];
    await workflowsA.persistWorkflowSnapshot({
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
    await pool.query(
      `CREATE FUNCTION ${functionName}() RETURNS trigger LANGUAGE plpgsql AS
       $$ BEGIN RAISE EXCEPTION 'forced admission rollback'; END $$`,
    );
    await pool.query(
      `CREATE TRIGGER ${triggerName} BEFORE UPDATE ON mastra_workflow_snapshot
       FOR EACH ROW WHEN (NEW.workflow_name = '${parent.workflowName}')
       EXECUTE FUNCTION ${functionName}()`,
    );
    try {
      await expect(
        workflowsA.admitWorkflowNestedRun({
          ...parent,
          stepId: 'nested',
          nestedWorkflowName: child.workflowName,
          nestedRunId: child.runId,
          expectedChildGraphFingerprint: EMPTY_CHILD_GRAPH_FINGERPRINT,
          result: { status: 'running', payload: {} },
          requestContext: {},
          recoveryAncestry: childAncestry,
        }),
      ).rejects.toThrow('forced admission rollback');
      await expect(workflowsA.getWorkflowTerminalRecoveryAncestry(child)).resolves.toEqual({
        status: 'missing_ancestry',
      });
      await expect(workflowsA.loadWorkflowSnapshot(parent)).resolves.not.toHaveProperty(
        'context.nested.metadata.nestedRunId',
      );
    } finally {
      await pool.query(`DROP TRIGGER IF EXISTS ${triggerName} ON mastra_workflow_snapshot`);
      await pool.query(`DROP FUNCTION IF EXISTS ${functionName}()`);
      await cleanup([child.workflowName, parent.workflowName]);
    }
  });

  it('materializes identical rich recovery evidence in PostgreSQL and InMemory', async () => {
    const suffix = randomUUID();
    const memory = new WorkflowsInMemory({ db: new InMemoryDB() });
    const parent = { workflowName: `parity-parent-${suffix}`, runId: 'parent-run' };
    const child = { workflowName: `parity-child-${suffix}`, runId: 'child-run' };
    const childGraph: WorkflowRunState['serializedStepGraph'] = [
      { type: 'step', step: { id: 'leaf', component: 'STEP' } },
    ];
    const childAncestry = ancestry(child, parent);
    const stores = [memory, workflowsA] as const;
    try {
      for (const store of stores) {
        await store.persistWorkflowSnapshot({ ...parent, snapshot: createParentSnapshot(parent.runId) });
        await store.persistWorkflowSnapshot({
          ...child,
          snapshot: { ...createEmptyWorkflowSnapshot(child.runId), serializedStepGraph: childGraph },
        });
        await expect(
          store.persistWorkflowTerminalRecoveryAncestry({ ...child, ancestry: childAncestry }),
        ).resolves.toMatchObject({ status: 'persisted' });
      }

      const fences = await Promise.all(
        stores.map(async (store, index) => {
          const claimed = await store.claimWorkflowTerminalization({
            ...child,
            eventKey: 'parity-terminal-event',
            terminalStatus: 'failed',
            ownerId: `parity-owner-${index}`,
            leaseMs: 10_000,
          });
          if (claimed.status !== 'acquired') throw new Error(`Expected acquired, received ${claimed.status}`);
          return {
            ...child,
            ownerId: claimed.record.ownerId,
            claimToken: claimed.record.claimToken,
            claimGeneration: claimed.record.claimGeneration,
          };
        }),
      );
      const snapshot: WorkflowRunState = {
        ...createEmptyWorkflowSnapshot(child.runId),
        status: 'failed',
        error: { name: 'Error', message: 'stale caller failure' },
        serializedStepGraph: childGraph,
        context: { __state: { exact: ['state', 1] } } as unknown as WorkflowRunState['context'],
        value: { stale: 'must-be-replaced' },
        requestContext: { tenant: 'tenant-a' },
      };
      const applicationClock = Date.now() + 60_000;
      const recoveryEnvelope = createTerminalRecoveryEnvelope({
        ...child,
        snapshot,
        terminalStatus: 'failed',
        ancestry: childAncestry,
        terminalResult: {
          status: 'failed',
          error: new Error('parity failure', { cause: new Error('parity cause') }),
          startedAt: applicationClock,
          endedAt: applicationClock + 1,
        },
      });
      for (const [index, store] of stores.entries()) {
        await expect(
          store.persistWorkflowTerminalState({ ...fences[index]!, snapshot, recoveryEnvelope }),
        ).resolves.toMatchObject({ status: 'persisted' });
        await expect(
          store.prepareWorkflowTerminalEffect({
            ...fences[index]!,
            expectedPhase: 'run_state_persisted',
            effect: {
              kind: 'parent-workflow-step-end',
              parentWorkflowName: parent.workflowName,
              parentRunId: parent.runId,
              parentStepId: 'nested',
              parentExecutionPath: [0],
            },
          }),
        ).resolves.toMatchObject({ status: 'prepared' });
      }
      const recovered = await Promise.all(
        stores.map((store, index) =>
          store.getWorkflowTerminalEffectForDispatch({
            ...fences[index]!,
            kind: 'parent-workflow-step-end',
          }),
        ),
      );
      expect(recovered[0]).toMatchObject({ status: 'found' });
      expect(recovered[1]).toMatchObject({ status: 'found' });
      if (recovered[0].status !== 'found' || recovered[1].status !== 'found') {
        throw new Error('Expected recovered terminal evidence');
      }
      expect(recovered[1].recovery).toMatchObject({
        envelopeHash: recovered[0].recovery.envelopeHash,
        envelope: recovered[0].recovery.envelope,
      });
      expect(recovered[1].effect).toMatchObject({
        effectKey: recovered[0].effect.effectKey,
        recoveryEnvelopeHash: recovered[0].effect.recoveryEnvelopeHash,
      });
      for (const result of recovered) {
        expect(() => validateWorkflowTerminalEffectIntegrity(result.effect)).not.toThrow();
        expect(result.effect.retainedRecordHash).toBe(result.recovery.recordHash);
      }
    } finally {
      await cleanup([child.workflowName, parent.workflowName]);
    }
  });

  it.each([
    {
      terminalStatus: 'failed' as const,
      terminalResult: { status: 'failed', error: { name: 'Error', message: 'authenticated PG failure' } },
      expectedError: { name: 'Error', message: 'authenticated PG failure' },
    },
    {
      terminalStatus: 'success' as const,
      terminalResult: { status: 'success', output: { done: true } },
      expectedError: undefined,
    },
    {
      terminalStatus: 'canceled' as const,
      terminalResult: { status: 'canceled' },
      expectedError: undefined,
    },
  ])(
    'projects authenticated $terminalStatus error truth into the PostgreSQL workflow row',
    async ({ terminalStatus, terminalResult, expectedError }) => {
      const run = { workflowName: `error-projection-${terminalStatus}-${randomUUID()}`, runId: 'run' };
      await workflowsA.persistWorkflowSnapshot({ ...run, snapshot: createEmptyWorkflowSnapshot(run.runId) });
      try {
        const fence = await claim(workflowsA, run, terminalStatus);
        const snapshot: WorkflowRunState = {
          ...createEmptyWorkflowSnapshot(run.runId),
          status: terminalStatus,
          error: { name: 'Error', message: 'stale caller failure' },
        };
        await expect(
          workflowsA.persistWorkflowTerminalState({
            ...fence,
            snapshot,
            recoveryEnvelope: createTerminalRecoveryEnvelope({
              ...run,
              snapshot,
              terminalStatus,
              terminalResult,
            }),
          }),
        ).resolves.toMatchObject({ status: 'persisted' });

        const persisted = await workflowsA.loadWorkflowSnapshot(run);
        expect(persisted?.result).toEqual(terminalResult);
        expect(persisted?.error).toEqual(expectedError);
        expect(Object.hasOwn(persisted ?? {}, 'error')).toBe(expectedError !== undefined);
      } finally {
        await cleanup([run.workflowName]);
      }
    },
  );

  it('persists immutable pre-terminal ancestry and an authenticated native-Error envelope without invoking toJSON', async () => {
    const suffix = randomUUID();
    const parent = { workflowName: `recovery-parent-${suffix}`, runId: 'parent-run' };
    const child = { workflowName: `recovery-child-${suffix}`, runId: 'child-run' };
    const childAncestry = ancestry(child, parent);
    const error = new Error('native terminal failure', { cause: new Error('native cause') });

    await workflowsA.persistWorkflowSnapshot({ ...parent, snapshot: createParentSnapshot(parent.runId) });
    await workflowsA.persistWorkflowSnapshot({ ...child, snapshot: createEmptyWorkflowSnapshot(child.runId) });
    try {
      const persisted = await Promise.all([
        workflowsA.persistWorkflowTerminalRecoveryAncestry({ ...child, ancestry: childAncestry }),
        workflowsB.persistWorkflowTerminalRecoveryAncestry({ ...child, ancestry: childAncestry }),
      ]);
      expect(persisted.map(result => result.status).sort()).toEqual(['already_persisted', 'persisted']);
      await expect(workflowsA.getWorkflowTerminalRecoveryAncestry(child)).resolves.toMatchObject({
        status: 'found',
        record: {
          workflowName: child.workflowName,
          runId: child.runId,
          ancestryHash: expect.stringMatching(/^sha256:[a-f0-9]{64}$/),
          ancestry: childAncestry,
        },
      });
      await expect(
        workflowsB.persistWorkflowTerminalRecoveryAncestry({
          ...child,
          ancestry: ancestry(child, { ...parent, runId: 'other-parent-run' }),
        }),
      ).resolves.toEqual({ status: 'ancestry_conflict' });

      const fence = await claim(workflowsA, child);
      const snapshot: WorkflowRunState = {
        ...createEmptyWorkflowSnapshot(child.runId),
        status: 'failed',
        context: { __state: { retained: true } } as unknown as WorkflowRunState['context'],
      };
      const toJSON = vi.fn(() => ({ forged: true }));
      await expect(
        workflowsA.persistWorkflowTerminalState({
          ...fence,
          snapshot,
          recoveryEnvelope: createTerminalRecoveryEnvelope({
            ...child,
            snapshot,
            terminalStatus: 'failed',
            ancestry: childAncestry,
            terminalResult: { status: 'failed', error: { message: 'must reject', toJSON } },
          }),
        }),
      ).resolves.toEqual({ status: 'invalid_recovery_envelope' });
      expect(toJSON).not.toHaveBeenCalled();
      const recoveryEnvelope = createTerminalRecoveryEnvelope({
        ...child,
        snapshot,
        terminalStatus: 'failed',
        ancestry: childAncestry,
        terminalResult: { status: 'failed', error },
      });
      await expect(
        workflowsA.persistWorkflowTerminalState({ ...fence, snapshot, recoveryEnvelope }),
      ).resolves.toMatchObject({ status: 'persisted', record: { phase: 'run_state_persisted' } });

      const retained = await pool.query<{ envelope_hash: `sha256:${string}`; envelope: unknown }>(
        `SELECT envelope_hash, envelope FROM mastra_workflow_terminal_snapshots_v2
         WHERE workflow_name = $1 AND run_id = $2`,
        [child.workflowName, child.runId],
      );
      expect(retained.rows[0]?.envelope_hash).toBe(
        getWorkflowTerminalRecoveryEnvelopeHash(retained.rows[0]!.envelope as never),
      );
      expect(retained.rows[0]?.envelope).toMatchObject({
        terminalResult: {
          status: 'failed',
          error: { name: 'Error', message: 'native terminal failure', cause: { message: 'native cause' } },
        },
      });

      await pool.query(
        `UPDATE mastra_workflow_terminal_snapshots_v2
         SET envelope = jsonb_set(envelope, '{finalState,retained}', 'false'::jsonb)
         WHERE workflow_name = $1 AND run_id = $2`,
        [child.workflowName, child.runId],
      );
      await expect(
        workflowsA.prepareWorkflowTerminalEffect({
          ...fence,
          expectedPhase: 'run_state_persisted',
          effect: {
            kind: 'parent-workflow-step-end',
            parentWorkflowName: parent.workflowName,
            parentRunId: parent.runId,
            parentStepId: 'nested',
            parentExecutionPath: [0],
          },
        }),
      ).rejects.toThrow('Invalid workflow terminal snapshot record');
    } finally {
      await cleanup([child.workflowName, parent.workflowName]);
    }
  });

  it('atomically assigns one nested owner per foreach iteration without losing sibling ownership', async () => {
    const workflowName = `ownership-${randomUUID()}`;
    const childWorkflowName = `each-${workflowName}`;
    const run = { workflowName, runId: 'parent-run' };
    const snapshot: WorkflowRunState = {
      ...createEmptyWorkflowSnapshot(run.runId),
      status: 'running',
      context: {
        each: {
          status: 'running',
          payload: ['a', 'b'],
          output: [null, null],
          metadata: {},
          startedAt: Date.now(),
        },
      } as unknown as WorkflowRunState['context'],
      serializedStepGraph: [{ type: 'foreach', step: { id: 'each', component: 'WORKFLOW' }, opts: { concurrency: 2 } }],
    };
    await workflowsA.persistWorkflowSnapshot({ ...run, snapshot });
    try {
      const admission = (forEachIndex: number, nestedRunId: string) => {
        const child = { workflowName: childWorkflowName, runId: nestedRunId };
        return {
          ...run,
          stepId: 'each',
          nestedWorkflowName: child.workflowName,
          nestedRunId,
          expectedChildGraphFingerprint: EMPTY_CHILD_GRAPH_FINGERPRINT,
          forEachIndex,
          result: { status: 'running' as const, payload: ['a', 'b'], output: [null, null], startedAt: 1 },
          requestContext: { ownership: true },
          recoveryAncestry: [
            {
              version: 1 as const,
              childWorkflowName: child.workflowName,
              childRunId: child.runId,
              parentWorkflowName: run.workflowName,
              parentRunId: run.runId,
              parentGraphFingerprint: createWorkflowTerminalGraphFingerprint(snapshot.serializedStepGraph),
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
      const concurrent = await Promise.all([
        workflowsA.admitWorkflowNestedRun(admission(0, 'child-a')),
        workflowsB.admitWorkflowNestedRun(admission(0, 'child-b')),
      ]);
      expect(concurrent.map(result => result.status).sort()).toEqual(['admitted', 'ownership_conflict']);
      const winner = concurrent.find(result => result.status === 'admitted');
      if (winner?.status !== 'admitted') throw new Error('Expected one ownership winner');
      const winnerId = (winner.stepResults.each as any).metadata.__workflow_meta.iterationRunIds['0'];
      await expect(workflowsA.admitWorkflowNestedRun(admission(0, winnerId))).resolves.toMatchObject({
        status: 'already_admitted',
      });
      await expect(workflowsB.admitWorkflowNestedRun(admission(1, 'child-c'))).resolves.toMatchObject({
        status: 'admitted',
      });
      const loserId = winnerId === 'child-a' ? 'child-b' : 'child-a';
      await expect(
        workflowsA.getWorkflowTerminalRecoveryAncestry({ workflowName: childWorkflowName, runId: loserId }),
      ).resolves.toEqual({ status: 'missing_ancestry' });
      await expect(workflowsA.loadWorkflowSnapshot(run)).resolves.toMatchObject({
        context: {
          each: {
            metadata: {
              __workflow_meta: { iterationRunIds: { '0': winnerId, '1': 'child-c' } },
            },
          },
        },
        requestContext: { ownership: true },
      });
    } finally {
      await cleanup([childWorkflowName, workflowName]);
    }
  });

  it('retains a completed recovery root while a recursively linked descendant lacks completed evidence', async () => {
    const suffix = randomUUID();
    const root = { workflowName: `cleanup-root-${suffix}`, runId: 'root-run' };
    const child = { workflowName: `cleanup-child-${suffix}`, runId: 'child-run' };
    const grandchild = { workflowName: `cleanup-grandchild-${suffix}`, runId: 'grandchild-run' };
    for (const run of [root, child, grandchild]) {
      await workflowsA.persistWorkflowSnapshot({ ...run, snapshot: createParentSnapshot(run.runId) });
    }
    try {
      await workflowsA.persistWorkflowTerminalRecoveryAncestry({ ...child, ancestry: ancestry(child, root) });
      await workflowsA.persistWorkflowTerminalRecoveryAncestry({
        ...grandchild,
        ancestry: [...ancestry(grandchild, child), ...ancestry(child, root)],
      });
      for (const run of [root, child]) {
        await claim(workflowsA, run);
      }
      await pool.query(
        `UPDATE mastra_workflow_terminalizations
         SET phase = 'complete', owner_id = NULL, claim_token = NULL, lease_expires_at = NULL,
             completed_at = updated_at
         WHERE workflow_name IN ($1, $2)`,
        [root.workflowName, child.workflowName],
      );

      await expect(
        workflowsA.deleteCompletedWorkflowTerminalizations({
          ...root,
          olderThan: new Date(Date.now() + 60_000),
        }),
      ).resolves.toEqual({ status: 'deleted', count: 0 });

      await claim(workflowsA, grandchild);
      await pool.query(
        `UPDATE mastra_workflow_terminalizations
         SET phase = 'complete', owner_id = NULL, claim_token = NULL, lease_expires_at = NULL,
             completed_at = updated_at
         WHERE workflow_name = $1 AND run_id = $2`,
        [grandchild.workflowName, grandchild.runId],
      );
      await expect(
        workflowsA.deleteCompletedWorkflowTerminalizations({
          ...root,
          olderThan: new Date(Date.now() + 60_000),
        }),
      ).resolves.toEqual({ status: 'deleted', count: 1 });
    } finally {
      await cleanup([grandchild.workflowName, child.workflowName, root.workflowName]);
    }
  });

  it('serializes completed-parent cleanup against pre-terminal ancestry admission', async () => {
    const suffix = randomUUID();
    const parent = { workflowName: `cleanup-race-parent-${suffix}`, runId: 'parent-run' };
    const child = { workflowName: `cleanup-race-child-${suffix}`, runId: 'child-run' };
    await workflowsA.persistWorkflowSnapshot({ ...parent, snapshot: createParentSnapshot(parent.runId) });
    await workflowsA.persistWorkflowSnapshot({ ...child, snapshot: createEmptyWorkflowSnapshot(child.runId) });
    try {
      const fence = await claim(workflowsA, parent);
      const snapshot: WorkflowRunState = {
        ...createParentSnapshot(parent.runId),
        status: 'failed',
        context: { __state: { terminal: true } } as unknown as WorkflowRunState['context'],
        value: { terminal: 'true' },
      };
      await expect(
        workflowsA.persistWorkflowTerminalState({
          ...fence,
          snapshot,
          recoveryEnvelope: createTerminalRecoveryEnvelope({
            ...parent,
            snapshot,
            terminalStatus: 'failed',
          }),
        }),
      ).resolves.toMatchObject({ status: 'persisted' });
      await pool.query(
        `UPDATE mastra_workflow_terminalizations
         SET phase = 'complete', owner_id = NULL, claim_token = NULL, lease_expires_at = NULL,
             updated_at = floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint,
             completed_at = floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint
         WHERE workflow_name = $1 AND run_id = $2`,
        [parent.workflowName, parent.runId],
      );

      const [cleanupResult, ancestryResult] = await Promise.allSettled([
        workflowsA.deleteCompletedWorkflowTerminalizations({
          ...parent,
          olderThan: new Date(Date.now() + 60_000),
        }),
        workflowsB.persistWorkflowTerminalRecoveryAncestry({ ...child, ancestry: ancestry(child, parent) }),
      ]);
      expect(cleanupResult.status).toBe('fulfilled');
      if (cleanupResult.status !== 'fulfilled') throw cleanupResult.reason;
      if (cleanupResult.value.count === 0) {
        expect(ancestryResult).toMatchObject({
          status: 'fulfilled',
          value: { status: expect.stringMatching(/^(persisted|already_persisted)$/) },
        });
      } else {
        expect(cleanupResult.value).toEqual({ status: 'deleted', count: 1 });
        expect(ancestryResult.status).toBe('rejected');
        const count = await pool.query<{ count: string }>(
          `SELECT count(*)::text AS count FROM mastra_workflow_terminal_recovery_ancestries
           WHERE workflow_name = $1 AND run_id = $2`,
          [child.workflowName, child.runId],
        );
        expect(count.rows[0]?.count).toBe('0');
      }
    } finally {
      await cleanup([child.workflowName, parent.workflowName]);
    }
  });

  it.each(['success', 'failed', 'canceled'] as const)(
    'rejects late atomic admission after cleanup of a %s parent without writes',
    async terminalStatus => {
      const suffix = randomUUID();
      const parent = { workflowName: `late-${terminalStatus}-parent-${suffix}`, runId: 'parent-run' };
      const child = { workflowName: `late-${terminalStatus}-child-${suffix}`, runId: 'child-run' };
      try {
        await workflowsA.persistWorkflowSnapshot({ ...parent, snapshot: createParentSnapshot(parent.runId) });
        const fence = await claim(workflowsA, parent, terminalStatus);
        const snapshot: WorkflowRunState = {
          ...createParentSnapshot(parent.runId),
          status: terminalStatus,
          context: { __state: { terminal: true } } as WorkflowRunState['context'],
          value: { terminal: true },
        };
        await workflowsA.persistWorkflowTerminalState({
          ...fence,
          snapshot,
          recoveryEnvelope: createTerminalRecoveryEnvelope({ ...parent, snapshot, terminalStatus }),
        });
        await pool.query(
          `UPDATE mastra_workflow_terminalizations
           SET phase = 'complete', owner_id = NULL, claim_token = NULL, lease_expires_at = NULL,
               completed_at = updated_at
           WHERE workflow_name = $1 AND run_id = $2`,
          [parent.workflowName, parent.runId],
        );
        await expect(
          workflowsA.deleteCompletedWorkflowTerminalizations({
            ...parent,
            olderThan: new Date(Date.now() + 60_000),
          }),
        ).resolves.toEqual({ status: 'deleted', count: 1 });
        const before = JSON.stringify(await workflowsA.loadWorkflowSnapshot(parent));
        const revisionBefore = await pool.query<{ generation: string }>(
          `SELECT generation::text FROM mastra_workflow_parent_revisions
           WHERE workflow_name = $1 AND run_id = $2`,
          [parent.workflowName, parent.runId],
        );

        await expect(workflowsB.admitWorkflowNestedRun(nestedAdmission(parent, child))).resolves.toEqual({
          status: 'parent_terminal',
        });
        expect(JSON.stringify(await workflowsA.loadWorkflowSnapshot(parent))).toBe(before);
        const revisionAfter = await pool.query<{ generation: string }>(
          `SELECT generation::text FROM mastra_workflow_parent_revisions
           WHERE workflow_name = $1 AND run_id = $2`,
          [parent.workflowName, parent.runId],
        );
        expect(revisionAfter.rows).toEqual(revisionBefore.rows);
        const childAncestry = await pool.query<{ count: string }>(
          `SELECT count(*)::text AS count FROM mastra_workflow_terminal_recovery_ancestries
           WHERE workflow_name = $1 AND run_id = $2`,
          [child.workflowName, child.runId],
        );
        expect(childAncestry.rows[0]?.count).toBe('0');
      } finally {
        await cleanup([child.workflowName, parent.workflowName]);
      }
    },
  );

  it('makes the first terminal claim authoritative before terminal snapshot persistence', async () => {
    const suffix = randomUUID();
    const parent = { workflowName: `claimed-terminal-parent-${suffix}`, runId: 'parent-run' };
    const ancestryChild = { workflowName: `claimed-terminal-ancestry-${suffix}`, runId: 'child-run' };
    const admissionChild = { workflowName: `claimed-terminal-admission-${suffix}`, runId: 'child-run' };
    await workflowsA.persistWorkflowSnapshot({ ...parent, snapshot: createParentSnapshot(parent.runId) });

    try {
      await expect(
        workflowsA.claimWorkflowTerminalization({
          ...parent,
          eventKey: 'parent-terminal',
          terminalStatus: 'failed',
          ownerId: 'owner',
          leaseMs: 10_000,
        }),
      ).resolves.toMatchObject({ status: 'acquired' });
      const marker = await pool.query<{ terminal_status: string | null }>(
        `SELECT terminal_status FROM mastra_workflow_parent_revisions
         WHERE workflow_name = $1 AND run_id = $2`,
        [parent.workflowName, parent.runId],
      );
      expect(marker.rows[0]?.terminal_status).toBe('failed');
      await expect(
        workflowsA.persistWorkflowTerminalRecoveryAncestry({
          ...ancestryChild,
          ancestry: ancestry(ancestryChild, parent),
        }),
      ).rejects.toThrow('parent evidence is unavailable');
      await expect(workflowsA.admitWorkflowNestedRun(nestedAdmission(parent, admissionChild))).resolves.toEqual({
        status: 'parent_terminal',
      });
    } finally {
      await cleanup([ancestryChild.workflowName, admissionChild.workflowName, parent.workflowName]);
    }
  });

  it('rejects a terminal canonical parent even when a journal exists and the marker was cleared', async () => {
    const suffix = randomUUID();
    const parent = { workflowName: `journal-terminal-parent-${suffix}`, runId: 'parent-run' };
    const child = { workflowName: `journal-terminal-child-${suffix}`, runId: 'child-run' };
    await workflowsA.persistWorkflowSnapshot({ ...parent, snapshot: createParentSnapshot(parent.runId) });

    try {
      await claim(workflowsA, parent, 'success');
      await workflowsA.persistWorkflowSnapshot({
        ...parent,
        snapshot: { ...createParentSnapshot(parent.runId), status: 'success' },
      });
      await pool.query(
        `UPDATE mastra_workflow_parent_revisions SET terminal_status = NULL
         WHERE workflow_name = $1 AND run_id = $2`,
        [parent.workflowName, parent.runId],
      );

      await expect(
        workflowsA.persistWorkflowTerminalRecoveryAncestry({ ...child, ancestry: ancestry(child, parent) }),
      ).rejects.toThrow('parent evidence is unavailable');
      await expect(workflowsA.getWorkflowTerminalRecoveryAncestry(child)).resolves.toEqual({
        status: 'missing_ancestry',
      });
    } finally {
      await cleanup([child.workflowName, parent.workflowName]);
    }
  });

  it('retains terminal ancestry evidence after journal cleanup and a running snapshot rewrite', async () => {
    const suffix = randomUUID();
    const parent = { workflowName: `terminal-marker-parent-${suffix}`, runId: 'parent-run' };
    const child = { workflowName: `terminal-marker-child-${suffix}`, runId: 'child-run' };
    try {
      await workflowsA.persistWorkflowSnapshot({ ...parent, snapshot: createParentSnapshot(parent.runId) });
      const fence = await claim(workflowsA, parent, 'failed');
      const terminalSnapshot: WorkflowRunState = {
        ...createParentSnapshot(parent.runId),
        status: 'failed',
        context: { __state: { terminal: true } } as WorkflowRunState['context'],
        value: { terminal: true },
      };
      await workflowsA.persistWorkflowTerminalState({
        ...fence,
        snapshot: terminalSnapshot,
        recoveryEnvelope: createTerminalRecoveryEnvelope({
          ...parent,
          snapshot: terminalSnapshot,
          terminalStatus: 'failed',
        }),
      });
      await pool.query(
        `UPDATE mastra_workflow_terminalizations
         SET phase = 'complete', owner_id = NULL, claim_token = NULL, lease_expires_at = NULL,
             completed_at = updated_at
         WHERE workflow_name = $1 AND run_id = $2`,
        [parent.workflowName, parent.runId],
      );
      await expect(
        workflowsA.deleteCompletedWorkflowTerminalizations({
          ...parent,
          olderThan: new Date(Date.now() + 60_000),
        }),
      ).resolves.toEqual({ status: 'deleted', count: 1 });

      await workflowsB.persistWorkflowSnapshot({
        ...parent,
        snapshot: { ...createParentSnapshot(parent.runId), status: 'running' },
      });
      await expect(workflowsA.loadWorkflowSnapshot(parent)).resolves.toMatchObject({ status: 'running' });
      await expect(
        workflowsA.persistWorkflowTerminalRecoveryAncestry({ ...child, ancestry: ancestry(child, parent) }),
      ).rejects.toThrow('Workflow terminal recovery ancestry parent evidence is unavailable');
      await expect(workflowsB.admitWorkflowNestedRun(nestedAdmission(parent, child))).resolves.toEqual({
        status: 'parent_terminal',
      });

      const marker = await pool.query<{ terminal_status: string | null }>(
        `SELECT terminal_status FROM mastra_workflow_parent_revisions
         WHERE workflow_name = $1 AND run_id = $2`,
        [parent.workflowName, parent.runId],
      );
      expect(marker.rows).toEqual([{ terminal_status: 'failed' }]);
    } finally {
      await cleanup([child.workflowName, parent.workflowName]);
    }
  });

  it('rejects ownership and recovery admission for a missing parent without retaining a revision row', async () => {
    const suffix = randomUUID();
    const parent = { workflowName: `missing-parent-${suffix}`, runId: 'parent-run' };
    const child = { workflowName: `missing-child-${suffix}`, runId: 'child-run' };
    try {
      await expect(
        workflowsA.bindWorkflowNestedRunOwnership({
          ...parent,
          stepId: 'nested',
          nestedRunId: child.runId,
          result: { status: 'running', payload: {} },
          requestContext: {},
        }),
      ).resolves.toEqual({ status: 'missing_run' });
      await expect(workflowsA.admitWorkflowNestedRun(nestedAdmission(parent, child))).resolves.toEqual({
        status: 'missing_run',
      });
      const revision = await pool.query<{ count: string }>(
        `SELECT count(*)::text AS count FROM mastra_workflow_parent_revisions
         WHERE workflow_name = $1 AND run_id = $2`,
        [parent.workflowName, parent.runId],
      );
      expect(revision.rows[0]?.count).toBe('0');
    } finally {
      await cleanup([child.workflowName, parent.workflowName]);
    }
  });

  it('retains a completed parent recovery root when atomic admission wins first', async () => {
    const suffix = randomUUID();
    const parent = { workflowName: `admission-first-parent-${suffix}`, runId: 'parent-run' };
    const child = { workflowName: `admission-first-child-${suffix}`, runId: 'child-run' };
    try {
      await workflowsA.persistWorkflowSnapshot({ ...parent, snapshot: createParentSnapshot(parent.runId) });
      await expect(workflowsA.admitWorkflowNestedRun(nestedAdmission(parent, child))).resolves.toMatchObject({
        status: 'admitted',
      });
      const fence = await claim(workflowsA, parent);
      const current = await workflowsA.loadWorkflowSnapshot(parent);
      if (!current) throw new Error('Expected admitted parent snapshot');
      const terminal: WorkflowRunState = {
        ...current,
        status: 'failed',
        context: { ...current.context, __state: { terminal: true } },
        value: { terminal: true },
      };
      await workflowsA.persistWorkflowTerminalState({
        ...fence,
        snapshot: terminal,
        recoveryEnvelope: createTerminalRecoveryEnvelope({ ...parent, snapshot: terminal, terminalStatus: 'failed' }),
      });
      await pool.query(
        `UPDATE mastra_workflow_terminalizations
         SET phase = 'complete', owner_id = NULL, claim_token = NULL, lease_expires_at = NULL,
             completed_at = updated_at
         WHERE workflow_name = $1 AND run_id = $2`,
        [parent.workflowName, parent.runId],
      );

      await expect(
        workflowsB.deleteCompletedWorkflowTerminalizations({
          ...parent,
          olderThan: new Date(Date.now() + 60_000),
        }),
      ).resolves.toEqual({ status: 'deleted', count: 0 });
      const retained = await pool.query<{ count: string }>(
        `SELECT count(*)::text AS count FROM mastra_workflow_terminal_recovery_ancestries
         WHERE workflow_name = $1 AND run_id = $2`,
        [child.workflowName, child.runId],
      );
      expect(retained.rows[0]?.count).toBe('1');
    } finally {
      await cleanup([child.workflowName, parent.workflowName]);
    }
  });

  it('exports, initializes, and clears recovery ancestry evidence in a custom schema', async () => {
    const schemaName = `recovery_${randomUUID().replaceAll('-', '').slice(0, 12)}`;
    const ddl = WorkflowsPG.getExportDDL(schemaName).join('\n');
    expect(ddl).toContain(`"${schemaName}"."mastra_workflow_terminal_recovery_ancestries"`);
    expect(ddl).toContain('"ancestry_hash" TEXT NOT NULL');
    expect(ddl).toContain('"immediate_parent_workflow_name" TEXT');
    expect(ddl).toContain('"immediate_parent_run_id" TEXT');

    await pool.query(`CREATE SCHEMA "${schemaName}"`);
    await pool.query(`CREATE TABLE "${schemaName}"."mastra_workflow_terminal_effects" (effect_key TEXT UNIQUE)`);
    await pool.query(`CREATE TABLE "${schemaName}"."mastra_workflow_terminal_snapshots" (legacy_shape TEXT)`);
    await pool.query(
      `CREATE TABLE "${schemaName}"."mastra_workflow_terminal_continuation_plans" (
         effect_key TEXT REFERENCES "${schemaName}"."mastra_workflow_terminal_effects" (effect_key)
       )`,
    );
    const workflows = new WorkflowsPG({ pool, schemaName });
    try {
      await workflows.init();
      const versionedTables = await pool.query<{ table_name: string }>(
        `SELECT table_name FROM information_schema.tables
         WHERE table_schema = $1 AND table_name IN ($2, $3, $4)`,
        [
          schemaName,
          'mastra_workflow_terminal_continuation_plans_v2',
          'mastra_workflow_terminal_effects_v2',
          'mastra_workflow_terminal_snapshots_v2',
        ],
      );
      expect(versionedTables.rows.map(row => row.table_name).sort()).toEqual([
        'mastra_workflow_terminal_continuation_plans_v2',
        'mastra_workflow_terminal_effects_v2',
        'mastra_workflow_terminal_snapshots_v2',
      ]);
      const planForeignKeys = await pool.query<{ definition: string }>(
        `SELECT pg_get_constraintdef(constraint_row.oid) AS definition
         FROM pg_constraint AS constraint_row
         JOIN pg_class AS table_row ON table_row.oid = constraint_row.conrelid
         JOIN pg_namespace AS namespace_row ON namespace_row.oid = table_row.relnamespace
         WHERE namespace_row.nspname = $1
           AND table_row.relname = 'mastra_workflow_terminal_continuation_plans_v2'
           AND constraint_row.contype = 'f'`,
        [schemaName],
      );
      expect(planForeignKeys.rows.map(row => row.definition).join('\n')).toContain(
        'mastra_workflow_terminal_effects_v2',
      );
      const indexes = await pool.query<{ indexname: string }>(
        `SELECT indexname FROM pg_indexes WHERE schemaname = $1 AND tablename = $2`,
        [schemaName, 'mastra_workflow_terminal_recovery_ancestries'],
      );
      expect(indexes.rows.map(row => row.indexname)).toContain(
        'mastra_workflow_terminal_recovery_ancestries_parent_idx',
      );
      const parent = { workflowName: 'custom-parent', runId: 'parent-run' };
      const child = { workflowName: 'custom-child', runId: 'child-run' };
      await workflows.persistWorkflowSnapshot({ ...parent, snapshot: createParentSnapshot(parent.runId) });
      await workflows.persistWorkflowSnapshot({ ...child, snapshot: createEmptyWorkflowSnapshot(child.runId) });
      await expect(
        workflows.persistWorkflowTerminalRecoveryAncestry({ ...child, ancestry: ancestry(child, parent) }),
      ).resolves.toMatchObject({ status: 'persisted' });
      const stored = await pool.query<{ immediate_parent_workflow_name: string; immediate_parent_run_id: string }>(
        `SELECT immediate_parent_workflow_name, immediate_parent_run_id
         FROM "${schemaName}"."mastra_workflow_terminal_recovery_ancestries"`,
      );
      expect(stored.rows).toEqual([
        { immediate_parent_workflow_name: parent.workflowName, immediate_parent_run_id: parent.runId },
      ]);
      await workflows.dangerouslyClearAll();
      const count = await pool.query<{ count: string }>(
        `SELECT count(*)::text AS count
         FROM "${schemaName}"."mastra_workflow_terminal_recovery_ancestries"`,
      );
      expect(count.rows[0]?.count).toBe('0');
    } finally {
      await pool.query(`DROP SCHEMA "${schemaName}" CASCADE`);
    }
  });
});
