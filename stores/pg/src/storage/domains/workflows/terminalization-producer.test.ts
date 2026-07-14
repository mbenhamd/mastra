import { randomUUID } from 'node:crypto';
import { createEmptyWorkflowSnapshot } from '@mastra/core/storage';
import { Pool } from 'pg';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { WorkflowsPG } from '.';

describe('WorkflowsPG terminal producer outbox', () => {
  const pool = new Pool({
    host: process.env.POSTGRES_HOST || '127.0.0.1',
    port: Number(process.env.POSTGRES_PORT) || 5434,
    database: process.env.POSTGRES_DB || 'postgres',
    user: process.env.POSTGRES_USER || 'postgres',
    password: process.env.POSTGRES_PASSWORD || 'postgres',
  });
  const workflowsA = new WorkflowsPG({ pool });
  const workflowsB = new WorkflowsPG({ pool });

  function assertSafeTestSqlIdentifier(value: string): string {
    if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(value)) {
      throw new Error(`Unsafe test SQL identifier: ${value}`);
    }
    return value;
  }

  function assertSafeTestSqlLiteral(value: string): string {
    if (!/^[a-zA-Z0-9_-]+$/.test(value)) {
      throw new Error(`Unsafe test SQL literal: ${value}`);
    }
    return value;
  }

  beforeAll(async () => {
    await workflowsA.init();
  });

  afterAll(async () => {
    await pool.end();
  });

  async function cleanup(workflowName: string): Promise<void> {
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await client.query(`DELETE FROM mastra_workflow_terminal_effects WHERE workflow_name = $1`, [workflowName]);
      await client.query(`DELETE FROM mastra_workflow_terminal_snapshots WHERE workflow_name = $1`, [workflowName]);
      await client.query(`DELETE FROM mastra_workflow_terminalizations WHERE workflow_name = $1`, [workflowName]);
      await client.query(`DELETE FROM mastra_workflow_snapshot WHERE workflow_name = $1`, [workflowName]);
      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  async function createTerminalRun(workflowName: string, runId: string, resourceId?: string) {
    const run = { workflowName, runId, resourceId };
    await workflowsA.persistWorkflowSnapshot({ ...run, snapshot: createEmptyWorkflowSnapshot(runId) });
    const claim = await workflowsA.claimWorkflowTerminalization({
      ...run,
      eventKey: `${runId}-event`,
      terminalStatus: 'failed',
      ownerId: `${runId}-owner`,
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
      ...createEmptyWorkflowSnapshot(runId),
      status: 'failed' as const,
      context: { marker: { status: 'success' as const, output: { retained: true } } },
    };
    await workflowsA.persistWorkflowTerminalState({ ...fence, snapshot });
    return { run, fence, snapshot };
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

  it('atomically converges concurrent producers and retains dispatch evidence after normal deletion', async () => {
    const workflowName = `terminal-producer-${randomUUID()}`;
    const { run, fence, snapshot } = await createTerminalRun(workflowName, 'run', 'resource-terminal');

    try {
      const results = await Promise.all([
        workflowsA.prepareWorkflowTerminalEffect({
          ...fence,
          expectedPhase: 'run_state_persisted',
          effect: { kind: 'workflow-finish' },
        }),
        workflowsB.prepareWorkflowTerminalEffect({
          ...fence,
          expectedPhase: 'run_state_persisted',
          effect: { kind: 'workflow-finish' },
        }),
      ]);
      expect(results.map(result => result.status).sort()).toEqual(['already_prepared', 'prepared']);
      const effects = results.flatMap(result =>
        result.status === 'prepared' || result.status === 'already_prepared' ? [result.effect] : [],
      );
      expect(effects).toHaveLength(2);
      expect(effects[1]).toEqual(effects[0]);
      const beforeRenewal = await pool.query<{ lease_expires_at: string }>(
        `SELECT lease_expires_at FROM mastra_workflow_terminalizations WHERE workflow_name = $1 AND run_id = $2`,
        [run.workflowName, run.runId],
      );
      await new Promise(resolve => setTimeout(resolve, 5));
      await expect(
        workflowsA.prepareWorkflowTerminalEffect({
          ...fence,
          expectedPhase: 'run_state_persisted',
          effect: { kind: 'workflow-finish' },
          leaseMs: 20_000,
        }),
      ).resolves.toMatchObject({ status: 'already_prepared' });
      const afterRenewal = await pool.query<{ lease_expires_at: string }>(
        `SELECT lease_expires_at FROM mastra_workflow_terminalizations WHERE workflow_name = $1 AND run_id = $2`,
        [run.workflowName, run.runId],
      );
      expect(Number(afterRenewal.rows[0]?.lease_expires_at)).toBeGreaterThan(
        Number(beforeRenewal.rows[0]?.lease_expires_at),
      );
      await expect(
        workflowsA.getWorkflowTerminalEffectForDispatch({ ...fence, kind: 'workflow-finish' }),
      ).resolves.toEqual({ status: 'found', effect: effects[0], snapshot, resourceId: run.resourceId });

      await workflowsA.deleteWorkflowRunById(run);
      await expect(
        workflowsB.getWorkflowTerminalEffectForDispatch({ ...fence, kind: 'workflow-finish' }),
      ).resolves.toEqual({ status: 'found', effect: effects[0], snapshot, resourceId: run.resourceId });
    } finally {
      await cleanup(workflowName);
    }
  });

  it('materializes prepare and dispatch identities once before locking a run', async () => {
    const intendedName = `terminal-envelope-${randomUUID()}`;
    const alternateName = `terminal-envelope-alternate-${randomUUID()}`;
    const intended = await createTerminalRun(intendedName, 'intended-run');
    const alternate = await createTerminalRun(alternateName, 'alternate-run');

    try {
      const prepare = withStatefulIdentity(
        {
          ...intended.fence,
          expectedPhase: 'run_state_persisted' as const,
          effect: { kind: 'workflow-finish' as const },
        },
        alternate.run,
      );
      await expect(workflowsA.prepareWorkflowTerminalEffect(prepare.operation)).resolves.toMatchObject({
        status: 'prepared',
      });
      expect(prepare.reads()).toEqual({ workflowName: 1, runId: 1 });
      await expect(workflowsA.getWorkflowTerminalization(alternate.run)).resolves.toMatchObject({
        status: 'found',
        record: { phase: 'run_state_persisted' },
      });

      const dispatch = withStatefulIdentity({ ...intended.fence, kind: 'workflow-finish' as const }, alternate.run);
      await expect(workflowsA.getWorkflowTerminalEffectForDispatch(dispatch.operation)).resolves.toMatchObject({
        status: 'found',
        snapshot: { runId: intended.run.runId },
      });
      expect(dispatch.reads()).toEqual({ workflowName: 1, runId: 1 });

      const alternateEffects = await pool.query<{ count: string }>(
        `SELECT count(*)::text AS count FROM mastra_workflow_terminal_effects
         WHERE workflow_name = $1 AND run_id = $2`,
        [alternate.run.workflowName, alternate.run.runId],
      );
      expect(alternateEffects.rows[0]?.count).toBe('0');
    } finally {
      await cleanup(intendedName);
      await cleanup(alternateName);
    }
  });

  it('validates descriptors, kinds, and fences before looking up a missing run', async () => {
    const missing = {
      workflowName: `terminal-missing-${randomUUID()}`,
      runId: 'missing-run',
      ownerId: 'owner',
      claimToken: 'token',
      claimGeneration: 1,
      expectedPhase: 'run_state_persisted' as const,
    };
    const accessorEffect: Record<string, unknown> = {};
    Object.defineProperty(accessorEffect, 'kind', { enumerable: true, get: () => 'workflow-finish' });

    await expect(
      workflowsA.prepareWorkflowTerminalEffect({ ...missing, effect: accessorEffect as never }),
    ).rejects.toThrow('effect contains unknown or accessor fields');
    await expect(
      workflowsA.prepareWorkflowTerminalEffect({
        ...missing,
        effect: { kind: 'workflow-finish', extra: true } as never,
      }),
    ).rejects.toThrow('effect contains unknown or accessor fields');
    await expect(
      workflowsA.getWorkflowTerminalEffectForDispatch({ ...missing, kind: 'invalid' as never }),
    ).rejects.toThrow('kind must be parent-workflow-step-end or workflow-finish');
    await expect(
      workflowsA.prepareWorkflowTerminalEffect({
        ...missing,
        claimToken: '',
        effect: { kind: 'workflow-finish' },
      }),
    ).rejects.toThrow('claimToken must be a well-formed non-empty string');
    await expect(
      workflowsA.getWorkflowTerminalEffectForDispatch({ ...missing, claimToken: '', kind: 'workflow-finish' }),
    ).rejects.toThrow('claimToken must be a well-formed non-empty string');
    await expect(
      workflowsA.getWorkflowTerminalEffectForDispatch({
        ...missing,
        workflowName: 'w'.repeat(513),
        kind: 'workflow-finish',
      }),
    ).rejects.toThrow('workflowName must be a well-formed non-empty string no longer than 512 characters');
    await expect(
      workflowsA.getWorkflowTerminalEffectForDispatch({
        ...missing,
        runId: `run${String.fromCharCode(0xd800)}`,
        kind: 'workflow-finish',
      }),
    ).rejects.toThrow('runId must be a well-formed non-empty string no longer than 512 characters');
    await expect(
      workflowsA.claimWorkflowTerminalization({
        workflowName: 'w'.repeat(513),
        runId: missing.runId,
        eventKey: 'event',
        terminalStatus: 'failed',
        ownerId: missing.ownerId,
        leaseMs: 10_000,
      }),
    ).rejects.toThrow('workflowName must be a well-formed non-empty string no longer than 512 characters');
    await expect(
      workflowsA.claimWorkflowTerminalization({
        workflowName: missing.workflowName,
        runId: 'r'.repeat(513),
        eventKey: 'event',
        terminalStatus: 'failed',
        ownerId: missing.ownerId,
        leaseMs: 10_000,
      }),
    ).rejects.toThrow('runId must be a well-formed non-empty string no longer than 512 characters');
  });

  it('rolls back canonical, retained, and journal state when either evidence write fails', async () => {
    const exerciseFailure = async (stage: 'retained' | 'journal') => {
      const workflowName = assertSafeTestSqlLiteral(`terminal-rollback-${stage}-${randomUUID()}`);
      const runId = 'run';
      const run = { workflowName, runId };
      const suffix = randomUUID().replaceAll('-', '');
      const functionName = assertSafeTestSqlIdentifier(`pf1762_fail_${suffix}`);
      const triggerName = assertSafeTestSqlIdentifier(`pf1762_trigger_${suffix}`);
      const table = assertSafeTestSqlIdentifier(
        stage === 'retained' ? 'mastra_workflow_terminal_snapshots' : 'mastra_workflow_terminalizations',
      );
      await workflowsA.persistWorkflowSnapshot({ ...run, snapshot: createEmptyWorkflowSnapshot(runId) });
      const claim = await workflowsA.claimWorkflowTerminalization({
        ...run,
        eventKey: 'event',
        terminalStatus: 'failed',
        ownerId: 'owner',
        leaseMs: 10_000,
      });
      if (claim.status !== 'acquired') throw new Error('claim failed');
      await pool.query(
        `CREATE FUNCTION ${functionName}() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
           IF NEW.workflow_name = '${workflowName}' ${stage === 'journal' ? "AND NEW.phase = 'run_state_persisted'" : ''}
           THEN RAISE EXCEPTION 'PF1762 injected failure'; END IF;
           RETURN NEW;
         END $$`,
      );
      await pool.query(
        `CREATE TRIGGER ${triggerName} BEFORE ${stage === 'retained' ? 'INSERT' : 'UPDATE'} ON ${table}
         FOR EACH ROW EXECUTE FUNCTION ${functionName}()`,
      );
      try {
        await expect(
          workflowsA.persistWorkflowTerminalState({
            ...run,
            ownerId: claim.record.ownerId,
            claimToken: claim.record.claimToken,
            claimGeneration: claim.record.claimGeneration,
            snapshot: { ...createEmptyWorkflowSnapshot(runId), status: 'failed' },
          }),
        ).rejects.toThrow('PF1762 injected failure');
        await expect(workflowsA.loadWorkflowSnapshot(run)).resolves.toMatchObject({ status: 'pending' });
        await expect(workflowsA.getWorkflowTerminalization(run)).resolves.toMatchObject({
          status: 'found',
          record: { phase: 'terminalization_pending' },
        });
        const retained = await pool.query<{ count: string }>(
          `SELECT count(*)::text AS count FROM mastra_workflow_terminal_snapshots
           WHERE workflow_name = $1 AND run_id = $2`,
          [workflowName, runId],
        );
        expect(retained.rows[0]?.count).toBe('0');
      } finally {
        await pool.query(`DROP TRIGGER IF EXISTS ${triggerName} ON ${table}`);
        await pool.query(`DROP FUNCTION IF EXISTS ${functionName}()`);
        await cleanup(workflowName);
      }
    };

    await exerciseFailure('retained');
    await exerciseFailure('journal');
  });

  it('fails closed on forged effect and retained evidence before a stale fence', async () => {
    const workflowName = `terminal-forged-${randomUUID()}`;
    const { run, fence } = await createTerminalRun(workflowName, 'run');

    try {
      await pool.query(
        `UPDATE mastra_workflow_terminal_snapshots AS retained
         SET created_at = journal.updated_at + 1
         FROM mastra_workflow_terminalizations AS journal
         WHERE retained.workflow_name = journal.workflow_name AND retained.run_id = journal.run_id
           AND retained.workflow_name = $1 AND retained.run_id = $2`,
        [run.workflowName, run.runId],
      );
      await new Promise(resolve => setTimeout(resolve, 5));
      await expect(
        workflowsA.prepareWorkflowTerminalEffect({
          ...fence,
          expectedPhase: 'run_state_persisted',
          effect: { kind: 'workflow-finish' },
        }),
      ).rejects.toThrow('Invalid workflow terminal snapshot journal link');
      await pool.query(
        `UPDATE mastra_workflow_terminal_snapshots AS retained
         SET created_at = journal.updated_at
         FROM mastra_workflow_terminalizations AS journal
         WHERE retained.workflow_name = journal.workflow_name AND retained.run_id = journal.run_id
           AND retained.workflow_name = $1 AND retained.run_id = $2`,
        [run.workflowName, run.runId],
      );
      await workflowsA.prepareWorkflowTerminalEffect({
        ...fence,
        expectedPhase: 'run_state_persisted',
        effect: { kind: 'workflow-finish' },
      });
      const originalHash = await pool.query<{ payload_hash: string }>(
        `SELECT payload_hash FROM mastra_workflow_terminal_effects
         WHERE workflow_name = $1 AND run_id = $2 AND effect_kind = 'workflow-finish'`,
        [run.workflowName, run.runId],
      );
      await pool.query(
        `UPDATE mastra_workflow_terminal_effects SET payload_hash = $1
         WHERE workflow_name = $2 AND run_id = $3 AND effect_kind = 'workflow-finish'`,
        [`sha256:${'0'.repeat(64)}`, run.workflowName, run.runId],
      );
      await expect(
        workflowsA.getWorkflowTerminalEffectForDispatch({
          ...fence,
          claimToken: 'stale-token',
          kind: 'workflow-finish',
        }),
      ).rejects.toThrow('Invalid workflow terminal effect integrity');

      await pool.query(
        `UPDATE mastra_workflow_terminal_effects SET payload_hash = $1
         WHERE workflow_name = $2 AND run_id = $3`,
        [originalHash.rows[0]?.payload_hash, run.workflowName, run.runId],
      );
      await pool.query(
        `UPDATE mastra_workflow_terminal_snapshots
         SET terminal_status = 'success', snapshot = jsonb_set(snapshot, '{status}', '"success"')
         WHERE workflow_name = $1 AND run_id = $2`,
        [run.workflowName, run.runId],
      );
      await expect(
        workflowsA.getWorkflowTerminalEffectForDispatch({ ...fence, kind: 'workflow-finish' }),
      ).rejects.toThrow('Invalid workflow terminal snapshot journal link');
    } finally {
      await cleanup(workflowName);
    }
  });

  it('retains producer evidence through normal deletion and removes it only with completed cleanup', async () => {
    const workflowName = `terminal-cleanup-${randomUUID()}`;
    const { run, fence } = await createTerminalRun(workflowName, 'run');

    try {
      await workflowsA.prepareWorkflowTerminalEffect({
        ...fence,
        expectedPhase: 'run_state_persisted',
        effect: {
          kind: 'parent-workflow-step-end',
          parentWorkflowName: 'parent',
          parentRunId: 'parent-run',
          parentStepId: 'nested-step',
          parentExecutionPath: [1, 3, 2],
        },
      });
      await expect(
        workflowsA.advanceWorkflowTerminalization({
          ...fence,
          expectedPhase: 'parent_outbox_pending',
          nextPhase: 'parent_effect_recorded',
        }),
      ).resolves.toEqual({ status: 'invalid_transition' });

      await workflowsA.deleteWorkflowRunById(run);
      await expect(
        workflowsA.getWorkflowTerminalEffectForDispatch({ ...fence, kind: 'parent-workflow-step-end' }),
      ).resolves.toMatchObject({
        status: 'found',
        effect: { parentExecutionPath: [1, 3, 2] },
        snapshot: { status: 'failed' },
      });
      await pool.query(
        `UPDATE mastra_workflow_terminalizations
         SET phase = 'complete', owner_id = NULL, claim_token = NULL, lease_expires_at = NULL,
             updated_at = floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint,
             completed_at = floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint
         WHERE workflow_name = $1 AND run_id = $2`,
        [run.workflowName, run.runId],
      );
      const suffix = randomUUID().replaceAll('-', '');
      const functionName = assertSafeTestSqlIdentifier(`pf1762_cleanup_fail_${suffix}`);
      const triggerName = assertSafeTestSqlIdentifier(`pf1762_cleanup_trigger_${suffix}`);
      await pool.query(
        `CREATE FUNCTION ${functionName}() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
           IF OLD.workflow_name = '${workflowName}' THEN RAISE EXCEPTION 'PF1762 cleanup failure'; END IF;
           RETURN OLD;
         END $$`,
      );
      await pool.query(
        `CREATE TRIGGER ${triggerName} BEFORE DELETE ON mastra_workflow_terminal_snapshots
         FOR EACH ROW EXECUTE FUNCTION ${functionName}()`,
      );
      try {
        await expect(
          workflowsA.deleteCompletedWorkflowTerminalizations({
            ...run,
            olderThan: new Date(Date.now() + 60_000),
          }),
        ).rejects.toThrow('PF1762 cleanup failure');
        for (const table of [
          'mastra_workflow_terminalizations',
          'mastra_workflow_terminal_effects',
          'mastra_workflow_terminal_snapshots',
        ]) {
          assertSafeTestSqlIdentifier(table);
          const count = await pool.query<{ count: string }>(
            `SELECT count(*)::text AS count FROM ${table} WHERE workflow_name = $1 AND run_id = $2`,
            [run.workflowName, run.runId],
          );
          expect(count.rows[0]?.count).toBe('1');
        }
      } finally {
        await pool.query(`DROP TRIGGER IF EXISTS ${triggerName} ON mastra_workflow_terminal_snapshots`);
        await pool.query(`DROP FUNCTION IF EXISTS ${functionName}()`);
      }
      await expect(
        workflowsA.deleteCompletedWorkflowTerminalizations({
          ...run,
          olderThan: new Date(Date.now() + 60_000),
        }),
      ).resolves.toEqual({ status: 'deleted', count: 1 });
      for (const table of [
        'mastra_workflow_terminalizations',
        'mastra_workflow_terminal_effects',
        'mastra_workflow_terminal_snapshots',
      ]) {
        assertSafeTestSqlIdentifier(table);
        const count = await pool.query<{ count: string }>(
          `SELECT count(*)::text AS count FROM ${table} WHERE workflow_name = $1 AND run_id = $2`,
          [run.workflowName, run.runId],
        );
        expect(count.rows[0]?.count).toBe('0');
      }
    } finally {
      await cleanup(workflowName);
    }
  });

  it('exports final alpha DDL without compatibility ALTERs or schema-prefixed index names', () => {
    const ddl = WorkflowsPG.getExportDDL().join('\n');
    expect(ddl).toContain('mastra_workflow_terminal_effects');
    expect(ddl).toContain('mastra_workflow_terminal_snapshots');
    expect(ddl).toContain('"parent_execution_path" JSONB');
    expect(ddl).toContain('"effect_key" TEXT NOT NULL UNIQUE');
    expect(ddl).toContain('"resource_id" TEXT');
    expect(ddl).toContain('"snapshot" JSONB NOT NULL');

    const custom = WorkflowsPG.getExportDDL('tenant').join('\n');
    expect(custom).toContain('"tenant"."mastra_workflow_terminal_effects"');
    expect(custom).toContain('"tenant"."mastra_workflow_terminal_snapshots"');
    expect(custom).toContain('"mastra_workflow_terminalizations_phase_lease_idx"');
    expect(custom).not.toContain('"tenant_mastra_workflow_terminalizations_phase_lease_idx"');
    expect(WorkflowsPG.prototype.init.toString()).not.toContain('ADD COLUMN IF NOT EXISTS "parent_execution_path"');
  });
});
