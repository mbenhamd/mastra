import { randomUUID } from 'node:crypto';
import { createEmptyWorkflowSnapshot } from '@mastra/core/storage';
import { Pool } from 'pg';
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';
import { WorkflowsPG } from '.';

describe('WorkflowsPG terminalization journal', () => {
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

  it('uses database time and fences concurrent adapter instances', async () => {
    const workflowName = `terminalization-${Date.now()}`;
    const runId = 'run';
    const run = { workflowName, runId };
    await workflowsA.persistWorkflowSnapshot({ ...run, snapshot: createEmptyWorkflowSnapshot(runId) });

    try {
      expect(workflowsA.supportsWorkflowTerminalizationJournal()).toBe(true);
      const results = await (async () => {
        const localClock = vi.spyOn(Date, 'now').mockReturnValue(0);
        try {
          return await Promise.all([
            workflowsA.claimWorkflowTerminalization({
              ...run,
              eventKey: 'event',
              terminalStatus: 'failed',
              ownerId: 'worker-a',
              leaseMs: 1_000,
            }),
            workflowsB.claimWorkflowTerminalization({
              ...run,
              eventKey: 'event',
              terminalStatus: 'failed',
              ownerId: 'worker-b',
              leaseMs: 1_000,
            }),
          ]);
        } finally {
          localClock.mockRestore();
        }
      })();

      expect(results.map(result => result.status).sort()).toEqual(['acquired', 'leased']);
      const acquired = results.find(result => result.status === 'acquired');
      const leased = results.find(result => result.status === 'leased');
      if (acquired?.status !== 'acquired') throw new Error('claim owner not found');
      if (leased?.status !== 'leased') throw new Error('leased result not found');
      expect(acquired.record.createdAt).toBeGreaterThan(0);
      expect(leased.record).not.toHaveProperty('ownerId');
      expect(leased.record).not.toHaveProperty('claimToken');
      expect(leased.record).not.toHaveProperty('claimGeneration');

      await expect(
        workflowsB.claimWorkflowTerminalization({
          ...run,
          eventKey: 'event',
          terminalStatus: 'failed',
          ownerId: acquired.record.ownerId!,
          claimToken: acquired.record.claimToken,
          claimGeneration: acquired.record.claimGeneration,
          leaseMs: 100,
        }),
      ).resolves.toMatchObject({ status: 'renewed' });

      await new Promise(resolve => setTimeout(resolve, 500));
      const takeover = await workflowsB.claimWorkflowTerminalization({
        ...run,
        eventKey: 'event',
        terminalStatus: 'failed',
        ownerId: 'worker-c',
        leaseMs: 1_000,
      });
      expect(takeover).toMatchObject({
        status: 'acquired',
        record: { claimGeneration: acquired.record.claimGeneration + 1 },
      });
      if (takeover.status !== 'acquired') throw new Error('takeover failed');

      await expect(
        workflowsA.persistWorkflowTerminalState({
          ...run,
          ownerId: acquired.record.ownerId!,
          claimToken: acquired.record.claimToken!,
          claimGeneration: acquired.record.claimGeneration,
          snapshot: { ...createEmptyWorkflowSnapshot(runId), status: 'failed' },
        }),
      ).resolves.toMatchObject({ status: 'not_owner' });
      await expect(
        workflowsB.advanceWorkflowTerminalization({
          ...run,
          ownerId: takeover.record.ownerId!,
          claimToken: takeover.record.claimToken!,
          claimGeneration: takeover.record.claimGeneration,
          expectedPhase: 'terminalization_pending',
          nextPhase: 'run_state_persisted',
        }),
      ).resolves.toEqual({ status: 'invalid_transition' });
      await expect(
        workflowsB.persistWorkflowTerminalState({
          ...run,
          ownerId: takeover.record.ownerId!,
          claimToken: takeover.record.claimToken!,
          claimGeneration: takeover.record.claimGeneration,
          snapshot: { ...createEmptyWorkflowSnapshot(runId), status: 'failed' },
        }),
      ).resolves.toMatchObject({ status: 'persisted', record: { phase: 'run_state_persisted' } });
      await expect(workflowsA.loadWorkflowSnapshot(run)).resolves.toMatchObject({ status: 'failed' });
    } finally {
      await cleanup(workflowName);
    }
  });

  it('observes terminalization without waiting for the writer advisory lock', async () => {
    const workflowName = `terminalization-read-only-${randomUUID()}`;
    const runId = 'run';
    const run = { workflowName, runId };
    await workflowsA.persistWorkflowSnapshot({ ...run, snapshot: createEmptyWorkflowSnapshot(runId) });
    await workflowsA.claimWorkflowTerminalization({
      ...run,
      eventKey: 'event',
      terminalStatus: 'failed',
      ownerId: 'worker-a',
      leaseMs: 1_000,
    });

    const lockClient = await pool.connect();
    try {
      await lockClient.query('BEGIN');
      await lockClient.query('SELECT pg_advisory_xact_lock(hashtextextended($1, 0))', [
        JSON.stringify([workflowName, runId]),
      ]);

      const observation = await Promise.race([
        workflowsB.getWorkflowTerminalization(run),
        new Promise<never>((_, reject) =>
          setTimeout(() => reject(new Error('observation waited for the writer lock')), 500),
        ),
      ]);
      expect(observation).toMatchObject({ status: 'found', record: { eventKey: 'event' } });
    } finally {
      await lockClient.query('ROLLBACK');
      lockClient.release();
      await cleanup(workflowName);
    }
  });

  it('rolls back the journal phase when canonical snapshot serialization fails', async () => {
    const workflowName = `terminalization-rollback-${randomUUID()}`;
    const runId = 'run';
    const run = { workflowName, runId };
    await workflowsA.persistWorkflowSnapshot({ ...run, snapshot: createEmptyWorkflowSnapshot(runId) });

    try {
      const claim = await workflowsA.claimWorkflowTerminalization({
        ...run,
        eventKey: 'terminal-event',
        terminalStatus: 'failed',
        ownerId: 'worker',
        leaseMs: 10_000,
      });
      if (claim.status !== 'acquired') throw new Error('claim failed');
      await expect(
        workflowsA.persistWorkflowTerminalState({
          ...run,
          ownerId: claim.record.ownerId,
          claimToken: claim.record.claimToken,
          claimGeneration: claim.record.claimGeneration,
          snapshot: undefined as never,
        }),
      ).resolves.toEqual({ status: 'invalid_snapshot' });
      await expect(
        workflowsA.persistWorkflowTerminalState({
          ...run,
          ownerId: claim.record.ownerId,
          claimToken: claim.record.claimToken,
          claimGeneration: claim.record.claimGeneration,
          snapshot: {
            ...createEmptyWorkflowSnapshot(runId),
            status: 'failed',
            value: { unserializable: 1n },
          } as never,
        }),
      ).rejects.toThrow('BigInt');
      await expect(workflowsA.getWorkflowTerminalization(run)).resolves.toMatchObject({
        status: 'found',
        record: { phase: 'terminalization_pending' },
      });
      await expect(workflowsA.loadWorkflowSnapshot(run)).resolves.toMatchObject({ status: 'pending' });
    } finally {
      await cleanup(workflowName);
    }
  });

  it('validates the same materialized snapshot that it writes when toJSON changes terminal status', async () => {
    const workflowName = `terminalization-materialization-${randomUUID()}`;
    const runId = 'run';
    const run = { workflowName, runId };
    await workflowsA.persistWorkflowSnapshot({ ...run, snapshot: createEmptyWorkflowSnapshot(runId) });

    try {
      const claim = await workflowsA.claimWorkflowTerminalization({
        ...run,
        eventKey: 'terminal-event',
        terminalStatus: 'failed',
        ownerId: 'worker',
        leaseMs: 10_000,
      });
      if (claim.status !== 'acquired') throw new Error('claim failed');
      const snapshot = {
        ...createEmptyWorkflowSnapshot(runId),
        status: 'failed' as const,
        toJSON() {
          return { ...this, status: 'success', toJSON: undefined };
        },
      };

      await expect(
        workflowsA.persistWorkflowTerminalState({
          ...run,
          ownerId: claim.record.ownerId,
          claimToken: claim.record.claimToken,
          claimGeneration: claim.record.claimGeneration,
          snapshot,
        }),
      ).resolves.toEqual({ status: 'invalid_snapshot' });
      await expect(workflowsA.getWorkflowTerminalization(run)).resolves.toMatchObject({
        status: 'found',
        record: { phase: 'terminalization_pending' },
      });
      await expect(workflowsA.loadWorkflowSnapshot(run)).resolves.toMatchObject({ status: 'pending' });
    } finally {
      await cleanup(workflowName);
    }
  });

  it('materializes accessor-backed operation identity once before locking and writing', async () => {
    const workflowName = `terminalization-envelope-${randomUUID()}`;
    const runId = 'run';
    const run = { workflowName, runId };
    const alternate = {
      workflowName: `terminalization-envelope-alternate-${randomUUID()}`,
      runId: 'alternate-run',
    };
    await workflowsA.persistWorkflowSnapshot({ ...run, snapshot: createEmptyWorkflowSnapshot(runId) });
    await workflowsA.persistWorkflowSnapshot({
      ...alternate,
      snapshot: createEmptyWorkflowSnapshot(alternate.runId),
    });

    try {
      const claim = await workflowsA.claimWorkflowTerminalization({
        ...run,
        eventKey: 'terminal-event',
        terminalStatus: 'failed',
        ownerId: 'worker',
        leaseMs: 10_000,
      });
      if (claim.status !== 'acquired') throw new Error('claim failed');
      const operation = {
        ...run,
        ownerId: claim.record.ownerId,
        claimToken: claim.record.claimToken,
        claimGeneration: claim.record.claimGeneration,
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

      await expect(workflowsA.persistWorkflowTerminalState(operation)).resolves.toMatchObject({
        status: 'persisted',
        record: { phase: 'run_state_persisted' },
      });
      expect({ workflowNameReads, runIdReads }).toEqual({ workflowNameReads: 1, runIdReads: 1 });
      await expect(workflowsA.loadWorkflowSnapshot(run)).resolves.toMatchObject({ status: 'failed' });
      await expect(workflowsA.loadWorkflowSnapshot(alternate)).resolves.toMatchObject({ status: 'pending' });
      await expect(workflowsA.getWorkflowTerminalization(run)).resolves.toMatchObject({
        status: 'found',
        record: { phase: 'run_state_persisted' },
      });
      await expect(workflowsA.getWorkflowTerminalization(alternate)).resolves.toEqual({ status: 'missing_record' });
    } finally {
      await cleanup(workflowName);
      await cleanup(alternate.workflowName);
    }
  });

  it('materializes every journal mutation envelope before locking or selecting a run', async () => {
    const workflowNames: string[] = [];
    const makePair = async (label: string) => {
      const intended = { workflowName: `terminalization-envelope-${label}-${randomUUID()}`, runId: `${label}-run` };
      const alternate = {
        workflowName: `terminalization-envelope-${label}-alternate-${randomUUID()}`,
        runId: `${label}-alternate-run`,
      };
      workflowNames.push(intended.workflowName, alternate.workflowName);
      await workflowsA.persistWorkflowSnapshot({
        ...intended,
        snapshot: createEmptyWorkflowSnapshot(intended.runId),
      });
      await workflowsA.persistWorkflowSnapshot({
        ...alternate,
        snapshot: createEmptyWorkflowSnapshot(alternate.runId),
      });
      return { intended, alternate };
    };
    const claim = async (run: { workflowName: string; runId: string }, eventKey: string) => {
      const result = await workflowsA.claimWorkflowTerminalization({
        ...run,
        eventKey,
        terminalStatus: 'failed',
        ownerId: `${eventKey}-owner`,
        leaseMs: 10_000,
      });
      if (result.status !== 'acquired') throw new Error(`claim failed for ${eventKey}`);
      return result.record;
    };

    try {
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
      await expect(workflowsA.claimWorkflowTerminalization(claimOperation.operation)).resolves.toMatchObject({
        status: 'acquired',
        record: { eventKey: 'claim-event' },
      });
      expect(claimOperation.reads()).toEqual({ workflowName: 1, runId: 1 });
      await expect(workflowsA.getWorkflowTerminalization(claimPair.alternate)).resolves.toEqual({
        status: 'missing_record',
      });
      const getOperation = withStatefulIdentity({ ...claimPair.intended }, claimPair.alternate);
      await expect(workflowsA.getWorkflowTerminalization(getOperation.operation)).resolves.toMatchObject({
        status: 'found',
        record: { eventKey: 'claim-event' },
      });
      expect(getOperation.reads()).toEqual({ workflowName: 1, runId: 1 });

      const advancePair = await makePair('advance');
      const advanceClaim = await claim(advancePair.intended, 'advance-intended');
      await claim(advancePair.alternate, 'advance-alternate');
      await pool.query(
        `UPDATE mastra_workflow_terminalizations
         SET phase = 'finish_effect_recorded'
         WHERE workflow_name IN ($1, $2)`,
        [advancePair.intended.workflowName, advancePair.alternate.workflowName],
      );
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
      await expect(workflowsA.advanceWorkflowTerminalization(advanceOperation.operation)).resolves.toMatchObject({
        status: 'advanced',
        record: { phase: 'complete' },
      });
      expect(advanceOperation.reads()).toEqual({ workflowName: 1, runId: 1 });
      await expect(workflowsA.getWorkflowTerminalization(advancePair.alternate)).resolves.toMatchObject({
        status: 'found',
        record: { phase: 'finish_effect_recorded' },
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
      await expect(workflowsA.releaseWorkflowTerminalization(releaseOperation.operation)).resolves.toMatchObject({
        status: 'released',
      });
      expect(releaseOperation.reads()).toEqual({ workflowName: 1, runId: 1 });
      const releasedOwners = await pool.query<{ workflow_name: string; owner_id: string | null }>(
        `SELECT workflow_name, owner_id
         FROM mastra_workflow_terminalizations
         WHERE workflow_name IN ($1, $2)
         ORDER BY workflow_name`,
        [releasePair.intended.workflowName, releasePair.alternate.workflowName],
      );
      expect(Object.fromEntries(releasedOwners.rows.map(row => [row.workflow_name, row.owner_id]))).toEqual({
        [releasePair.intended.workflowName]: null,
        [releasePair.alternate.workflowName]: alternateReleaseClaim.ownerId,
      });

      const cleanupPair = await makePair('cleanup');
      await claim(cleanupPair.intended, 'cleanup-intended');
      await claim(cleanupPair.alternate, 'cleanup-alternate');
      await pool.query(
        `UPDATE mastra_workflow_terminalizations
         SET phase = 'complete', owner_id = NULL, claim_token = NULL,
             lease_expires_at = NULL, completed_at = updated_at
         WHERE workflow_name IN ($1, $2)`,
        [cleanupPair.intended.workflowName, cleanupPair.alternate.workflowName],
      );
      const cleanupOperation = withStatefulIdentity(
        { ...cleanupPair.intended, olderThan: new Date(Date.now() + 60_000) },
        cleanupPair.alternate,
      );
      await expect(workflowsA.deleteCompletedWorkflowTerminalizations(cleanupOperation.operation)).resolves.toEqual({
        status: 'deleted',
        count: 1,
      });
      expect(cleanupOperation.reads()).toEqual({ workflowName: 1, runId: 1 });
      await expect(workflowsA.getWorkflowTerminalization(cleanupPair.intended)).resolves.toEqual({
        status: 'missing_record',
      });
      await expect(workflowsA.getWorkflowTerminalization(cleanupPair.alternate)).resolves.toMatchObject({
        status: 'found',
        record: { phase: 'complete' },
      });
    } finally {
      await Promise.all(workflowNames.map(cleanup));
    }
  });

  it('validates claim and fence envelopes before reporting a missing PostgreSQL run', async () => {
    const missing = { workflowName: `missing-${randomUUID()}`, runId: 'missing-run' };
    await expect(
      workflowsA.claimWorkflowTerminalization({
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
      workflowsA.advanceWorkflowTerminalization({
        ...invalidFence,
        expectedPhase: 'finish_effect_recorded',
        nextPhase: 'complete',
      }),
    ).rejects.toThrow(TypeError);
    await expect(workflowsA.releaseWorkflowTerminalization(invalidFence)).rejects.toThrow(TypeError);
    await expect(
      workflowsA.persistWorkflowTerminalState({
        ...invalidFence,
        snapshot: { ...createEmptyWorkflowSnapshot(missing.runId), status: 'failed' },
      }),
    ).rejects.toThrow(TypeError);
  });

  it('keeps journal state isolated from snapshot replacement and deletion', async () => {
    const workflowName = `terminalization-isolation-${Date.now()}`;
    const runId = 'run';
    const run = { workflowName, runId };
    await workflowsA.persistWorkflowSnapshot({ ...run, snapshot: createEmptyWorkflowSnapshot(runId) });

    try {
      const claim = await workflowsA.claimWorkflowTerminalization({
        ...run,
        eventKey: 'event',
        terminalStatus: 'failed',
        ownerId: 'worker',
        leaseMs: 1_000,
      });
      expect(claim.status).toBe('acquired');
      await workflowsB.persistWorkflowSnapshot({ ...run, snapshot: createEmptyWorkflowSnapshot(runId) });
      await expect(workflowsB.getWorkflowTerminalization(run)).resolves.toMatchObject({
        status: 'found',
        record: { eventKey: 'event' },
      });

      await workflowsB.deleteWorkflowRunById(run);
      await expect(workflowsA.getWorkflowTerminalization(run)).resolves.toMatchObject({
        status: 'found',
        record: { eventKey: 'event' },
      });
    } finally {
      await cleanup(workflowName);
    }
  });

  it('exports the journal table and recovery indexes', () => {
    const ddl = WorkflowsPG.getExportDDL().join('\n');
    expect(ddl).toContain('mastra_workflow_terminalizations');
    expect(ddl).toContain('PRIMARY KEY ("workflow_name", "run_id")');
    expect(ddl).toContain('mastra_workflow_terminalizations_phase_lease_idx');
    expect(ddl).toContain('mastra_workflow_terminalizations_completed_idx');

    const schemaName = 'tenant_identifier';
    const customSchemaDDL = WorkflowsPG.getExportDDL(schemaName).join('\n');
    expect(customSchemaDDL).toContain(`"${schemaName}"."mastra_workflow_terminalizations"`);
    expect(customSchemaDDL).toContain('"mastra_workflow_terminalizations_phase_lease_idx"');

    const indexNames = WorkflowsPG.getDefaultIndexDefs(schemaName).map(index => index.name);
    expect(new Set(indexNames).size).toBe(indexNames.length);
    expect(indexNames.every(name => Buffer.byteLength(name, 'utf8') <= 63)).toBe(true);
  });

  it('creates both recovery indexes in a long custom schema', async () => {
    const schemaName = `tenant_identifier_${randomUUID().replaceAll('-', '').slice(0, 16)}`;
    await pool.query(`CREATE SCHEMA "${schemaName}"`);
    try {
      const workflows = new WorkflowsPG({ pool, schemaName });
      await workflows.init();
      const indexes = await pool.query<{ indexname: string }>(
        `SELECT indexname FROM pg_indexes WHERE schemaname = $1 AND tablename = $2 ORDER BY indexname`,
        [schemaName, 'mastra_workflow_terminalizations'],
      );
      expect(indexes.rows.map(row => row.indexname)).toEqual(
        expect.arrayContaining([
          'mastra_workflow_terminalizations_completed_idx',
          'mastra_workflow_terminalizations_phase_lease_idx',
        ]),
      );
    } finally {
      await pool.query(`DROP SCHEMA IF EXISTS "${schemaName}" CASCADE`);
    }
  });

  it('fails closed when a persisted journal row has an unknown version', async () => {
    const workflowName = `terminalization-invalid-${Date.now()}`;
    const runId = 'run';
    await pool.query(
      `INSERT INTO mastra_workflow_terminalizations
       (workflow_name, run_id, version, event_key, terminal_status, phase, owner_id, claim_token,
        claim_generation, lease_expires_at, created_at, updated_at, completed_at)
       VALUES ($1, $2, 99, 'event', 'failed', 'terminalization_pending', 'worker', 'token',
        1, 1000, 1, 1, NULL)`,
      [workflowName, runId],
    );

    try {
      await expect(workflowsA.getWorkflowTerminalization({ workflowName, runId })).rejects.toThrow(
        'Invalid workflow terminalization record',
      );
    } finally {
      await cleanup(workflowName);
    }
  });

  it('fails closed when completed timestamps contradict the persisted phase transition', async () => {
    const workflowName = `terminalization-invalid-completion-${randomUUID()}`;
    const runId = 'run';
    await pool.query(
      `INSERT INTO mastra_workflow_terminalizations
       (workflow_name, run_id, version, event_key, terminal_status, phase, owner_id, claim_token,
        claim_generation, lease_expires_at, created_at, updated_at, completed_at)
       VALUES ($1, $2, 1, 'event', 'failed', 'complete', NULL, NULL, 1, NULL, 1, 100, 50)`,
      [workflowName, runId],
    );

    try {
      await expect(workflowsA.getWorkflowTerminalization({ workflowName, runId })).rejects.toThrow(
        'Invalid workflow terminalization record',
      );
    } finally {
      await cleanup(workflowName);
    }
  });

  it('fails closed when an active lease exceeds the bounded lease contract', async () => {
    const workflowName = `terminalization-invalid-lease-${randomUUID()}`;
    const runId = 'run';
    await pool.query(
      `INSERT INTO mastra_workflow_terminalizations
       (workflow_name, run_id, version, event_key, terminal_status, phase, owner_id, claim_token,
        claim_generation, lease_expires_at, created_at, updated_at, completed_at)
       VALUES ($1, $2, 1, 'event', 'failed', 'terminalization_pending', 'worker', 'token',
        1, $3, 1, 1, NULL)`,
      [workflowName, runId, Number.MAX_SAFE_INTEGER],
    );

    try {
      await expect(workflowsA.getWorkflowTerminalization({ workflowName, runId })).rejects.toThrow(
        'Invalid workflow terminalization record',
      );
    } finally {
      await cleanup(workflowName);
    }
  });

  it('fails closed when persisted journal timestamps are ahead of database time', async () => {
    const workflowName = `terminalization-invalid-future-${randomUUID()}`;
    const runId = 'run';
    const futureUpdatedAt = Date.now() + 30 * 86_400_000;
    await pool.query(
      `INSERT INTO mastra_workflow_terminalizations
       (workflow_name, run_id, version, event_key, terminal_status, phase, owner_id, claim_token,
        claim_generation, lease_expires_at, created_at, updated_at, completed_at)
       VALUES ($1, $2, 1, 'event', 'failed', 'terminalization_pending', 'worker', 'token',
        1, $3, 1, $4, NULL)`,
      [workflowName, runId, futureUpdatedAt + 1_000, futureUpdatedAt],
    );

    try {
      await expect(workflowsA.getWorkflowTerminalization({ workflowName, runId })).rejects.toThrow(
        'Invalid workflow terminalization record',
      );
    } finally {
      await cleanup(workflowName);
    }
  });

  it('accepts exact completion and maximum lease timestamp boundaries', async () => {
    const completedWorkflow = `terminalization-valid-completion-${randomUUID()}`;
    const leaseWorkflow = `terminalization-valid-lease-${randomUUID()}`;
    const runId = 'run';
    await pool.query(
      `INSERT INTO mastra_workflow_terminalizations
       (workflow_name, run_id, version, event_key, terminal_status, phase, owner_id, claim_token,
        claim_generation, lease_expires_at, created_at, updated_at, completed_at)
       VALUES ($1, $3, 1, 'complete-event', 'failed', 'complete', NULL, NULL, 1, NULL, 1, 50, 50),
              ($2, $3, 1, 'lease-event', 'failed', 'terminalization_pending', 'worker', 'token',
               1, 86400001, 1, 1, NULL)`,
      [completedWorkflow, leaseWorkflow, runId],
    );

    try {
      await expect(
        workflowsA.getWorkflowTerminalization({ workflowName: completedWorkflow, runId }),
      ).resolves.toMatchObject({ status: 'found', record: { phase: 'complete', completedAt: 50 } });
      await expect(
        workflowsA.getWorkflowTerminalization({ workflowName: leaseWorkflow, runId }),
      ).resolves.toMatchObject({
        status: 'found',
        record: { phase: 'terminalization_pending', leaseExpiresAt: 86_400_001 },
      });
    } finally {
      await cleanup(completedWorkflow);
      await cleanup(leaseWorkflow);
    }
  });
});
