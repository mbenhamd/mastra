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
    await pool.query(`DELETE FROM mastra_workflow_terminalizations WHERE workflow_name = $1`, [workflowName]);
    await pool.query(`DELETE FROM mastra_workflow_snapshot WHERE workflow_name = $1`, [workflowName]);
  }

  it('uses database time and fences concurrent adapter instances', async () => {
    const workflowName = `terminalization-${Date.now()}`;
    const runId = 'run';
    const run = { workflowName, runId };
    await workflowsA.persistWorkflowSnapshot({ ...run, snapshot: createEmptyWorkflowSnapshot(runId) });

    try {
      expect(workflowsA.supportsWorkflowTerminalizationJournal()).toBe(true);
      const localClock = vi.spyOn(Date, 'now').mockReturnValue(0);
      const results = await Promise.all([
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
      localClock.mockRestore();

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
        workflowsA.advanceWorkflowTerminalization({
          ...run,
          ownerId: acquired.record.ownerId!,
          claimToken: acquired.record.claimToken!,
          claimGeneration: acquired.record.claimGeneration,
          expectedPhase: 'terminalization_pending',
          nextPhase: 'run_state_persisted',
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
      ).resolves.toMatchObject({ status: 'advanced', record: { phase: 'run_state_persisted' } });
    } finally {
      await cleanup(workflowName);
    }
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

    const customSchemaDDL = WorkflowsPG.getExportDDL('tenant').join('\n');
    expect(customSchemaDDL).toContain('"tenant"."mastra_workflow_terminalizations"');
    expect(customSchemaDDL).toContain('"tenant_mastra_workflow_terminalizations_phase_lease_idx"');
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
