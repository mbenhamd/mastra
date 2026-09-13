import { randomUUID } from 'node:crypto';
import { createEmptyWorkflowSnapshot } from '@mastra/core/storage';
import type { WorkflowRunState } from '@mastra/core/workflows';
import { Pool } from 'pg';
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';
import { WorkflowsPG } from '.';

const connection = {
  host: process.env.POSTGRES_HOST || '127.0.0.1',
  port: Number(process.env.POSTGRES_PORT) || 5434,
  database: process.env.POSTGRES_DB || 'postgres',
  user: process.env.POSTGRES_USER || 'postgres',
  password: process.env.POSTGRES_PASSWORD || 'postgres',
};

const SNAPSHOT_COLUMN_TYPES = ['jsonb', 'json', 'text'] as const;
type SnapshotColumnType = (typeof SNAPSHOT_COLUMN_TYPES)[number];

describe('WorkflowsPG compact execution state', () => {
  const pool = new Pool(connection);
  const readPool = new Pool(connection);
  const schemas = new Set<string>();

  beforeAll(async () => {
    await pool.query('SELECT 1');
    await readPool.query('SELECT 1');
  });

  afterAll(async () => {
    try {
      for (const schema of schemas) {
        await pool.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`);
      }
    } finally {
      await Promise.all([pool.end(), readPool.end()]);
    }
  });

  async function createSchema(snapshotColumnType: SnapshotColumnType): Promise<string> {
    const schema = `pf4240_${snapshotColumnType}_${randomUUID().replaceAll('-', '').slice(0, 12)}`;
    await pool.query(`CREATE SCHEMA "${schema}"`);
    schemas.add(schema);
    return schema;
  }

  function snapshot(runId: string, executionGeneration?: string): WorkflowRunState {
    return {
      ...createEmptyWorkflowSnapshot(runId),
      status: 'suspended',
      ...(executionGeneration === undefined ? {} : { executionGeneration }),
      value: { payload: 'x'.repeat(64 * 1024) },
    };
  }

  it.each(SNAPSHOT_COLUMN_TYPES)('projects status and generation from %s snapshots', async snapshotColumnType => {
    const schema = await createSchema(snapshotColumnType);
    const initializer = new WorkflowsPG({ pool, schemaName: schema, skipDefaultIndexes: true });
    await initializer.init();
    if (snapshotColumnType !== 'jsonb') {
      await pool.query(
        `ALTER TABLE "${schema}"."mastra_workflow_snapshot"
         ALTER COLUMN snapshot TYPE ${snapshotColumnType}
         USING snapshot::${snapshotColumnType}`,
      );
    }

    const readQuery = vi.spyOn(readPool, 'query').mockRejectedValue(new Error('compact read used replica'));
    const workflows = new WorkflowsPG({ pool, readPool, schemaName: schema, skipDefaultIndexes: true });
    const workflowName = `compact-state-${snapshotColumnType}`;
    const runId = randomUUID();
    const generation = `wfeg:pf4240:${snapshotColumnType}`;
    const writerQuery = vi.spyOn(pool, 'query');

    try {
      await workflows.persistWorkflowSnapshot({ workflowName, runId, snapshot: snapshot(runId, generation) });

      writerQuery.mockClear();
      const compact = await workflows.getWorkflowExecutionState({ workflowName, runId });
      expect(compact).toEqual({ status: 'suspended', executionGeneration: generation });
      expect(readQuery).not.toHaveBeenCalled();

      const compactCalls = writerQuery.mock.calls.filter(([, values]) => {
        return Array.isArray(values) && values[0] === workflowName && values[1] === runId;
      });
      expect(compactCalls).toHaveLength(1);
      expect(compactCalls[0]?.[0]).toContain("->'status'");
      expect(compactCalls[0]?.[0]).toContain("->'executionGeneration'");
      expect(compactCalls[0]?.[0]).not.toContain('SELECT *');

      const withoutGeneration = snapshot(runId);
      await workflows.persistWorkflowSnapshot({ workflowName, runId, snapshot: withoutGeneration });
      await expect(workflows.getWorkflowExecutionState({ workflowName, runId })).resolves.toEqual({
        status: 'suspended',
      });

      await workflows.persistWorkflowSnapshot({
        workflowName,
        runId,
        snapshot: { ...snapshot(runId, generation), executionGeneration: null } as unknown as WorkflowRunState,
      });
      await expect(workflows.getWorkflowExecutionState({ workflowName, runId })).resolves.toEqual({
        status: 'suspended',
        executionGeneration: null,
      });

      await expect(
        workflows.getWorkflowExecutionState({ workflowName, runId: 'missing-compact-execution-state' }),
      ).resolves.toBeNull();

      if (snapshotColumnType !== 'jsonb') {
        const legacyAuthorityKeys = JSON.stringify({
          status: 'suspended',
          executionGeneration: generation,
          '\u0000executionGeneration': 'legacy-generation',
          value: { payload: 'x'.repeat(64 * 1024) },
        });
        await pool.query(
          `UPDATE "${schema}"."mastra_workflow_snapshot" SET snapshot = $1
           WHERE workflow_name = $2 AND run_id = $3`,
          [legacyAuthorityKeys, workflowName, runId],
        );
        await expect(workflows.getWorkflowExecutionState({ workflowName, runId })).resolves.toEqual({
          status: 'suspended',
          executionGeneration: generation,
        });
      }

      if (snapshotColumnType !== 'text') {
        const serializedSnapshot = JSON.stringify({ status: 'running', executionGeneration: generation });
        await pool.query(
          `UPDATE "${schema}"."mastra_workflow_snapshot" SET snapshot = $1
           WHERE workflow_name = $2 AND run_id = $3`,
          [JSON.stringify(serializedSnapshot), workflowName, runId],
        );
        await expect(workflows.getWorkflowExecutionState({ workflowName, runId })).resolves.toEqual({
          status: 'running',
          executionGeneration: generation,
        });

        await pool.query(
          `UPDATE "${schema}"."mastra_workflow_snapshot" SET snapshot = $1
           WHERE workflow_name = $2 AND run_id = $3`,
          [JSON.stringify('not a serialized snapshot'), workflowName, runId],
        );
        await expect(workflows.getWorkflowExecutionState({ workflowName, runId })).rejects.toThrow();
      } else {
        await pool.query(
          `UPDATE "${schema}"."mastra_workflow_snapshot" SET snapshot = $1
           WHERE workflow_name = $2 AND run_id = $3`,
          ['not a serialized snapshot', workflowName, runId],
        );
        await expect(workflows.getWorkflowExecutionState({ workflowName, runId })).rejects.toThrow();
      }

      for (const falsySnapshot of ['false', '0', 'null']) {
        await pool.query(
          `UPDATE "${schema}"."mastra_workflow_snapshot" SET snapshot = $1
           WHERE workflow_name = $2 AND run_id = $3`,
          [falsySnapshot, workflowName, runId],
        );
        await expect(workflows.getWorkflowExecutionState({ workflowName, runId })).resolves.toBeNull();
      }

      await pool.query(
        `UPDATE "${schema}"."mastra_workflow_snapshot" SET snapshot = $1
         WHERE workflow_name = $2 AND run_id = $3`,
        ['""', workflowName, runId],
      );
      if (snapshotColumnType === 'text') {
        await expect(workflows.getWorkflowExecutionState({ workflowName, runId })).resolves.toBeNull();
      } else {
        await expect(workflows.getWorkflowExecutionState({ workflowName, runId })).rejects.toThrow();
      }
    } finally {
      writerQuery.mockRestore();
      readQuery.mockRestore();
    }
  });

  it('propagates writer query failures', async () => {
    const failingPool = new Pool(connection);
    await failingPool.end();
    const workflows = new WorkflowsPG({ pool: failingPool, schemaName: 'pf4240_failure', skipDefaultIndexes: true });

    await expect(workflows.getWorkflowExecutionState({ workflowName: 'failure', runId: 'run' })).rejects.toThrow();
  });
});
