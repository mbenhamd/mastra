import { randomUUID } from 'node:crypto';
import { createEmptyWorkflowSnapshot } from '@mastra/core/storage';
import { Pool } from 'pg';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { WorkflowsPG } from '.';

describe('WorkflowsPG terminal final state', () => {
  const pool = new Pool({
    host: process.env.POSTGRES_HOST || '127.0.0.1',
    port: Number(process.env.POSTGRES_PORT) || 5434,
    database: process.env.POSTGRES_DB || 'postgres',
    user: process.env.POSTGRES_USER || 'postgres',
    password: process.env.POSTGRES_PASSWORD || 'postgres',
  });
  const workflows = new WorkflowsPG({ pool });

  beforeAll(async () => {
    await workflows.init();
  });

  afterAll(async () => {
    await pool.end();
  });

  it('atomically replaces both terminal state views with the database clock', async () => {
    const workflowName = `terminal-final-state-${randomUUID()}`;
    const runId = 'run';
    const snapshot = createEmptyWorkflowSnapshot(runId);
    snapshot.status = 'running';
    snapshot.context.__state = { stale: true } as never;
    snapshot.value = { stale: true };
    snapshot.timestamp = 1;

    try {
      await workflows.persistWorkflowSnapshot({ workflowName, runId, snapshot });
      const before = Date.now();
      await workflows.updateWorkflowState({
        workflowName,
        runId,
        opts: { status: 'success', finalState: { exact: { answer: 42 } } },
      });
      const after = Date.now();

      const retained = await workflows.loadWorkflowSnapshot({ workflowName, runId });
      expect(retained).toMatchObject({
        status: 'success',
        context: { __state: { exact: { answer: 42 } } },
        value: { exact: { answer: 42 } },
      });
      expect(retained!.timestamp).toBeGreaterThanOrEqual(before);
      expect(retained!.timestamp).toBeLessThanOrEqual(after);
      expect(retained!.context.__state).toEqual(retained!.value);
    } finally {
      await pool.query(`DELETE FROM mastra_workflow_snapshot WHERE workflow_name = $1`, [workflowName]);
    }
  });
});
