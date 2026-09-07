import { randomUUID } from 'node:crypto';
import {
  expectWorkflowTerminalParentStorageContract,
  expectWorkflowTerminalStorageContract,
} from '@internal/storage-test-utils';
import { Pool } from 'pg';
import { afterAll, beforeAll, describe, it } from 'vitest';
import { WorkflowsPG } from '.';

describe('WorkflowsPG shared terminal storage contract', () => {
  const connection = {
    host: process.env.POSTGRES_HOST || '127.0.0.1',
    port: Number(process.env.POSTGRES_PORT) || 5434,
    database: process.env.POSTGRES_DB || 'postgres',
    user: process.env.POSTGRES_USER || 'postgres',
    password: process.env.POSTGRES_PASSWORD || 'postgres',
  };
  // Separate pools force independent database clients; this schema belongs only to this suite.
  const pool = new Pool(connection);
  const concurrentPool = new Pool(connection);
  const schemaName = `terminal_contract_${randomUUID().replaceAll('-', '')}`;
  const primary = new WorkflowsPG({ pool, schemaName });
  const concurrent = new WorkflowsPG({ pool: concurrentPool, schemaName });

  beforeAll(async () => {
    await primary.init();
    await concurrent.init();
  });

  afterAll(async () => {
    try {
      await pool.query(`DROP SCHEMA IF EXISTS "${schemaName}" CASCADE`);
    } finally {
      await Promise.all([pool.end(), concurrentPool.end()]);
    }
  });

  it('preserves journal, outbox, receipt, and fencing semantics across clients', async () => {
    await expectWorkflowTerminalStorageContract({ primary, concurrent, workflowName: 'terminal-contract-pg' });
  });

  it('applies a graph-bound child result and continuation once across clients', async () => {
    await expectWorkflowTerminalParentStorageContract({ primary, concurrent, workflowName: 'terminal-parent-pg' });
  });
});
