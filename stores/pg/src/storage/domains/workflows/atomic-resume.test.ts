import { randomUUID } from 'node:crypto';
import { expectAtomicWorkflowResumeStorageContract } from '@internal/storage-test-utils';
import { Pool } from 'pg';
import { afterAll, beforeAll, describe, it } from 'vitest';
import { WorkflowsPG } from '.';

describe('WorkflowsPG atomic resume transaction', () => {
  const pool = new Pool({
    host: process.env.POSTGRES_HOST || '127.0.0.1',
    port: Number(process.env.POSTGRES_PORT) || 5434,
    database: process.env.POSTGRES_DB || 'postgres',
    user: process.env.POSTGRES_USER || 'postgres',
    password: process.env.POSTGRES_PASSWORD || 'postgres',
  });
  const primary = new WorkflowsPG({ pool });
  const concurrent = new WorkflowsPG({ pool });

  beforeAll(async () => {
    await primary.init();
  });

  afterAll(async () => {
    await pool.end();
  });

  it('serializes concurrent admission, conflict, finalization, and receipt consumption', async () => {
    await expectAtomicWorkflowResumeStorageContract({
      primary,
      concurrent,
      workflowName: `atomic-resume-pg-${randomUUID()}`,
    });
  });
});
