import { createClient } from '@libsql/client';
import { expectAtomicWorkflowResumeStorageContract } from '@internal/storage-test-utils';
import { InMemoryDB, TABLE_SCHEMAS, TABLE_WORKFLOW_SNAPSHOT, WorkflowsInMemory } from '@mastra/core/storage';
import { describe, it } from 'vitest';
import { LibSQLDB } from '../../db';
import { WorkflowsLibSQL } from '.';

describe('atomic workflow resume storage contract', () => {
  it('holds for the in-memory adapter under concurrent admission', async () => {
    const db = new InMemoryDB();
    await expectAtomicWorkflowResumeStorageContract({
      primary: new WorkflowsInMemory({ db }),
      concurrent: new WorkflowsInMemory({ db }),
      workflowName: 'atomic-resume-in-memory',
    });
  });

  it('holds for LibSQL transactions and JSON materialization', async () => {
    const client = createClient({ url: 'file::memory:?cache=shared' });
    const concurrentClient = createClient({ url: 'file::memory:?cache=shared' });
    const primary = new WorkflowsLibSQL({ client, maxRetries: 5, initialBackoffMs: 1 });
    const concurrent = new WorkflowsLibSQL({ client: concurrentClient, maxRetries: 5, initialBackoffMs: 1 });
    try {
      const db = new LibSQLDB({ client, maxRetries: 1, initialBackoffMs: 1 });
      await db.createTable({
        tableName: TABLE_WORKFLOW_SNAPSHOT,
        schema: TABLE_SCHEMAS[TABLE_WORKFLOW_SNAPSHOT],
      });
      await expectAtomicWorkflowResumeStorageContract({
        primary,
        concurrent,
        workflowName: 'atomic-resume-libsql',
      });
    } finally {
      client.close();
      concurrentClient.close();
    }
  });
});
