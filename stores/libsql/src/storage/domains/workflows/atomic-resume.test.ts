import { expectAtomicWorkflowResumeStorageContract } from '@internal/storage-test-utils';
import { createClient } from '@libsql/client';
import { InMemoryDB, TABLE_SCHEMAS, TABLE_WORKFLOW_SNAPSHOT, WorkflowsInMemory } from '@mastra/core/storage';
import type { WorkflowRunState } from '@mastra/core/workflows';
import { describe, expect, it } from 'vitest';
import { LibSQLStore } from '../..';
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

  it('holds across LibSQL clients and JSON materialization', async () => {
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

  it('holds for the standard isolated :memory: store', async () => {
    const store = new LibSQLStore({ id: 'atomic-resume-bare-memory', url: ':memory:' });
    try {
      await store.init();
      const workflows = await store.getStore('workflows');
      await expectAtomicWorkflowResumeStorageContract({
        primary: workflows!,
        workflowName: 'atomic-resume-bare-memory',
      });
    } finally {
      await store.close();
    }
  });

  it('inserts an ordinary unfenced first step update', async () => {
    const store = new LibSQLStore({ id: 'first-step-update-bare-memory', url: ':memory:' });
    try {
      await store.init();
      const workflows = (await store.getStore('workflows'))!;
      const snapshot = {
        runId: 'first-step-update-run',
        status: 'running',
        value: {},
        context: {},
        serializedStepGraph: [],
        activePaths: [],
        activeStepsPath: {},
        suspendedPaths: {},
        resumeLabels: {},
        waitingPaths: {},
        timestamp: 1,
      } as WorkflowRunState;

      await expect(
        workflows.persistWorkflowStepUpdate({
          workflowName: 'first-step-update-workflow',
          runId: snapshot.runId,
          snapshot,
        }),
      ).resolves.toEqual({ status: 'persisted' });
      await expect(
        workflows.loadWorkflowSnapshot({
          workflowName: 'first-step-update-workflow',
          runId: snapshot.runId,
        }),
      ).resolves.toMatchObject({ runId: snapshot.runId, status: 'running' });
    } finally {
      await store.close();
    }
  });
});
