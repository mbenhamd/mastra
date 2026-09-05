import { TABLE_WORKFLOW_SNAPSHOT } from '@mastra/core/storage';
import type { WorkflowRunState } from '@mastra/core/workflows';
import type { GlideClient } from '@valkey/valkey-glide';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { WorkflowsValkey } from './storage';
import type { ValkeyClient } from './storage';
import { getKey } from './storage/domains/utils';
import { ValkeyServerCache } from './index';

const createClient = () =>
  ({
    customCommand: vi.fn(),
  }) as unknown as GlideClient;

const createWorkflowClient = () => {
  const records = new Map<string, string>();
  const client = {
    get: vi.fn(async (key: string) => records.get(key) ?? null),
    set: vi.fn(async (key: string, value: string) => {
      records.set(key, value);
      return 'OK';
    }),
    del: vi.fn(async (keys: string | string[]) => {
      const keysToDelete = Array.isArray(keys) ? keys : [keys];
      return keysToDelete.reduce((deleted, key) => deleted + Number(records.delete(key)), 0);
    }),
    scan: vi.fn(async (_cursor: string, { MATCH }: { MATCH: string }) => {
      const pattern = new RegExp(
        '^' +
          MATCH.split('*')
            .map(part => part.replace(/[.*+?^{}$()|[\]\\]/g, '\\$&'))
            .join('.*') +
          '$',
      );
      return { cursor: '0', keys: [...records.keys()].filter(key => pattern.test(key)) };
    }),
    mGet: vi.fn(async (keys: string[]) => keys.map(key => records.get(key) ?? null)),
  } as unknown as ValkeyClient;

  return { client, records };
};

describe('ValkeyServerCache', () => {
  let client: GlideClient;
  let command: ReturnType<typeof vi.fn>;
  let cache: ValkeyServerCache;

  beforeEach(() => {
    client = createClient();
    command = client.customCommand as ReturnType<typeof vi.fn>;
    cache = new ValkeyServerCache({ client });
  });

  it('stores and reads JSON values through GLIDE custom commands', async () => {
    command.mockResolvedValueOnce('OK').mockResolvedValueOnce('{"foo":"bar"}');

    await cache.set('key', { foo: 'bar' });
    const value = await cache.get('key');

    expect(command.mock.calls[0]?.[0]).toEqual(['SET', 'mastra:cache:key', '{"foo":"bar"}', 'EX', '300']);
    expect(command.mock.calls[1]?.[0]).toEqual(['GET', 'mastra:cache:key']);
    expect(value).toEqual({ foo: 'bar' });
  });

  it('clears all matching keys across scan pages', async () => {
    command
      .mockResolvedValueOnce(['12', ['mastra:cache:a']])
      .mockResolvedValueOnce(1)
      .mockResolvedValueOnce(['0', ['mastra:cache:b']])
      .mockResolvedValueOnce(1);

    await cache.clear();

    expect(command.mock.calls.map(call => call[0])).toEqual([
      ['SCAN', '0', 'MATCH', 'mastra:cache:*', 'COUNT', '100'],
      ['DEL', 'mastra:cache:a'],
      ['SCAN', '12', 'MATCH', 'mastra:cache:*', 'COUNT', '100'],
      ['DEL', 'mastra:cache:b'],
    ]);
  });
});

describe('WorkflowsValkey', () => {
  it('uses the run identity for resource-scoped load, update, listing, and delete', async () => {
    const { client, records } = createWorkflowClient();
    const workflows = new WorkflowsValkey({ client });
    const workflowName = 'resource-workflow';
    const runId = 'resource-run';
    const resourceId = 'resource-id';
    const snapshot = {
      runId,
      status: 'suspended',
      context: {},
    } as WorkflowRunState;

    await workflows.persistWorkflowSnapshot({ workflowName, runId, resourceId, snapshot });

    const key = getKey(TABLE_WORKFLOW_SNAPSHOT, {
      namespace: 'workflows',
      workflow_name: workflowName,
      run_id: runId,
    });
    expect([...records.keys()]).toEqual([key]);
    await expect(workflows.loadWorkflowSnapshot({ namespace: 'workflows', workflowName, runId })).resolves.toEqual(
      snapshot,
    );

    await expect(
      workflows.updateWorkflowState({ workflowName, runId, opts: { status: 'running' } }),
    ).resolves.toMatchObject({ status: 'running' });
    expect(JSON.parse(records.get(key)!).resourceId).toBe(resourceId);

    await workflows.persistWorkflowSnapshot({
      workflowName,
      runId: 'other-run',
      resourceId: 'other-resource',
      snapshot: { ...snapshot, runId: 'other-run' },
    });

    for (const filters of [{ resourceId }, { workflowName, resourceId }]) {
      const listed = await workflows.listWorkflowRuns(filters);
      expect(listed.runs.map(run => run.runId)).toEqual([runId]);
      expect(listed.total).toBe(1);
    }
    await expect(workflows.listWorkflowRuns({ workflowName, resourceId: 'missing-resource' })).resolves.toEqual({
      runs: [],
      total: 0,
    });

    await workflows.deleteWorkflowRunById({ workflowName, runId });
    await expect(workflows.loadWorkflowSnapshot({ namespace: 'workflows', workflowName, runId })).resolves.toBeNull();
  });
});
