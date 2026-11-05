/**
 * The plan-task cap counts live tasks only. A long thread that keeps finishing
 * its plans across turns must still be able to plan again, with the history
 * stored in PostgreSQL and the current plan readable through the paged check.
 */
import { randomUUID } from 'node:crypto';

import { Agent } from '@mastra/core/agent';
import { Harness } from '@mastra/core/harness/v1';
import { InMemoryStore } from '@mastra/core/storage';
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';

import { PostgresStore } from '../..';

vi.setConfig({ testTimeout: 120_000, hookTimeout: 60_000 });

// The shared `test-utils` module pulls in the memory test suites; this file
// needs only the connection.
const TEST_CONFIG = {
  host: process.env.POSTGRES_HOST || '127.0.0.1',
  port: Number(process.env.POSTGRES_PORT) || 5434,
  database: process.env.POSTGRES_DB || 'postgres',
  user: process.env.POSTGRES_USER || 'postgres',
  password: process.env.POSTGRES_PASSWORD || 'postgres',
};

/** A provider that answers every turn with a short reply. */
function answeringModel() {
  return {
    specificationVersion: 'v2' as const,
    provider: 'plan-cap',
    modelId: 'plan-cap-model',
    supportedUrls: {},
    doGenerate: async () => {
      throw new Error('doGenerate is not used');
    },
    doStream: async () => {
      const parts = [
        { type: 'stream-start', warnings: [] },
        { type: 'response-metadata', id: 'response-1', modelId: 'plan-cap-model', timestamp: new Date(0) },
        { type: 'text-start', id: 'text-1' },
        { type: 'text-delta', id: 'text-1', delta: 'done' },
        { type: 'text-end', id: 'text-1' },
        { type: 'finish', finishReason: 'stop', usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 } },
      ];
      return {
        stream: new ReadableStream({
          start(controller) {
            for (const part of parts) controller.enqueue(part);
            controller.close();
          },
        }),
        rawCall: { rawPrompt: null, rawSettings: {} },
        warnings: [],
      };
    },
  };
}

describe('plan-task live cap across turns (PostgreSQL)', () => {
  const schemaName = `plan_cap_${randomUUID().replaceAll('-', '_')}`;
  let store: PostgresStore;
  let harness: Harness;

  beforeAll(async () => {
    store = new PostgresStore({
      ...TEST_CONFIG,
      id: 'plan-cap-store',
      schemaName,
      enabledDomains: ['harness'],
    } as never);
    await store.init();
    harness = new Harness({
      agents: {
        default: new Agent({ id: 'default', name: 'default', instructions: 'reply', model: answeringModel() as never }),
      } as never,
      storage: new InMemoryStore(),
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
      sessions: { storage: store.stores.harness! },
    });
  });

  afterAll(async () => {
    await harness?.shutdown().catch(() => {});
    await store?.db.none(`DROP SCHEMA IF EXISTS "${schemaName}" CASCADE`);
    await store?.close().catch(() => {});
  });

  it('spends more than 100 tasks over several turns and still plans the next request', async () => {
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const roots: string[] = [];
    // Five requests, one turn each: a root plus 29 steps, all finished.
    for (let turn = 0; turn < 5; turn++) {
      await session.message({ content: `request ${turn}` });
      const root = await session._planTaskAdd({ content: `request ${turn}` });
      roots.push(root.taskId);
      const steps = await session._planTaskDecompose(
        root.taskId,
        Array.from({ length: 29 }, (_, i) => ({ content: `request ${turn} step ${i}` })),
      );
      for (const step of steps) await session._planTaskComplete(step.taskId);
    }
    const storage = store.stores.harness!;
    const history = await storage.countPlanTasksByStatus({ harnessName: 'default', sessionId: session.id });
    expect(history).toMatchObject({ total: 150, byStatus: { completed: 150 } });

    // The sixth request plans a full live plan on top of the history.
    await session.message({ content: 'request 5' });
    const next = await session._planTaskAdd({ content: 'request 5' });
    const nextSteps = await session._planTaskDecompose(
      next.taskId,
      Array.from({ length: 99 }, (_, i) => ({ content: `request 5 step ${i}` })),
    );
    expect(nextSteps).toHaveLength(99);
    await expect(session._planTaskAdd({ content: 'one too many' })).rejects.toThrow(/limit is 100 live/);

    const after = await storage.countPlanTasksByStatus({ harnessName: 'default', sessionId: session.id });
    expect(after).toMatchObject({ total: 250, rootCount: 6, byStatus: { completed: 150, pending: 100 } });
    expect(session.getDisplayState().planTasks).toMatchObject({ total: 250, byStatus: { pending: 100 } });

    // The paged check walks past the finished roots to the current plan.
    const seen: string[] = [];
    let cursor: string | undefined;
    do {
      const page = await session._planTaskCheck({ depth: 0, limit: 2, ...(cursor ? { cursor } : {}) });
      seen.push(...page.tasks.map(task => task.taskId));
      cursor = page.nextCursor;
    } while (cursor !== undefined);
    expect(seen).toEqual([...roots, next.taskId]);
  });
});
