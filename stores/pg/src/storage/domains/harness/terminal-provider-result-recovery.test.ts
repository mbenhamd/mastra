/**
 * PF-5566 (PF-5177 R1): a native terminal turn whose provider already finished
 * must recover as completed, with its reply and usage, even when its process
 * dies before the run is treated as complete. Process A runs a real Agent
 * whose `processOutputResult` (where memory persists a turn) never returns,
 * which models a death after the provider call and before completion. Process
 * B adopts the turn over the same PostgreSQL store.
 */
import { randomUUID } from 'node:crypto';

import { Agent } from '@mastra/core/agent';
import { Harness } from '@mastra/core/harness/v1';
import type { Processor } from '@mastra/core/processors';
import { InMemoryStore } from '@mastra/core/storage';
import type { HarnessTerminalFinalizer, HarnessTerminalFinalizerInput } from '@mastra/core/storage';
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from 'vitest';

import { PostgresStore } from '../..';

vi.setConfig({ testTimeout: 60_000, hookTimeout: 60_000 });

// The shared `test-utils` module pulls in the memory test suites; this file
// needs only the connection.
const TEST_CONFIG = {
  host: process.env.POSTGRES_HOST || '127.0.0.1',
  port: Number(process.env.POSTGRES_PORT) || 5434,
  database: process.env.POSTGRES_DB || 'postgres',
  user: process.env.POSTGRES_USER || 'postgres',
  password: process.env.POSTGRES_PASSWORD || 'postgres',
};

const LEASE_AND_DISPATCH_CLAIM_LAPSED_MS = 31_000;
const REPLY = 'The finished answer.';
const USAGE = { inputTokens: 11, outputTokens: 7, totalTokens: 18 };
const grant = { key: 'pf5566-grant', generation: 1 };
const admissionId = 'pf5566-turn';

/** A provider that answers once with REPLY and USAGE. */
function answeringModel(calls: { count: number }) {
  return {
    specificationVersion: 'v2' as const,
    provider: 'pf5566',
    modelId: 'pf5566-model',
    supportedUrls: {},
    doGenerate: async () => {
      throw new Error('doGenerate is not used');
    },
    doStream: async () => {
      calls.count += 1;
      const parts = [
        { type: 'stream-start', warnings: [] },
        { type: 'response-metadata', id: 'response-1', modelId: 'pf5566-model', timestamp: new Date(0) },
        { type: 'text-start', id: 'text-1' },
        { type: 'text-delta', id: 'text-1', delta: REPLY },
        { type: 'text-end', id: 'text-1' },
        { type: 'finish', finishReason: 'stop', usage: USAGE },
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

function recordingFinalizer(calls: HarnessTerminalFinalizerInput[]): HarnessTerminalFinalizer {
  return {
    id: 'doxa.chat-usage.terminal',
    version: '2',
    finalize: async input => {
      calls.push(input);
      return {
        projectionKind: 'chat.summary.v2',
        projectionId: input.result.runId,
        payload: { status: input.result.status },
      };
    },
  };
}

describe('PF-5566 provider result recovery (PostgreSQL)', () => {
  const schemaName = `pf5566_${randomUUID().replaceAll('-', '_')}`;
  const stores: PostgresStore[] = [];
  const harnesses: Harness[] = [];
  // A process that died mid-turn is never shut down cleanly.
  const abandoned = new Set<Harness>();

  function storeFor(id: string) {
    const store = new PostgresStore({
      ...TEST_CONFIG,
      id,
      schemaName,
      enabledDomains: ['harness'],
      sessionRecordProjection: { enabled: true },
      terminalHandoff: { enabled: true },
    } as never);
    stores.push(store);
    return store;
  }

  async function harnessProcess(id: string, agent: Agent<any, any, any>, finalizer: HarnessTerminalFinalizer) {
    const store = storeFor(id);
    await store.init();
    const storage = store.stores.harness!;
    const harness = new Harness({
      agents: { default: agent } as never,
      storage: new InMemoryStore(),
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
      sessions: { storage, terminalHandoff: { finalizer } },
    });
    harnesses.push(harness);
    return { harness, storage };
  }

  beforeAll(async () => {
    await storeFor('pf5566-schema').init();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  afterAll(async () => {
    for (const harness of harnesses) {
      if (!abandoned.has(harness)) await harness.shutdown().catch(() => {});
    }
    const admin = stores[0]!;
    await admin.db.none(`DROP SCHEMA IF EXISTS "${schemaName}" CASCADE`);
    for (const store of stores) await store.close().catch(() => {});
  });

  it('recovers a turn whose provider finished before its process died as completed, with its reply and usage', async () => {
    const providerCalls = { count: 0 };
    let reachedAfterProvider!: () => void;
    const afterProvider = new Promise<void>(resolve => (reachedAfterProvider = resolve));
    // The process dies here: the provider call is complete, and the turn is
    // being persisted to memory. This promise never settles.
    const dyingMemoryPersistence: Processor = {
      id: 'dying-memory-persistence',
      processOutputResult: async () => {
        reachedAfterProvider();
        return await new Promise<never>(() => {});
      },
    };
    const ownerAgent = new Agent({
      id: 'default',
      name: 'default',
      instructions: 'reply',
      model: answeringModel(providerCalls) as never,
      outputProcessors: [dyingMemoryPersistence],
    });
    const ownerCalls: HarnessTerminalFinalizerInput[] = [];
    const owner = await harnessProcess('pf5566-owner', ownerAgent, recordingFinalizer(ownerCalls));
    abandoned.add(owner.harness);
    // The early stage runs alongside the output processors: let it commit
    // before the owner is treated as dead (bounded, so a build without it
    // still reaches the assertions).
    let stageCommitted!: () => void;
    const staged = new Promise<void>(resolve => (stageCommitted = resolve));
    const stage = owner.storage.stageTerminalCompletion.bind(owner.storage);
    vi.spyOn(owner.storage, 'stageTerminalCompletion').mockImplementation(async input => {
      const receipt = await stage(input);
      stageCommitted();
      return receipt;
    });
    const session = await owner.harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    void session
      .message({
        content: 'hi',
        admissionId,
        executionAuthorityGrant: grant,
        terminalAdmissionSeed: { v: 2 },
      } as never)
      .catch(() => {});
    await afterProvider;
    await Promise.race([staged, new Promise(resolve => setTimeout(resolve, 2_000))]);
    expect(providerCalls.count).toBe(1);

    // The owner is gone; its lease and dispatch claim lapse.
    vi.useFakeTimers({ toFake: ['Date'] });
    vi.setSystemTime(Date.now() + LEASE_AND_DISPATCH_CLAIM_LAPSED_MS);
    const adopterProviderCalls = { count: 0 };
    const adopterAgent = new Agent({
      id: 'default',
      name: 'default',
      instructions: 'reply',
      model: answeringModel(adopterProviderCalls) as never,
    });
    const adopterCalls: HarnessTerminalFinalizerInput[] = [];
    const adopter = await harnessProcess('pf5566-adopter', adopterAgent, recordingFinalizer(adopterCalls));
    await adopter.harness.session({ sessionId: session.id, resourceId: 'u1' });

    const admission = await adopter.storage.loadTerminalAdmission({
      harnessName: 'default',
      sessionId: session.id,
      admissionId,
      executionGrant: grant,
    });
    // Recovery settles the finished turn through the recorded finalizer as
    // completed, never as interrupted, and never calls the provider again.
    expect(admission).toMatchObject({
      status: 'committed',
      finalizerVersion: '2',
      terminalResult: { status: 'completed' },
    });
    expect(adopterProviderCalls.count).toBe(0);
    expect(ownerCalls).toEqual([]);
    expect(adopterCalls).toHaveLength(1);
    expect(adopterCalls[0]!.result.status).toBe('completed');
    const recovered = adopterCalls[0]!.fullOutput as { text?: unknown; totalUsage?: Record<string, unknown> };
    expect(recovered.text).toBe(REPLY);
    expect(recovered.totalUsage).toMatchObject(USAGE);

    const evidence = await adopter.storage.loadMessageResultEvidence({
      harnessName: 'default',
      sessionId: session.id,
      resourceId: session.resourceId,
      threadId: session.threadId,
      signalId: admission!.signalId,
    });
    expect(evidence).toMatchObject({ status: 'completed', result: { text: REPLY } });
  });

  it('completes a live turn normally when staging its provider result fails', async () => {
    const providerCalls = { count: 0 };
    const agent = new Agent({
      id: 'default',
      name: 'default',
      instructions: 'reply',
      model: answeringModel(providerCalls) as never,
    });
    const calls: HarnessTerminalFinalizerInput[] = [];
    const live = await harnessProcess('pf5566-live', agent, recordingFinalizer(calls));
    const stage = vi.spyOn(live.storage, 'stageTerminalCompletion');
    stage.mockRejectedValueOnce(new Error('storage unavailable'));
    const session = await live.harness.session({ resourceId: 'u2', threadId: { fresh: true } });
    const liveAdmissionId = 'pf5566-live-turn';
    const liveGrant = { key: 'pf5566-live-grant', generation: 1 };

    const full = (await session.message({
      content: 'hi',
      admissionId: liveAdmissionId,
      executionAuthorityGrant: liveGrant,
      terminalAdmissionSeed: { v: 2 },
    } as never)) as { text?: unknown };

    expect(full.text).toBe(REPLY);
    expect(providerCalls.count).toBe(1);
    await expect(
      live.storage.loadTerminalAdmission({
        harnessName: 'default',
        sessionId: session.id,
        admissionId: liveAdmissionId,
        executionGrant: liveGrant,
      }),
    ).resolves.toMatchObject({ status: 'committed', terminalResult: { status: 'completed' } });
    expect(calls.map(call => (call.fullOutput as { text?: unknown }).text)).toEqual([REPLY]);
  });
});
