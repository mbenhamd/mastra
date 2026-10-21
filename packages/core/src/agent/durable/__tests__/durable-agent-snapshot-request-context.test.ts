/**
 * PF-6182 (F3): `durableRequestContextKeys` bounds what a durable run stores,
 * not only its workflow input. The persisted workflow snapshot keeps the
 * allowlisted request context; everything else, credentials included, stays in
 * the live context, which the run's tools still see.
 */
import type { LanguageModelV2 } from '@ai-sdk/provider-v5';
import { MockLanguageModelV2, convertArrayToReadableStream } from '@internal/ai-sdk-v5/test';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { z } from 'zod';
import { EventEmitterPubSub } from '../../../events/event-emitter';
import { Mastra } from '../../../mastra';
import { RequestContext } from '../../../request-context';
import { MockStore } from '../../../storage/mock';
import { createTool } from '../../../tools';
import { Agent } from '../../agent';
import { createDurableAgent } from '../create-durable-agent';
import { globalRunRegistry } from '../run-registry';

function toolCallThenText(toolName: string) {
  let calls = 0;
  return new MockLanguageModelV2({
    doStream: async () => {
      calls += 1;
      const finish = (finishReason: 'tool-calls' | 'stop') => ({
        type: 'finish' as const,
        finishReason,
        usage: { inputTokens: 10, outputTokens: 20, totalTokens: 30 },
      });
      return {
        stream: convertArrayToReadableStream(
          calls === 1
            ? [
                { type: 'stream-start', warnings: [] },
                { type: 'response-metadata', id: 'id-0', modelId: 'mock-model-id', timestamp: new Date(0) },
                {
                  type: 'tool-call',
                  toolCallType: 'function',
                  toolCallId: 'call-1',
                  toolName,
                  input: JSON.stringify({ value: 'x' }),
                  providerExecuted: false,
                },
                finish('tool-calls'),
              ]
            : [
                { type: 'stream-start', warnings: [] },
                { type: 'response-metadata', id: 'id-1', modelId: 'mock-model-id', timestamp: new Date(0) },
                { type: 'text-start', id: 'text-1' },
                { type: 'text-delta', id: 'text-1', delta: 'done' },
                { type: 'text-end', id: 'text-1' },
                finish('stop'),
              ],
        ),
        rawCall: { rawPrompt: null, rawSettings: {} },
        warnings: [],
      };
    },
  });
}

describe('DurableAgent stored snapshot request context (PF-6182)', () => {
  let pubsub: EventEmitterPubSub;

  beforeEach(() => {
    pubsub = new EventEmitterPubSub();
  });

  afterEach(async () => {
    globalRunRegistry.clear();
    await pubsub.close();
  });

  it('stores only the allowlisted request context while tools keep the live context', async () => {
    const seenCredentials: unknown[] = [];
    const approvalTool = createTool({
      id: 'approvalTool',
      description: 'approval-gated tool',
      inputSchema: z.object({ value: z.string() }),
      requireApproval: true,
      execute: async (_input, context) => {
        seenCredentials.push(context?.requestContext?.get('credentials'));
        return 'ok';
      },
    });
    const baseAgent = new Agent({
      id: 'snapshot-context-agent',
      name: 'Snapshot Context Agent',
      instructions: 'Use the approval tool.',
      model: toolCallThenText('approvalTool') as LanguageModelV2,
      tools: { approvalTool },
    });
    const durableAgent = createDurableAgent({ agent: baseAgent, pubsub, durableRequestContextKeys: ['tenantId'] });
    const storage = new MockStore();
    new Mastra({ logger: false, storage, agents: { snapshotContextAgent: durableAgent } });

    const requestContext = new RequestContext();
    requestContext.set('tenantId', 'tenant-acme');
    requestContext.set('credentials', { accessToken: 'live-secret-token' });

    let suspended: unknown;
    const initial = await durableAgent.stream('Run the approval tool', {
      requestContext,
      onSuspended: data => {
        suspended = data;
      },
    });
    await vi.waitFor(() => expect(suspended).toBeDefined());

    const workflows = (await storage.getStore('workflows'))!;
    const stored = await workflows.getWorkflowRunById({ runId: initial.runId });
    expect(stored).not.toBeNull();
    const snapshot = typeof stored!.snapshot === 'string' ? JSON.parse(stored!.snapshot) : stored!.snapshot;
    expect(snapshot.requestContext?.tenantId).toBe('tenant-acme');
    expect(snapshot.requestContext).not.toHaveProperty('credentials');
    expect(JSON.stringify(snapshot)).not.toContain('live-secret-token');

    let finished: unknown;
    const resumed = await durableAgent.resume(
      initial.runId,
      { approved: true },
      {
        onFinish: data => {
          finished = data;
        },
      },
    );
    await vi.waitFor(() => expect(finished).toBeDefined());
    expect(seenCredentials).toEqual([{ accessToken: 'live-secret-token' }]);

    resumed.cleanup();
    initial.cleanup();
  });
});
