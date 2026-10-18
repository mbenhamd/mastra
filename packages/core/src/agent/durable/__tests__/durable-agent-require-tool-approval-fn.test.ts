/**
 * DurableAgent function-form requireToolApproval serialization.
 *
 * A function policy cannot cross a process boundary: the closure stays on the in-process run
 * registry, and the serialized workflow input carries the safe `true` shadow. Behaviour (per-call
 * evaluation, throwing policies, resume context) is covered on every engine by the
 * `approval-policy` aimock scenario.
 */

import type { LanguageModelV2 } from '@ai-sdk/provider-v5';
import { MockLanguageModelV2 } from '@internal/ai-sdk-v5/test';
import { describe, it, expect, vi, afterEach } from 'vitest';
import { z } from 'zod';
import { EventEmitterPubSub } from '../../../events/event-emitter';
import { createTool } from '../../../tools';
import { Agent } from '../../agent';
import { createDurableAgent } from '../create-durable-agent';
import { globalRunRegistry } from '../run-registry';

describe('DurableAgent function-form requireToolApproval', () => {
  const pubsub = new EventEmitterPubSub();

  afterEach(async () => {
    await pubsub.close();
  });

  it('stores function policy on the registry and serializes a `true` shadow into workflow input', async () => {
    const safeTool = createTool({
      id: 'safeTool',
      description: 'safe',
      inputSchema: z.object({ x: z.number() }),
      execute: async () => 'ok',
    });
    const baseAgent = new Agent({
      id: 'fn-policy-agent',
      name: 'Fn Policy Agent',
      instructions: 'tools',
      model: new MockLanguageModelV2() as LanguageModelV2,
      tools: { safeTool },
    });
    const durableAgent = createDurableAgent({ agent: baseAgent, pubsub });

    const policy = vi.fn().mockReturnValue(false);
    const prep = await durableAgent.prepare('Run safeTool', { requireToolApproval: policy });

    // Cross-process shadow is the safe default `true`.
    expect(prep.workflowInput.options.requireToolApproval).toBe(true);
    // In-process registry preserves the closure verbatim.
    const entry = globalRunRegistry.get(prep.runId);
    expect(entry?.requireToolApproval).toBe(policy);

    durableAgent.runRegistry.cleanup(prep.runId);
    globalRunRegistry.delete(prep.runId);
  });

  it('evaluates function policy per tool call with real toolName/args', async () => {
    const mockModel = createMultipleToolCallsModel([
      { name: 'readTool', args: { id: 'safe-1' } },
      { name: 'writeTool', args: { id: 'danger-1' } },
    ]);

    const readTool = createTool({
      id: 'readTool',
      description: 'read',
      inputSchema: z.object({ id: z.string() }),
      execute: async () => 'read-ok',
    });
    const writeTool = createTool({
      id: 'writeTool',
      description: 'write',
      inputSchema: z.object({ id: z.string() }),
      execute: async () => 'write-ok',
    });

    const baseAgent = new Agent({
      id: 'per-call-agent',
      name: 'Per Call Agent',
      instructions: 'tools',
      model: mockModel as LanguageModelV2,
      tools: { readTool, writeTool },
    });
    const durableAgent = createDurableAgent({ agent: baseAgent, pubsub });

    // Gate only `writeTool` — `readTool` should run without approval.
    const policy = vi.fn(({ toolName }: { toolName: string }) => toolName === 'writeTool');

    const runId = 'per-call-approval-policy-run';
    const events = collectPubsubEvents(pubsub, runId);

    const { cleanup } = await durableAgent.stream('Run both tools', {
      runId,
      requireToolApproval: policy,
    });

    // Let the workflow run far enough for the gate decisions to happen.
    await delay(600);

    // Policy must have been invoked for each tool call, with the actual args.
    expect(policy).toHaveBeenCalled();
    const callArgs = policy.mock.calls.map(args => args[0]);
    const calledNames = callArgs.map(ctx => ctx.toolName).sort();
    expect(calledNames).toEqual(['readTool', 'writeTool']);

    // Sanity: the args object was forwarded with the real values.
    const writeCall = callArgs.find(ctx => ctx.toolName === 'writeTool');
    expect(writeCall?.args).toEqual({ id: 'danger-1' });
    const readCall = callArgs.find(ctx => ctx.toolName === 'readTool');
    expect(readCall?.args).toEqual({ id: 'safe-1' });

    // Only writeTool should have triggered an approval chunk on pubsub.
    const approvalChunks = events.filter(
      e => e.type === AgentStreamEventTypes.CHUNK && (e as any).data?.type === 'tool-call-approval',
    );
    const approvalNames = approvalChunks.map(e => (e as any).data?.payload?.toolName);
    expect(approvalNames).toContain('writeTool');
    expect(approvalNames).not.toContain('readTool');

    cleanup();
  });

  it('evaluates a warm-resumed policy with the resumed caller context', async () => {
    const mockModel = createToolCallThenTextModel({ name: 'tenantTool', args: { value: 'test' } });
    const execute = vi.fn(async () => 'ok');
    const tenantTool = createTool({
      id: 'tenantTool',
      description: 'tenant-aware tool',
      inputSchema: z.object({ value: z.string() }),
      execute,
    });
    const baseAgent = new Agent({
      id: 'warm-resume-policy-agent',
      name: 'Warm Resume Policy Agent',
      instructions: 'Use the tenant tool.',
      model: mockModel as LanguageModelV2,
      tools: { tenantTool },
    });
    const durableAgent = createDurableAgent({ agent: baseAgent, pubsub });
    new Mastra({
      logger: false,
      storage: new MockStore(),
      agents: { warmResumePolicyAgent: durableAgent },
    });

    const policy = vi.fn(() => true);
    let suspendedData: unknown;
    const initial = await durableAgent.stream('Run the tenant tool', {
      requireToolApproval: policy,
      requestContext: new RequestContext([
        ['tenantId', 'initial-tenant'],
        ['authToken', 'initial-auth'],
      ]),
      onSuspended: data => {
        suspendedData = data;
      },
    });
    await vi.waitFor(() => expect(suspendedData).toBeDefined());

    policy.mockClear();
    let finishData: unknown;
    const resumed = await durableAgent.resume(
      initial.runId,
      { approved: true },
      {
        requestContext: new RequestContext([
          ['tenantId', 'resume-tenant'],
          ['authToken', 'resume-auth'],
        ]),
        onFinish: data => {
          finishData = data;
        },
      },
    );
    await vi.waitFor(() => expect(finishData).toBeDefined());

    expect(policy).toHaveBeenCalledWith(
      expect.objectContaining({
        requestContext: expect.objectContaining({
          tenantId: 'resume-tenant',
          authToken: 'resume-auth',
        }),
      }),
    );
    expect(policy).toHaveBeenCalledTimes(1);
    expect(execute).toHaveBeenCalledTimes(1);

    resumed.cleanup();
    initial.cleanup();
  });

  it('defaults to "require approval" when the function policy throws (safe default)', async () => {
    const mockModel = createMultipleToolCallsModel([{ name: 'someTool', args: { ok: true } }]);
    const someTool = createTool({
      id: 'someTool',
      description: 'a tool',
      inputSchema: z.object({ ok: z.boolean() }),
      execute: async () => 'done',
    });

    const baseAgent = new Agent({
      id: 'throwing-policy-agent',
      name: 'Throwing Policy Agent',
      instructions: 'tools',
      model: mockModel as LanguageModelV2,
      tools: { someTool },
    });
    const durableAgent = createDurableAgent({ agent: baseAgent, pubsub });

    const policy = vi.fn(() => {
      throw new Error('boom');
    });

    const runId = 'throwing-approval-policy-run';
    const events = collectPubsubEvents(pubsub, runId);

    const { cleanup } = await durableAgent.stream('Run it', {
      runId,
      requireToolApproval: policy,
    });

    await delay(600);

    const approvalChunks = events.filter(
      e => e.type === AgentStreamEventTypes.CHUNK && (e as any).data?.type === 'tool-call-approval',
    );
    const approvalNames = approvalChunks.map(e => (e as any).data?.payload?.toolName);
    expect(approvalNames).toContain('someTool');

    cleanup();
  });
});
