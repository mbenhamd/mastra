/**
 * Owned nested-run authority for durable resume/discovery.
 *
 * The evented engine persists nested iteration runs under derived run ids and
 * records the owned id on the outer step metadata (`nestedRunId`). That owned
 * id is authoritative: resume, rehydration, and listing must select the owned
 * row exclusively and fail closed when it is missing or malformed — even when
 * a stale same-parent-ID row exists. The same-parent-ID lookup applies only to
 * the legacy/default case without owned metadata.
 */

import { MockLanguageModelV2, convertArrayToReadableStream } from '@internal/ai-sdk-v5/test';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { z } from 'zod/v4';
import { EventEmitterPubSub } from '../../../events/event-emitter';
import { Mastra } from '../../../mastra';
import { InMemoryStore } from '../../../storage';
import { createTool } from '../../../tools';
import { Agent } from '../../agent';
import { DurableStepIds } from '../constants';
import { createEventedAgent } from '../create-evented-agent';

const openPubsubs: EventEmitterPubSub[] = [];

afterEach(async () => {
  vi.unstubAllEnvs();
  await Promise.all(openPubsubs.splice(0).map(pubsub => pubsub.close()));
});

function toolCallModel(toolCallId = 'owned-nested-call') {
  return new MockLanguageModelV2({
    doStream: async () => ({
      rawCall: { rawPrompt: null, rawSettings: {} },
      warnings: [],
      stream: convertArrayToReadableStream([
        { type: 'stream-start', warnings: [] },
        { type: 'response-metadata', id: 'tool-turn', modelId: 'mock-model', timestamp: new Date(0) },
        {
          type: 'tool-call',
          toolCallType: 'function',
          toolCallId,
          toolName: 'protectedTool',
          input: JSON.stringify({ value: 'persisted' }),
          providerExecuted: false,
        },
        {
          type: 'finish',
          finishReason: 'tool-calls',
          usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
        },
      ]),
    }),
  });
}

function textModel() {
  return new MockLanguageModelV2({
    doStream: async () => ({
      rawCall: { rawPrompt: null, rawSettings: {} },
      warnings: [],
      stream: convertArrayToReadableStream([
        { type: 'stream-start', warnings: [] },
        { type: 'response-metadata', id: 'text-turn', modelId: 'mock-model', timestamp: new Date(0) },
        { type: 'text-start', id: 'text-1' },
        { type: 'text-delta', id: 'text-1', delta: 'recovered.' },
        { type: 'text-end', id: 'text-1' },
        {
          type: 'finish',
          finishReason: 'stop',
          usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
        },
      ]),
    }),
  });
}

function createAgent(storage: InMemoryStore, model: any, execute: any = vi.fn().mockResolvedValue({ ok: true })) {
  const tool = createTool({
    id: 'protectedTool',
    description: 'Requires an explicit approval',
    inputSchema: z.object({ value: z.string() }),
    requireApproval: true,
    execute,
  });
  const baseAgent = new Agent({
    id: 'owned-nested-agent',
    name: 'owned-nested-agent',
    instructions: 'Use the protected tool.',
    model: model as any,
    tools: { protectedTool: tool },
  });
  const pubsub = new EventEmitterPubSub();
  openPubsubs.push(pubsub);
  const agent = createEventedAgent({ agent: baseAgent, pubsub });
  new Mastra({ logger: false, storage, agents: { [agent.id]: agent as any } });
  return { agent, execute };
}

function parseSnapshot(snapshot: unknown): any {
  return typeof snapshot === 'string' ? JSON.parse(snapshot) : structuredClone(snapshot);
}

/** Stream a real suspended run on the evented engine and return its owned nested id. */
async function streamEventedSuspendedRun(storage: InMemoryStore, runId: string, toolCallId: string) {
  const { agent } = createAgent(storage, toolCallModel(toolCallId));
  expect((agent.getWorkflow() as any).engineType).toBe('evented');
  const started = await agent.stream('run it', {
    runId,
    requireToolApproval: true,
    memory: { thread: 'thread-1', resource: 'resource-1' },
  });
  await vi.waitFor(async () => {
    expect((await agent.listSuspendedRuns({ resourceId: 'resource-1' })).total).toBe(1);
  });
  started.cleanup();

  const workflows = (await storage.getStore('workflows'))!;
  const outerRow = (await workflows.getWorkflowRunById({
    workflowName: DurableStepIds.AGENTIC_LOOP,
    runId,
  })) as any;
  const outerSnapshot = parseSnapshot(outerRow.snapshot);
  const ownedNestedRunId = outerSnapshot?.context?.[DurableStepIds.AGENTIC_EXECUTION]?.metadata?.nestedRunId;
  expect(typeof ownedNestedRunId).toBe('string');
  expect(ownedNestedRunId).not.toBe(runId);
  const ownedRow = (await workflows.getWorkflowRunById({
    workflowName: DurableStepIds.AGENTIC_EXECUTION,
    runId: ownedNestedRunId,
  })) as any;
  expect(ownedRow).toBeTruthy();
  return { runId, toolCallId, ownedNestedRunId: ownedNestedRunId as string };
}

describe('DurableAgent owned nested-run authority', () => {
  it('fails closed when the owned nested row is missing, even with a valid same-ID row', async () => {
    const storage = new InMemoryStore();
    const runId = 'owned-nested-missing-run';
    const toolCallId = 'owned-nested-missing-call';
    const { ownedNestedRunId } = await streamEventedSuspendedRun(storage, runId, toolCallId);

    const workflows = (await storage.getStore('workflows'))!;
    const ownedRow = (await workflows.getWorkflowRunById({
      workflowName: DurableStepIds.AGENTIC_EXECUTION,
      runId: ownedNestedRunId,
    })) as any;
    // Plant a pair-consistent same-parent-ID row, then remove the owned row.
    // Falling back to the same-ID row must not satisfy the resume.
    await workflows.persistWorkflowSnapshot({
      workflowName: DurableStepIds.AGENTIC_EXECUTION,
      runId,
      resourceId: ownedRow.resourceId,
      snapshot: parseSnapshot(ownedRow.snapshot),
    });
    await workflows.deleteWorkflowRunById({ workflowName: DurableStepIds.AGENTIC_EXECUTION, runId: ownedNestedRunId });

    const restarted = createAgent(storage, textModel());
    await expect(restarted.agent.listSuspendedRuns({ resourceId: 'resource-1' })).resolves.toMatchObject({
      total: 0,
      runs: [],
    });

    const error = await restarted.agent
      .resume(runId, { approved: true }, { toolCallId, memory: { thread: 'thread-1', resource: 'resource-1' } })
      .then(
        () => null,
        (cause: unknown) => cause as { id?: string },
      );
    expect(error).toBeTruthy();
    expect(String(error?.id ?? '')).toMatch(/^DURABLE_AGENT_RESUME_/);
  });

  it('resumes from the owned nested row while a stale same-ID row exists', async () => {
    const storage = new InMemoryStore();
    const runId = 'owned-nested-stale-run';
    const toolCallId = 'owned-nested-stale-call';
    const { ownedNestedRunId } = await streamEventedSuspendedRun(storage, runId, toolCallId);

    const workflows = (await storage.getStore('workflows'))!;
    const ownedRow = (await workflows.getWorkflowRunById({
      workflowName: DurableStepIds.AGENTIC_EXECUTION,
      runId: ownedNestedRunId,
    })) as any;
    // Plant a stale same-parent-ID row: still suspended, but owned by another
    // agent so the pair check fails on it. It must neither satisfy nor reject
    // the resume of the valid owned row.
    const staleSnapshot = parseSnapshot(ownedRow.snapshot);
    staleSnapshot.context.input.agentId = 'stale-other-agent';
    await workflows.persistWorkflowSnapshot({
      workflowName: DurableStepIds.AGENTIC_EXECUTION,
      runId,
      resourceId: ownedRow.resourceId,
      snapshot: staleSnapshot,
    });

    const { agent, execute } = createAgent(storage, textModel());
    await expect(agent.listSuspendedRuns({ resourceId: 'resource-1' })).resolves.toMatchObject({
      total: 1,
      runs: [{ runId, toolCalls: [{ toolCallId }] }],
    });

    const resumed = await agent.resume(
      runId,
      { approved: true },
      { toolCallId, memory: { thread: 'thread-1', resource: 'resource-1' } },
    );
    await resumed.output.consumeStream();
    expect(execute).toHaveBeenCalledOnce();
    expect(await resumed.output.text).toContain('recovered.');
    resumed.cleanup();
  });

  it('rejects when owned metadata and suspension routing disagree, without executing', async () => {
    const storage = new InMemoryStore();
    const runId = 'owned-nested-routing-conflict-run';
    const toolCallId = 'owned-nested-routing-conflict-call';
    const { ownedNestedRunId } = await streamEventedSuspendedRun(storage, runId, toolCallId);

    const workflows = (await storage.getStore('workflows'))!;
    const outerRow = (await workflows.getWorkflowRunById({
      workflowName: DurableStepIds.AGENTIC_LOOP,
      runId,
    })) as any;
    const outerSnapshot = parseSnapshot(outerRow.snapshot);
    const agenticStep = outerSnapshot?.context?.[DurableStepIds.AGENTIC_EXECUTION];
    // Canary: the genuine evented snapshot agrees — routing names the owned run.
    expect(agenticStep?.suspendPayload?.__workflow_meta?.runId).toBe(ownedNestedRunId);
    // Tamper only the routing identity: owned metadata still names N while
    // native dispatch would target M. The pair must reject before dispatch.
    agenticStep.suspendPayload.__workflow_meta.runId = 'contradictory-nested-run';
    await workflows.persistWorkflowSnapshot({
      workflowName: DurableStepIds.AGENTIC_LOOP,
      runId,
      resourceId: outerRow.resourceId,
      snapshot: outerSnapshot,
    });

    const restarted = createAgent(storage, textModel());
    await expect(restarted.agent.listSuspendedRuns({ resourceId: 'resource-1' })).resolves.toMatchObject({
      total: 0,
      runs: [],
    });
    const error = await restarted.agent
      .resume(runId, { approved: true }, { toolCallId, memory: { thread: 'thread-1', resource: 'resource-1' } })
      .then(
        () => null,
        (cause: unknown) => cause as { id?: string },
      );
    expect(error).toBeTruthy();
    expect(String(error?.id ?? '')).toMatch(/^DURABLE_AGENT_RESUME_/);
    expect(restarted.execute).not.toHaveBeenCalled();
  });

  it('rejects malformed owned metadata, without executing', async () => {
    const storage = new InMemoryStore();
    const runId = 'owned-nested-malformed-run';
    const toolCallId = 'owned-nested-malformed-call';
    await streamEventedSuspendedRun(storage, runId, toolCallId);

    const workflows = (await storage.getStore('workflows'))!;
    const outerRow = (await workflows.getWorkflowRunById({
      workflowName: DurableStepIds.AGENTIC_LOOP,
      runId,
    })) as any;
    const outerSnapshot = parseSnapshot(outerRow.snapshot);
    outerSnapshot.context[DurableStepIds.AGENTIC_EXECUTION].metadata.nestedRunId = 12345;
    await workflows.persistWorkflowSnapshot({
      workflowName: DurableStepIds.AGENTIC_LOOP,
      runId,
      resourceId: outerRow.resourceId,
      snapshot: outerSnapshot,
    });

    const restarted = createAgent(storage, textModel());
    await expect(restarted.agent.listSuspendedRuns({ resourceId: 'resource-1' })).resolves.toMatchObject({
      total: 0,
      runs: [],
    });
    const error = await restarted.agent
      .resume(runId, { approved: true }, { toolCallId, memory: { thread: 'thread-1', resource: 'resource-1' } })
      .then(
        () => null,
        (cause: unknown) => cause as { id?: string },
      );
    expect(error).toBeTruthy();
    expect(String(error?.id ?? '')).toMatch(/^DURABLE_AGENT_RESUME_/);
    expect(restarted.execute).not.toHaveBeenCalled();
  });

  it('rejects a resource-mismatched nested row, without executing', async () => {
    const storage = new InMemoryStore();
    const runId = 'owned-nested-resource-conflict-run';
    const toolCallId = 'owned-nested-resource-conflict-call';
    const { ownedNestedRunId } = await streamEventedSuspendedRun(storage, runId, toolCallId);

    const workflows = (await storage.getStore('workflows'))!;
    const ownedRow = (await workflows.getWorkflowRunById({
      workflowName: DurableStepIds.AGENTIC_EXECUTION,
      runId: ownedNestedRunId,
    })) as any;
    // Same snapshot content, but the row now claims another resource: the
    // outer/nested ownership pair no longer agrees.
    await workflows.persistWorkflowSnapshot({
      workflowName: DurableStepIds.AGENTIC_EXECUTION,
      runId: ownedNestedRunId,
      resourceId: 'other-resource',
      snapshot: parseSnapshot(ownedRow.snapshot),
    });

    const restarted = createAgent(storage, textModel());
    await expect(restarted.agent.listSuspendedRuns({ resourceId: 'resource-1' })).resolves.toMatchObject({
      total: 0,
      runs: [],
    });
    const error = await restarted.agent
      .resume(runId, { approved: true }, { toolCallId, memory: { thread: 'thread-1', resource: 'resource-1' } })
      .then(
        () => null,
        (cause: unknown) => cause as { id?: string },
      );
    expect(error).toBeTruthy();
    expect(String(error?.id ?? '')).toMatch(/^DURABLE_AGENT_RESUME_/);
    expect(restarted.execute).not.toHaveBeenCalled();
  });
});
