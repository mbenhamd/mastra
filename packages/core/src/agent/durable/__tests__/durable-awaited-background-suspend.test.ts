/**
 * Awaited background-task suspension on the actual durable wrappers (S4).
 *
 * For both `createDurableAgent` and `createEventedAgent`: an awaited tool
 * suspends mid-background-execution, the outer run suspends with a persisted
 * checkpoint, and an agent-level `resume()` completes the SAME task exactly
 * once. Covers live (warm registry) and persisted-reattach (registry entry
 * evicted, storage as authority) resumes, preserving the suspend payload,
 * tool-call label, task identity, and actor on both attempts.
 */

import { MockLanguageModelV2, convertArrayToReadableStream } from '@internal/ai-sdk-v5/test';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { z } from 'zod/v4';
import { EventEmitterPubSub } from '../../../events/event-emitter';
import { Mastra } from '../../../mastra';
import { InMemoryStore } from '../../../storage';
import { createTool } from '../../../tools';
import { Agent } from '../../agent';
import { createDurableAgent } from '../create-durable-agent';
import { createEventedAgent } from '../create-evented-agent';

const openPubsubs: EventEmitterPubSub[] = [];
const shutdowns: Array<() => Promise<void>> = [];
const ACTOR = { actorKind: 'system', sourceWorkflow: 'awaited-bg' } as const;

afterEach(async () => {
  vi.unstubAllEnvs();
  await Promise.all(shutdowns.splice(0).map(fn => fn().catch(() => undefined)));
  await Promise.all(openPubsubs.splice(0).map(pubsub => pubsub.close()));
});

/** First turn dispatches the awaited tool call; later turns answer with text. */
function dispatchThenTextModel(toolCallId: string, finalText: string) {
  let callCount = 0;
  return new MockLanguageModelV2({
    doStream: async () => {
      callCount += 1;
      if (callCount === 1) {
        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream: convertArrayToReadableStream([
            { type: 'stream-start', warnings: [] },
            { type: 'response-metadata', id: 'id-0', modelId: 'mock', timestamp: new Date(0) },
            {
              type: 'tool-call',
              toolCallType: 'function',
              toolCallId,
              toolName: 'research',
              input: JSON.stringify({ topic: 'solana', _background: { disposition: 'awaited' } }),
              providerExecuted: false,
            },
            {
              type: 'finish',
              finishReason: 'tool-calls',
              usage: { inputTokens: 15, outputTokens: 10, totalTokens: 25 },
            },
          ]),
        };
      }
      return {
        rawCall: { rawPrompt: null, rawSettings: {} },
        warnings: [],
        stream: convertArrayToReadableStream([
          { type: 'stream-start', warnings: [] },
          { type: 'response-metadata', id: 'id-1', modelId: 'mock', timestamp: new Date(0) },
          { type: 'text-start', id: 'text-1' },
          { type: 'text-delta', id: 'text-1', delta: finalText },
          { type: 'text-end', id: 'text-1' },
          {
            type: 'finish',
            finishReason: 'stop',
            usage: { inputTokens: 20, outputTokens: 15, totalTokens: 25 },
          },
        ]),
      };
    },
  });
}

function setup(kind: 'durable' | 'evented', toolCallId: string) {
  const attempts: Array<{ actor: unknown }> = [];
  const execute = vi.fn(async ({ topic }: any, options: any) => {
    attempts.push({ actor: options?.actor });
    const ctx = options?.agent as
      | { suspend?: (data?: unknown) => Promise<void>; resumeData?: { approved?: boolean; notes?: string } }
      | undefined;
    const resumeData = ctx?.resumeData;
    if (!resumeData) {
      await ctx?.suspend?.({ awaiting: 'analyst-approval', topic });
      return { summary: '' };
    }
    if (resumeData.approved !== true) throw new Error(`Research on "${topic}" was declined`);
    return { summary: `Research complete on "${topic}": ${resumeData.notes ?? 'approved'}.` };
  });
  const researchTool = createTool({
    id: 'research',
    description: 'Research a topic. Suspends until an analyst approves.',
    inputSchema: z.object({ topic: z.string() }),
    outputSchema: z.object({ summary: z.string() }),
    execute: execute as any,
    background: { enabled: true },
  });
  const baseAgent = new Agent({
    id: `awaited-bg-${kind}`,
    name: `Awaited BG ${kind}`,
    instructions: 'Research when asked',
    model: dispatchThenTextModel(toolCallId, 'Done researching') as any,
    tools: { research: researchTool },
    backgroundTasks: { tools: { research: true } },
  });
  const pubsub = new EventEmitterPubSub();
  openPubsubs.push(pubsub);
  const wrapped =
    kind === 'durable'
      ? createDurableAgent({ agent: baseAgent, pubsub })
      : createEventedAgent({ agent: baseAgent, pubsub });
  const storage = new InMemoryStore();
  const mastra = new Mastra({
    logger: false,
    storage,
    backgroundTasks: { enabled: true },
    agents: { [wrapped.id]: wrapped as any },
  });
  // Assert after registration: engine resolution is memoized at first
  // getWorkflow(), and an unregistered EventedAgent falls back to default.
  if (kind === 'evented') expect((wrapped.getWorkflow() as any).engineType).toBe('evented');
  shutdowns.push(async () => {
    await mastra.backgroundTaskManager?.shutdown().catch(() => undefined);
    await mastra.shutdown().catch(() => undefined);
  });
  return { agent: wrapped, mastra, execute, attempts };
}

const memoryOptions = () => ({ memory: { thread: 'thread-1', resource: 'resource-1' } }) as const;

async function streamUntilSuspended(agent: any, runId: string) {
  const started = await agent.stream('Research solana', {
    runId,
    ...memoryOptions(),
    actor: { ...ACTOR },
  });
  let suspended = false;
  for await (const chunk of started.fullStream as AsyncIterable<any>) {
    if (chunk.type === 'tool-call-suspended') {
      suspended = true;
      break;
    }
  }
  expect(suspended).toBe(true);
  return started;
}

describe.each(['durable', 'evented'] as const)('awaited background suspension [%s]', kind => {
  it('live resume completes the same suspended task exactly once', async () => {
    const runId = `awaited-bg-live-${kind}`;
    const toolCallId = `awaited-bg-live-call-${kind}`;
    const { agent, mastra, execute, attempts } = setup(kind, toolCallId);
    await mastra.startWorkers();

    const started = await streamUntilSuspended(agent, runId);

    const manager = mastra.backgroundTaskManager!;
    let taskId: string | undefined;
    await vi.waitFor(async () => {
      const { tasks } = await manager.listTasks({ toolCallId } as any);
      const suspended = tasks.filter((task: any) => task.status === 'suspended');
      expect(suspended).toHaveLength(1);
      taskId = suspended[0]!.id;
      expect(suspended[0]!.suspendPayload).toMatchObject({ awaiting: 'analyst-approval', topic: 'solana' });
    });
    // Outer checkpoint persists with the tool-call label intact (the chunk
    // precedes persistence, so wait for the row to settle).
    await vi.waitFor(async () => {
      await expect(agent.listSuspendedRuns({ resourceId: 'resource-1' })).resolves.toMatchObject({
        total: 1,
        runs: [{ runId, toolCalls: [{ toolCallId }] }],
      });
    });

    const resumed = await agent.resume(
      runId,
      { approved: true, notes: 'looks good' },
      { toolCallId, ...memoryOptions(), actor: { ...ACTOR } },
    );
    await resumed.output.consumeStream();
    expect(await resumed.output.text).toContain('Done researching');

    await vi.waitFor(async () => {
      expect((await manager.getTask(taskId!))?.status).toBe('completed');
    });
    const completed = await manager.getTask(taskId!);
    expect((completed?.result as any)?.summary).toContain('solana');
    expect((completed?.result as any)?.summary).toContain('looks good');
    // Exactly one task row for the call: resumed, never redispatched.
    expect((await manager.listTasks({ toolCallId } as any)).tasks).toHaveLength(1);
    // Two attempts (suspend + resume) reconciled into one result.
    expect(execute).toHaveBeenCalledTimes(2);
    expect(attempts).toEqual([{ actor: { ...ACTOR } }, { actor: { ...ACTOR } }]);
    await expect(agent.listSuspendedRuns({ resourceId: 'resource-1' })).resolves.toMatchObject({
      total: 0,
      runs: [],
    });

    started.cleanup();
    resumed.cleanup();
  }, 30_000);
});
