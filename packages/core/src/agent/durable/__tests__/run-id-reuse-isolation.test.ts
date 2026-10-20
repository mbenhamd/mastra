import type { LanguageModelV2Prompt } from '@ai-sdk/provider-v5';
import { MockLanguageModelV2, convertArrayToReadableStream } from '@internal/ai-sdk-v5/test';
import { describe, expect, it } from 'vitest';
import { z } from 'zod';
import { EventEmitterPubSub } from '../../../events/event-emitter';
import { Mastra } from '../../../mastra';
import { MockMemory } from '../../../memory/mock';
import { InMemoryStore } from '../../../storage';
import { createTool } from '../../../tools';
import { Agent } from '../../agent';
import { ensureRemoteAbortListener } from '../abort-transport';
import { createDurableAgent } from '../create-durable-agent';
import { createEventedAgent } from '../create-evented-agent';
import {
  deleteBoundRunRegistryEntry,
  getRunRegistryEntry,
  globalRunRegistry,
  setRunRegistryEntry,
} from '../run-registry';
import type { RunRegistryEntry } from '../types';

function model(prompts: string[]) {
  return new MockLanguageModelV2({
    doStream: async ({ prompt }: { prompt: LanguageModelV2Prompt }) => {
      prompts.push(JSON.stringify(prompt));
      const parts = prompt.some(m => m.role === 'tool')
        ? [
            { type: 'text-start', id: 't' },
            { type: 'text-delta', id: 't', delta: 'done' },
            { type: 'text-end', id: 't' },
            { type: 'finish', finishReason: 'stop', usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 } },
          ]
        : [
            { type: 'tool-call', toolCallId: `call-${prompts.length}`, toolName: 'lookup', input: '{"q":"a"}' },
            { type: 'finish', finishReason: 'tool-calls', usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 } },
          ];
      return {
        stream: convertArrayToReadableStream([
          { type: 'stream-start', warnings: [] },
          { type: 'response-metadata', id: 'id', modelId: 'mock', timestamp: new Date(0) },
          ...parts,
        ] as any[]),
        rawCall: { rawPrompt: null, rawSettings: {} },
      };
    },
  });
}

type Kind = 'durable' | 'evented';

/** Runs one tenant's agent in its own Mastra app, always with the same runId. */
async function runTenant(name: string, kind: Kind, tripwire: boolean) {
  const storage = new InMemoryStore();
  const prompts: string[] = [];
  const agent = new Agent({
    id: 'support',
    name: 'support',
    instructions: 'help',
    model: model(prompts),
    memory: new MockMemory({ storage }),
    outputProcessors: tripwire
      ? [
          {
            id: 'trip',
            processOutputStream: async ({ part, abort }: any) => {
              if (part.type === 'text-delta') abort('tripwire');
              return part;
            },
          } as any,
        ]
      : [],
    tools: {
      lookup: createTool({
        id: 'lookup',
        description: 'lookup',
        inputSchema: z.object({ q: z.string() }),
        execute: async () => ({ secret: `${name}-tool-secret` }),
      }),
    },
  });
  const runner =
    kind === 'durable'
      ? createDurableAgent({ agent, pubsub: new EventEmitterPubSub() })
      : createEventedAgent({ agent });
  new Mastra({ agents: { support: runner } as never, storage, logger: false });

  const { output, cleanup } = await (runner as any).stream(`${name} private message`, {
    runId: 'shared-run-id',
    memory: { thread: `${name}-thread`, resource: `${name}-user` },
  });
  for await (const _ of output.fullStream);
  await cleanup?.();
  return prompts;
}

describe('runId reuse across Mastra apps', () => {
  // The first run ends on a tripwire while its workflow is still finishing in the
  // background; the second run starts immediately with the same runId.
  it.each<[Kind, Kind]>([
    ['durable', 'durable'],
    ['durable', 'evented'],
    ['evented', 'evented'],
    ['evented', 'durable'],
  ])("%s -> %s: the second app's model never sees the first app's run", async (first, second) => {
    await runTenant('alice', first, true);
    const bobPrompts = await runTenant('bob', second, false);

    expect(bobPrompts.length).toBeGreaterThan(0);
    const all = bobPrompts.join('\n');
    expect(all).not.toMatch(/alice/);
    // Bob's own run is intact: his message and his tool result reach his model.
    expect(all).toContain('bob private message');
    expect(all).toContain('bob-tool-secret');
  });
});

describe('ownerless registry entries', () => {
  const alice = new Mastra({ logger: false });
  const bob = new Mastra({ logger: false });

  it('a rehydrated entry is owned by the instance that published it', () => {
    const runId = 'rehydrated-run';
    const rebuilt = { isPlaceholder: false, tools: {} } as unknown as RunRegistryEntry;
    expect(setRunRegistryEntry(runId, rebuilt, alice)).toBe(true);

    expect(getRunRegistryEntry(runId, alice)).toBe(rebuilt);
    expect(getRunRegistryEntry(runId, bob)).toBeUndefined();
    expect(setRunRegistryEntry(runId, { tools: {} } as unknown as RunRegistryEntry, bob)).toBe(false);
    globalRunRegistry.delete(runId);
  });

  it('an ownerless entry is claimed by its first scoped reader', () => {
    const runId = 'ownerless-run';
    globalRunRegistry.set(runId, { isPlaceholder: true } as RunRegistryEntry);

    expect(getRunRegistryEntry(runId, alice)).toBeDefined();
    expect(getRunRegistryEntry(runId, bob)).toBeUndefined();
    globalRunRegistry.delete(runId);
  });

  // PF-5928: a released execution's still-running tail must not resurrect its run id through a
  // remote-abort placeholder (a later run reusing the id would inherit it). The retirement lasts
  // until that execution settles, however many other executions retire meanwhile.
  it('a late step of a released, still-running execution does not seed a remote-abort placeholder', async () => {
    const runId = 'released-run';
    const pubsub = new EventEmitterPubSub();
    let settle!: () => void;
    const workflowExecution = new Promise<void>(resolve => {
      settle = resolve;
    });
    const entry = { runtimeBindingId: 'released-binding', tools: {}, workflowExecution } as unknown as RunRegistryEntry;
    expect(setRunRegistryEntry(runId, entry, alice)).toBe(true);
    expect(deleteBoundRunRegistryEntry(runId, 'released-binding')).toBe(true);

    // Many other executions released meanwhile do not age this retirement out.
    for (let other = 0; other < 1100; other++) {
      const otherRunId = `other-released-run-${other}`;
      const otherEntry = {
        runtimeBindingId: `other-binding-${other}`,
        tools: {},
        workflowExecution: new Promise(() => {}),
      } as unknown as RunRegistryEntry;
      setRunRegistryEntry(otherRunId, otherEntry, alice);
      deleteBoundRunRegistryEntry(otherRunId, `other-binding-${other}`);
    }

    await ensureRemoteAbortListener(pubsub, runId, 'released-binding', alice);
    expect(globalRunRegistry.has(runId)).toBe(false);

    // A worker that never ran the execution still seeds one.
    await ensureRemoteAbortListener(pubsub, 'fresh-worker-run', 'fresh-worker-binding', alice);
    expect(globalRunRegistry.get('fresh-worker-run')).toMatchObject({
      isPlaceholder: true,
      runtimeBindingId: 'fresh-worker-binding',
    });
    globalRunRegistry.delete('fresh-worker-run');

    // Once the execution settles, the retirement is pruned.
    settle();
    await workflowExecution;
    await Promise.resolve();
    await ensureRemoteAbortListener(pubsub, runId, 'released-binding', alice);
    expect(globalRunRegistry.get(runId)).toMatchObject({ isPlaceholder: true, runtimeBindingId: 'released-binding' });
    globalRunRegistry.delete(runId);
  });

  it("a remote-abort placeholder never hands one instance's controller to another", async () => {
    const runId = 'abort-placeholder-run';
    const pubsub = new EventEmitterPubSub();
    // Fork (PF): the listener is bound to the run's runtime binding, then scoped to the Mastra instance.
    await ensureRemoteAbortListener(pubsub, runId, 'abort-placeholder-binding', alice);
    const aliceController = globalRunRegistry.get(runId)?.abortController;
    expect(aliceController).toBeDefined();

    expect(getRunRegistryEntry(runId, bob)).toBeUndefined();
    await ensureRemoteAbortListener(pubsub, runId, 'abort-placeholder-binding', bob);
    expect(globalRunRegistry.get(runId)?.abortController).toBe(aliceController);
    globalRunRegistry.delete(runId);
  });

  it("observe().abort() from another app never aborts this app's run", async () => {
    const runId = 'observe-abort-run';
    const aliceController = new AbortController();
    // Fork (PF): a live registry entry always carries its run's runtime binding.
    globalRunRegistry.set(runId, {
      mastra: alice,
      runtimeBindingId: 'observe-abort-binding',
      abortController: aliceController,
    } as unknown as RunRegistryEntry);

    const storage = new InMemoryStore();
    const agent = new Agent({ id: 'support', name: 'support', instructions: 'help', model: model([]) });
    const runner = createDurableAgent({ agent, pubsub: new EventEmitterPubSub() });
    new Mastra({ agents: { support: runner } as never, storage, logger: false });

    const observed = await (runner as any).observe(runId, { idleTimeoutMs: 50 });
    await observed.abort().catch(() => {});
    expect(aliceController.signal.aborted).toBe(false);
    expect(globalRunRegistry.get(runId)?.abortController).toBe(aliceController);
    await observed.cleanup?.();
    globalRunRegistry.delete(runId);
  });
});
