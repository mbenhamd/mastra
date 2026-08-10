import type { LanguageModelV2 } from '@ai-sdk/provider-v5';
import { MockLanguageModelV2 } from '@internal/ai-sdk-v5/test';
import { describe, expect, it, vi } from 'vitest';
import { InMemoryServerCache } from '../../../cache/inmemory';
import { CachingPubSub } from '../../../events/caching-pubsub';
import { EventEmitterPubSub } from '../../../events/event-emitter';
import { Mastra } from '../../../mastra';
import { InMemoryStore } from '../../../storage/mock';
import { Agent } from '../../agent';
import {
  ensureRemoteAbortListener as ensureRemoteAbortListenerForBinding,
  publishAbortRequest as publishAbortRequestForBinding,
} from '../abort-transport';
import { AGENT_CONTROL_TOPIC, AGENT_STREAM_TOPIC } from '../constants';
import { createDurableAgent } from '../create-durable-agent';
import {
  getGlobalRunRegistryEntry,
  globalRunRegistry,
  pinGlobalRunRegistryEntry,
  unpinGlobalRunRegistryEntry,
} from '../run-registry';
import type { RunRegistryEntry } from '../types';

const TEST_RUNTIME_BINDING_ID = 'cross-process-abort-test-binding';
const ensureRemoteAbortListener = (
  pubsub: EventEmitterPubSub | CachingPubSub,
  runId: string,
  runtimeBindingId = TEST_RUNTIME_BINDING_ID,
) => ensureRemoteAbortListenerForBinding(pubsub, runId, runtimeBindingId);
const publishAbortRequest = (
  pubsub: EventEmitterPubSub | CachingPubSub,
  runId: string,
  runtimeBindingId = TEST_RUNTIME_BINDING_ID,
) => publishAbortRequestForBinding(pubsub, runId, runtimeBindingId);

/**
 * A model that opens a stream and then never produces anything, so the only
 * way the run ends is an abort. Without one of these a fast mock finishes
 * before the abort lands and the test passes for the wrong reason.
 */
function createHangingModel() {
  return new MockLanguageModelV2({
    doStream: async ({ abortSignal }: { abortSignal?: AbortSignal }) => ({
      stream: new ReadableStream({
        start(controller) {
          controller.enqueue({ type: 'stream-start', warnings: [] });
          controller.enqueue({ type: 'response-metadata', id: 'id-0', modelId: 'mock', timestamp: new Date(0) });
          controller.enqueue({ type: 'text-start', id: 'text-1' });
          abortSignal?.addEventListener(
            'abort',
            () => {
              const error = new Error('Aborted');
              error.name = 'AbortError';
              controller.error(error);
            },
            { once: true },
          );
        },
      }),
      rawCall: { rawPrompt: null, rawSettings: {} },
    }),
  });
}

describe('DurableAgent cross-process abort', () => {
  it('aborts a run whose executing process never held the caller AbortController', async () => {
    // A worker that rehydrated a run from storage has no controller in its
    // registry entry — that is precisely why a caller's local abort() could
    // never reach it. Model that state directly.
    const runId = 'worker-side-run';
    const pubsub = new EventEmitterPubSub();
    const entry = { isPlaceholder: true } as RunRegistryEntry;
    globalRunRegistry.set(runId, entry);

    try {
      await ensureRemoteAbortListener(pubsub, runId);

      // The listener gives the worker a signal of its own, which is what every
      // downstream model/tool call in the step already reads.
      expect(entry.abortSignal).toBeDefined();
      expect(entry.abortSignal!.aborted).toBe(false);

      const aborted = new Promise<void>(resolve => {
        entry.abortSignal!.addEventListener('abort', () => resolve(), { once: true });
      });

      await publishAbortRequest(pubsub, runId);
      await aborted;

      expect(entry.abortSignal!.aborted).toBe(true);
    } finally {
      globalRunRegistry.delete(runId);
      await pubsub.close();
    }
  });

  it('stays abortable when the step starts before anything registered the run', async () => {
    // A worker's first step can run before `resolveRuntimeDependencies` has
    // seeded the registry. Skipping the listener there would leave the run
    // permanently deaf to remote aborts.
    const runId = 'unregistered-run';
    const pubsub = new EventEmitterPubSub();

    try {
      await ensureRemoteAbortListener(pubsub, runId);

      const entry = globalRunRegistry.get(runId);
      expect(entry).toBeDefined();
      // Still a placeholder, so the real runtime rebuild is not skipped.
      expect(entry!.isPlaceholder).toBe(true);
      expect(entry!.abortSignal!.aborted).toBe(false);

      const aborted = new Promise<void>(resolve => {
        entry!.abortSignal!.addEventListener('abort', () => resolve(), { once: true });
      });

      await publishAbortRequest(pubsub, runId);
      await aborted;

      expect(entry!.abortSignal!.aborted).toBe(true);
    } finally {
      globalRunRegistry.delete(runId);
      await pubsub.close();
    }
  });

  it('installs the listener on a pinned-only active entry without seeding a shadow placeholder', async () => {
    const runId = 'pinned-only-abort-listener';
    const pubsub = new EventEmitterPubSub();
    const entry = {
      runtimeBindingId: TEST_RUNTIME_BINDING_ID,
      tools: {},
      model: {} as any,
    } as RunRegistryEntry;
    globalRunRegistry.set(runId, entry);
    expect(pinGlobalRunRegistryEntry(runId)).toBe(entry);
    globalRunRegistry.delete(runId);

    try {
      await ensureRemoteAbortListener(pubsub, runId);
      expect(getGlobalRunRegistryEntry(runId)).toBe(entry);
      expect(globalRunRegistry.get(runId)).toBeUndefined();
      expect(entry.remoteAbortListenerInstalled).toBe(true);
    } finally {
      entry.cleanup?.();
      unpinGlobalRunRegistryEntry(runId, TEST_RUNTIME_BINDING_ID);
      globalRunRegistry.delete(runId);
      await pubsub.close();
    }
  });

  it('replays an abort published before a remote worker installs its listener', async () => {
    const runId = 'queued-worker-run';
    const innerPubsub = new EventEmitterPubSub();
    const pubsub = new CachingPubSub(innerPubsub, new InMemoryServerCache());

    try {
      // The caller can abort while the workflow is still queued. The control
      // event must remain in the cache until the worker reaches its first step.
      await publishAbortRequest(pubsub, runId);
      await ensureRemoteAbortListener(pubsub, runId);

      const entry = globalRunRegistry.get(runId);
      expect(entry?.abortSignal?.aborted).toBe(true);
    } finally {
      globalRunRegistry.delete(runId);
      await innerPubsub.close();
    }
  });

  it('rejects a stale runtime binding before replay can abort a reused run ID', async () => {
    const runId = 'reused-run-id';
    const innerPubsub = new EventEmitterPubSub();
    const pubsub = new CachingPubSub(innerPubsub, new InMemoryServerCache());
    const abortController = new AbortController();
    globalRunRegistry.set(runId, {
      runtimeBindingId: 'new-binding',
      tools: {},
      model: {} as any,
      abortController,
      abortSignal: abortController.signal,
    } as RunRegistryEntry);

    try {
      await publishAbortRequest(pubsub, runId, 'stale-binding');

      await expect(ensureRemoteAbortListener(pubsub, runId, 'stale-binding')).rejects.toThrow(
        /no longer matches its registered runtime dependencies/,
      );
      expect(abortController.signal.aborted).toBe(false);
    } finally {
      globalRunRegistry.delete(runId);
      await pubsub.clearTopic(AGENT_CONTROL_TOPIC(runId, 'stale-binding'));
      await innerPubsub.close();
    }
  });

  it('does not replay an old binding tombstone into a legitimate reused run ID', async () => {
    const runId = 'legitimately-reused-run-id';
    const oldBinding = 'old-binding';
    const newBinding = 'new-binding';
    const innerPubsub = new EventEmitterPubSub();
    const pubsub = new CachingPubSub(innerPubsub, new InMemoryServerCache());
    const abortController = new AbortController();
    globalRunRegistry.set(runId, {
      runtimeBindingId: newBinding,
      tools: {},
      model: {} as any,
      abortController,
      abortSignal: abortController.signal,
    } as RunRegistryEntry);

    try {
      await publishAbortRequest(pubsub, runId, oldBinding);
      await ensureRemoteAbortListener(pubsub, runId, newBinding);
      expect(abortController.signal.aborted).toBe(false);

      await publishAbortRequest(pubsub, runId, newBinding);
      expect(abortController.signal.aborted).toBe(true);
    } finally {
      globalRunRegistry.delete(runId);
      await pubsub.clearTopic(AGENT_CONTROL_TOPIC(runId, oldBinding));
      await pubsub.clearTopic(AGENT_CONTROL_TOPIC(runId, newBinding));
      await innerPubsub.close();
    }
  });

  it('only installs one listener per run no matter how many steps start', async () => {
    const runId = 'idempotent-listener-run';
    const pubsub = new EventEmitterPubSub();
    globalRunRegistry.set(runId, { isPlaceholder: true } as RunRegistryEntry);

    try {
      await ensureRemoteAbortListener(pubsub, runId);
      const firstSignal = globalRunRegistry.get(runId)!.abortSignal;

      // A run runs many steps in the same process; each one calls this.
      await ensureRemoteAbortListener(pubsub, runId);
      await ensureRemoteAbortListener(pubsub, runId);

      // Same controller reused — a replacement would strand the signal already
      // handed to an in-flight model call.
      expect(globalRunRegistry.get(runId)!.abortSignal).toBe(firstSignal);
    } finally {
      globalRunRegistry.delete(runId);
      await pubsub.close();
    }
  });

  it('stays retryable when the subscription fails', async () => {
    const runId = 'failed-subscribe-run';
    const pubsub = new EventEmitterPubSub();
    let failNext = true;
    const originalSubscribe = pubsub.subscribe.bind(pubsub);
    pubsub.subscribe = (async (...args: Parameters<typeof originalSubscribe>) => {
      if (failNext) {
        failNext = false;
        throw new Error('pubsub down');
      }
      return originalSubscribe(...args);
    }) as typeof pubsub.subscribe;
    globalRunRegistry.set(runId, { isPlaceholder: true } as RunRegistryEntry);

    try {
      // A transient transport failure must not leave the run permanently deaf:
      // the next step gets to try again.
      await expect(ensureRemoteAbortListener(pubsub, runId)).rejects.toThrow('pubsub down');
      expect(globalRunRegistry.get(runId)!.remoteAbortListenerInstalled).toBe(false);

      await ensureRemoteAbortListener(pubsub, runId);
      const entry = globalRunRegistry.get(runId)!;
      const aborted = new Promise<void>(resolve => {
        entry.abortSignal!.addEventListener('abort', () => resolve(), { once: true });
      });
      await publishAbortRequest(pubsub, runId);
      await aborted;
      expect(entry.abortSignal!.aborted).toBe(true);
    } finally {
      globalRunRegistry.delete(runId);
      await pubsub.close();
    }
  });

  it('contains and reports asynchronous abort-listener unsubscribe failures during cleanup', async () => {
    const runId = 'failed-unsubscribe-run';
    const unsubscribeError = new Error('pubsub unsubscribe failed');
    const pubsub = {
      subscribeWithReplay: vi.fn().mockResolvedValue(undefined),
      unsubscribe: vi.fn().mockRejectedValue(unsubscribeError),
    };
    const consoleError = vi.spyOn(console, 'error').mockImplementation(() => {});
    globalRunRegistry.set(runId, {
      runtimeBindingId: TEST_RUNTIME_BINDING_ID,
      tools: {},
      model: {} as any,
    } as RunRegistryEntry);

    try {
      await ensureRemoteAbortListener(pubsub as any, runId);
      expect(() => globalRunRegistry.get(runId)?.cleanup?.()).not.toThrow();
      await vi.waitFor(() =>
        expect(consoleError).toHaveBeenCalledWith(
          `[DurableAgent] Failed to unsubscribe the cross-process abort listener for ${runId}:`,
          unsubscribeError,
        ),
      );
    } finally {
      const entry = globalRunRegistry.get(runId);
      if (entry) entry.cleanup = undefined;
      globalRunRegistry.delete(runId);
      consoleError.mockRestore();
    }
  });

  it('rejects an awaitable abort when remote dispatch cannot be confirmed', async () => {
    const pubsub = new EventEmitterPubSub();
    const agent = new Agent({
      id: 'abort-dispatch-failure-agent',
      name: 'Abort Dispatch Failure Agent',
      instructions: 'test',
      model: createHangingModel() as unknown as LanguageModelV2,
    });
    const durableAgent = createDurableAgent({ agent, pubsub });
    new Mastra({ agents: { 'abort-dispatch-failure-agent': durableAgent as any }, logger: false });
    const result = await durableAgent.stream('Go');
    const dispatchError = new Error('abort transport unavailable');
    const runtimeBindingId = globalRunRegistry.get(result.runId)?.runtimeBindingId;
    expect(runtimeBindingId).toEqual(expect.any(String));
    const originalPublish = pubsub.publish.bind(pubsub);
    pubsub.publish = vi.fn(async (topic, event) => {
      if (topic === AGENT_CONTROL_TOPIC(result.runId, runtimeBindingId!)) throw dispatchError;
      return originalPublish(topic, event);
    }) as typeof pubsub.publish;

    try {
      await new Promise(resolve => setTimeout(resolve, 50));
      await expect(result.abort('stop')).rejects.toBe(dispatchError);
      await result.output.consumeStream();
    } finally {
      result.cleanup();
      pubsub.publish = originalPublish;
      await pubsub.close();
    }
  });

  it('stops an in-flight run from an abort request it did not raise locally, and still terminates the stream', async () => {
    const pubsub = new EventEmitterPubSub();
    const storage = new InMemoryStore();
    const agent = new Agent({
      id: 'cross-process-abort-agent',
      name: 'cross-process-abort-agent',
      instructions: 'test',
      model: createHangingModel() as unknown as LanguageModelV2,
    });
    const durableAgent = createDurableAgent({ agent, pubsub });
    new Mastra({ agents: { 'cross-process-abort-agent': durableAgent as any }, storage, logger: false });

    const { output, runId, cleanup } = await durableAgent.stream('Go');

    const terminalEvents: string[] = [];
    await pubsub.subscribe(AGENT_STREAM_TOPIC(runId), (event: any) => {
      if (event?.type === 'finish' || event?.type === 'abort' || event?.type === 'error') {
        terminalEvents.push(event.type);
      }
    });

    // Let the run get as far as an in-flight model call.
    await new Promise(resolve => setTimeout(resolve, 50));

    // Deliberately NOT result.abort(): that flips a controller in this process
    // and proves nothing about a caller living somewhere else. Publishing the
    // request straight to pubsub is what a different pod's abort() looks like
    // from here — the executing process must pick it up on its own.
    const runtimeBindingId = globalRunRegistry.get(runId)?.runtimeBindingId;
    expect(runtimeBindingId).toEqual(expect.any(String));
    await publishAbortRequest(pubsub, runId, runtimeBindingId!);

    // The run must actually end, and it must end with a terminal event. A hard
    // workflow cancel would stop the work but skip the event, leaving every
    // stream consumer waiting forever.
    await output.consumeStream();

    expect(terminalEvents).toContain('finish');

    cleanup();
    await pubsub.close();
  }, 20000);
});
