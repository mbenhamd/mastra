import { describe, expect, it, vi } from 'vitest';

import type { Agent } from '../agent';
import { AgentThreadStreamRuntime } from '../thread-stream-runtime';
import { LeasePubSub } from './thread-stream-test-utils';

async function waitFor(predicate: () => boolean, timeoutMs = 2_000) {
  const start = Date.now();
  while (!predicate()) {
    if (Date.now() - start > timeoutMs) throw new Error('Timed out waiting for condition');
    await new Promise(resolve => setTimeout(resolve, 0));
  }
}

const settle = () => new Promise(resolve => setTimeout(resolve, 50));

function setup({ withMemory = true } = {}) {
  const pubsub = new LeasePubSub();
  pubsub.retain = true;
  const topics = new Set<string>();
  const registrations: string[] = [];
  const publish = pubsub.publish.bind(pubsub);
  vi.spyOn(pubsub, 'publish').mockImplementation(async (topic: string, event: any) => {
    topics.add(topic);
    await publish(topic, event);
    if (event.type === 'run-registered') registrations.push(event.runId);
  });
  const trim = vi.spyOn(pubsub, 'trimTopic');
  /** Resolves `getMemory`; tests hold it to delay a trim until later publishes land. */
  let memoryGate: Promise<void> = Promise.resolve();
  const agent = {
    id: 'trim-agent',
    getMemory: async () => {
      await memoryGate;
      return withMemory ? {} : undefined;
    },
  } as unknown as Agent<any, any, any, any>;
  const options = { memory: { thread: 'trim-thread', resource: 'trim-user' } } as any;

  const register = (
    runId: string,
    status: 'success' | 'failed' | 'suspended' = 'success',
    runtime = new AgentThreadStreamRuntime(),
  ) => {
    let finish!: () => void;
    const finished = new Promise<void>(resolve => (finish = resolve));
    const output = {
      runId,
      status: 'running',
      fullStream: new ReadableStream({
        start(controller) {
          controller.enqueue({ type: 'start', runId });
          controller.close();
        },
      }),
      _waitUntilFinished: () => finished,
    } as any;
    // Non-strict registerRun resolves at terminal delivery; wait for this
    // registration's `run-registered` publication instead.
    const published = registrations.length;
    const completion = runtime.registerRun(agent, output, options, pubsub);
    void completion?.catch(() => {});
    const registered = waitFor(() => registrations.slice(published).includes(runId));
    return {
      registered,
      complete: () => {
        output.status = status;
        finish();
      },
    };
  };
  const entriesFor = (runId: string) =>
    [...topics].flatMap(topic => pubsub.retainedEvents(topic)).filter(event => event.runId === runId);
  /** Registered and its `start` part is on the topic. */
  const published = async (run: { registered: Promise<unknown> }, runId: string) => {
    await run.registered;
    await waitFor(() => entriesFor(runId).some(event => event.type === 'stream-part'));
  };
  const holdMemory = () => {
    let release!: () => void;
    memoryGate = new Promise<void>(resolve => (release = resolve));
    return release;
  };
  return { trim, register, entriesFor, holdMemory, published };
}

describe('thread topic trim', () => {
  it("deletes a saved run's own entries once it completes", async () => {
    const { trim, register, entriesFor } = setup();
    const run = register('trim-run-1');
    await run.registered;
    expect(entriesFor('trim-run-1').length).toBeGreaterThan(0);
    run.complete();
    await waitFor(() => trim.mock.calls.length === 1);
    expect(entriesFor('trim-run-1')).toEqual([]);
  });

  it('keeps a failed run for live readers, then deletes it', async () => {
    const { trim, register, entriesFor } = setup();
    const run = register('trim-run-failed', 'failed');
    await run.registered;
    vi.useFakeTimers({ toFake: ['setTimeout'] });
    try {
      run.complete();
      await vi.advanceTimersByTimeAsync(1_000);
      expect(trim).not.toHaveBeenCalled();
      expect(entriesFor('trim-run-failed').length).toBeGreaterThan(0);
      await vi.advanceTimersByTimeAsync(30_000);
    } finally {
      vi.useRealTimers();
    }
    await waitFor(() => trim.mock.calls.length === 1);
    expect(entriesFor('trim-run-failed')).toEqual([]);
  });

  it('does not trim when the agent has no storage', async () => {
    const { trim, register } = setup({ withMemory: false });
    const run = register('trim-run-no-memory');
    await run.registered;
    run.complete();
    await settle();
    expect(trim).not.toHaveBeenCalled();
  });

  it('keeps a run registered after another run completes but before its trim runs', async () => {
    const { trim, register, entriesFor, holdMemory, published } = setup();
    const runtime = new AgentThreadStreamRuntime();
    const first = register('trim-run-first', 'success', runtime);
    await first.registered;
    const release = holdMemory();
    first.complete();
    await waitFor(() => entriesFor('trim-run-first').some(event => event.type === 'run-completed'));
    const next = register('trim-run-next', 'success', runtime);
    await published(next, 'trim-run-next');
    const nextEntries = entriesFor('trim-run-next');
    expect(nextEntries.length).toBeGreaterThan(0);
    release();
    await waitFor(() => trim.mock.calls.length === 1);
    expect(entriesFor('trim-run-first')).toEqual([]);
    expect(entriesFor('trim-run-next')).toEqual(nextEntries);
  });

  it('keeps a suspended run when another run on the thread completes, before and after a restart', async () => {
    const { trim, register, entriesFor } = setup();
    const runtime = new AgentThreadStreamRuntime();
    const suspended = register('trim-run-suspended', 'suspended', runtime);
    await suspended.registered;
    suspended.complete();
    await settle();
    const suspendedEntries = entriesFor('trim-run-suspended');
    expect(suspendedEntries.length).toBeGreaterThan(0);

    const done = register('trim-run-done', 'success', runtime);
    await done.registered;
    done.complete();
    await waitFor(() => trim.mock.calls.length === 1);
    expect(entriesFor('trim-run-suspended')).toEqual(suspendedEntries);

    const restarted = register('trim-run-after-restart', 'success', new AgentThreadStreamRuntime());
    await restarted.registered;
    restarted.complete();
    await waitFor(() => trim.mock.calls.length === 2);
    expect(entriesFor('trim-run-after-restart')).toEqual([]);
    expect(entriesFor('trim-run-suspended')).toEqual(suspendedEntries);
  });

  it('deletes the suspended half of a run resumed after a restart', async () => {
    const { trim, register, entriesFor } = setup();
    const suspended = register('trim-run-resumed', 'suspended', new AgentThreadStreamRuntime());
    await suspended.registered;
    suspended.complete();
    await settle();
    expect(entriesFor('trim-run-resumed').length).toBeGreaterThan(0);

    const resumed = register('trim-run-resumed', 'success', new AgentThreadStreamRuntime());
    await resumed.registered;
    resumed.complete();
    await waitFor(() => trim.mock.calls.length === 1);
    expect(entriesFor('trim-run-resumed')).toEqual([]);
  });

  // PF-4402 decision: this fork keeps one exact lease owner per thread. A
  // second runtime whose run cannot acquire the thread lease fails closed
  // (AgentThreadLeaseOwnershipLostError; it never publishes `run-registered`
  // or any part), where upstream let both runtimes publish concurrently. The
  // upstream invariant kept here: nothing another runtime does trims a run
  // that is still held, and the held run is trimmed once it completes.
  it('keeps a run held by another runtime on the same pubsub', async () => {
    const { trim, register, entriesFor, published } = setup();
    const held = register('trim-run-held', 'success', new AgentThreadStreamRuntime());
    await published(held, 'trim-run-held');
    const heldEntries = entriesFor('trim-run-held');

    const other = register('trim-run-other', 'success', new AgentThreadStreamRuntime());
    other.complete();
    await settle();
    expect(entriesFor('trim-run-other')).toEqual([]);
    // Only the refused run's own (empty) entries may be trimmed.
    expect(trim.mock.calls.every(([, options]) => options.runId === 'trim-run-other')).toBe(true);
    expect(entriesFor('trim-run-held')).toEqual(heldEntries);

    const trimsBefore = trim.mock.calls.length;
    held.complete();
    await waitFor(() => trim.mock.calls.some(([, options]) => options.runId === 'trim-run-held'));
    expect(trim.mock.calls.length).toBe(trimsBefore + 1);
    expect(entriesFor('trim-run-held')).toEqual([]);
  });
});
