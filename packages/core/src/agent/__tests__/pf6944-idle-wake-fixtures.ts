// PF-6944 (PF-6993): fixtures for the "input queued behind an idle wake that loses its lease"
// cells in agent-signals.test.ts. Kept in a plain module so the cells themselves are the only
// lines that change in that file (the fork PR validator selects coverage per named case).
import { MockLanguageModelV2 } from '@internal/ai-sdk-v5/test';
import { expect, vi } from 'vitest';

import { PubSub } from '../../events/pubsub';
import type { LeaseProvider } from '../../events/pubsub';
import type { EventCallback } from '../../events/types';
import { MockMemory } from '../../memory/mock';
import { Agent } from '../agent';
import { AgentThreadStreamRuntime } from '../thread-stream-runtime';

function decodeLeaseOwnerRunId(owner: string | undefined): string | undefined {
  if (owner === undefined || !owner.startsWith('mastra-thread-owner:')) return owner;
  try {
    const decoded = JSON.parse(owner.slice('mastra-thread-owner:'.length));
    return Array.isArray(decoded) && typeof decoded[0] === 'string' ? decoded[0] : owner;
  } catch {
    return owner;
  }
}

function createBlockingFirstTextStreamModel(firstResponseText: string, laterResponseText: string) {
  let releaseFirst!: () => void;
  const firstFinished = new Promise<void>(resolve => {
    releaseFirst = resolve;
  });
  let streamCount = 0;
  const model = new MockLanguageModelV2({
    doStream: async () => {
      streamCount += 1;
      const currentStreamCount = streamCount;
      const responseText = currentStreamCount === 1 ? firstResponseText : laterResponseText;
      return {
        rawCall: { rawPrompt: null, rawSettings: {} },
        warnings: [],
        stream: new ReadableStream({
          async start(controller) {
            controller.enqueue({ type: 'stream-start', warnings: [] });
            controller.enqueue({
              type: 'response-metadata',
              id: `blocking-stream-${currentStreamCount}`,
              modelId: 'mock-model-id',
              timestamp: new Date(0),
            });
            controller.enqueue({ type: 'text-start', id: 'text-1' });
            controller.enqueue({ type: 'text-delta', id: 'text-1', delta: responseText });
            controller.enqueue({ type: 'text-end', id: 'text-1' });
            if (currentStreamCount === 1) {
              await firstFinished;
            }
            controller.enqueue({
              type: 'finish',
              finishReason: 'stop',
              usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
            });
            controller.close();
          },
        }),
      };
    },
  });

  return { model, releaseFirst, getStreamCount: () => streamCount };
}

function nextTick() {
  return new Promise(resolve => setTimeout(resolve, 0));
}

async function waitForCondition(predicate: () => boolean, timeoutMs = 500) {
  const startedAt = Date.now();
  while (!predicate()) {
    if (Date.now() - startedAt > timeoutMs) {
      throw new Error('Timed out waiting for condition');
    }
    await nextTick();
  }
}

class RetainedAsyncCallbackPubSub extends PubSub {
  #subscribers = new Map<string, Set<EventCallback>>();

  subscriberCount(topic: string) {
    return this.#subscribers.get(topic)?.size ?? 0;
  }
  #history = new Map<string, any[]>();
  #pending = new Set<Promise<void>>();
  #index = 0;
  /** Callback rejections, which a real backend turns into a nack and redelivery. */
  subscriptionFailures: unknown[] = [];

  async publish(topic: string, event: any): Promise<void> {
    const envelope = { ...event, id: `retained-${this.#index}`, createdAt: new Date(), index: this.#index++ };
    const history = this.#history.get(topic) ?? [];
    history.push(envelope);
    this.#history.set(topic, history);
    const subscribers = [...(this.#subscribers.get(topic) ?? [])];
    const pending = new Promise<void>(resolve => {
      setTimeout(() => {
        for (const subscriber of subscribers) {
          void Promise.resolve(subscriber(envelope)).catch(error => this.subscriptionFailures.push(error));
        }
        resolve();
      }, 0);
    });
    this.#pending.add(pending);
    pending.finally(() => this.#pending.delete(pending));
  }

  async subscribe(topic: string, cb: EventCallback): Promise<void> {
    const subscribers = this.#subscribers.get(topic) ?? new Set<EventCallback>();
    subscribers.add(cb);
    this.#subscribers.set(topic, subscribers);
    for (const event of this.#history.get(topic) ?? []) {
      void Promise.resolve(cb(event)).catch(error => this.subscriptionFailures.push(error));
    }
  }

  async unsubscribe(topic: string, cb: EventCallback): Promise<void> {
    this.#subscribers.get(topic)?.delete(cb);
  }

  async flush(): Promise<void> {
    await Promise.all([...this.#pending]);
  }
}

export class ControlledLeasePubSub extends RetainedAsyncCallbackPubSub implements LeaseProvider {
  owners = new Map<string, string>();
  publishedData: any[] = [];
  ownerReadDelayMs = 0;
  ownerReadFailures = 0;
  acquireLeaseWait: Promise<void> | undefined;
  onAcquireLease: (() => void) | undefined;
  transferLeaseWait: Promise<void> | undefined;
  onTransferLease: (() => void) | undefined;
  denyLeaseAcquisition = false;
  /** The next N lease acquisitions report a loss without an owner (after their wait). */
  ownerlessLosses = 0;
  denyLeaseTransfer = false;
  rejectPublishedTypes = new Set<string>();
  unsubscribeCount = 0;

  override async publish(topic: string, event: any): Promise<void> {
    this.publishedData.push(event.data);
    await super.publish(topic, event);
    if (this.rejectPublishedTypes.has(event.data?.type)) {
      throw new Error(`publish rejected after delivery: ${event.data.type}`);
    }
  }

  async acquireLease(key: string, owner: string): Promise<{ acquired: boolean; owner?: string }> {
    this.onAcquireLease?.();
    await this.acquireLeaseWait;
    const current = this.owners.get(key);
    if (this.ownerlessLosses > 0) {
      this.ownerlessLosses -= 1;
      return { acquired: false };
    }
    if (this.denyLeaseAcquisition) return { acquired: false, owner: current ?? 'competing-run' };
    if (current && current !== owner) return { acquired: false, owner: current };
    this.owners.set(key, owner);
    return { acquired: true, owner };
  }

  async getLeaseOwner(key: string): Promise<string | undefined> {
    if (this.ownerReadDelayMs) await new Promise(resolve => setTimeout(resolve, this.ownerReadDelayMs));
    if (this.ownerReadFailures > 0) {
      this.ownerReadFailures -= 1;
      throw new Error('transient owner read failure');
    }
    return this.owners.get(key);
  }

  async releaseLease(key: string, owner: string): Promise<void> {
    if (this.owners.get(key) === owner) this.owners.delete(key);
  }

  async renewLease(key: string, owner: string): Promise<boolean> {
    return this.owners.get(key) === owner;
  }

  async transferLease(key: string, fromOwner: string, toOwner: string): Promise<boolean> {
    this.onTransferLease?.();
    await this.transferLeaseWait;
    if (this.denyLeaseTransfer) return false;
    if (this.owners.get(key) !== fromOwner) return false;
    this.owners.set(key, toOwner);
    return true;
  }

  override async unsubscribe(topic: string, cb: EventCallback): Promise<void> {
    this.unsubscribeCount += 1;
    await super.unsubscribe(topic, cb);
  }
}

export const encodedOwner = (runId: string, source: string) =>
  `mastra-thread-owner:${JSON.stringify([runId, source, '33333333-3333-4333-8333-333333333333'])}`;

export async function startIdleWakeWithFollowUps(
  prefix: string,
  followUpCount: number,
  options: { afterFinishingRun?: boolean; queuedIdleSibling?: boolean } = {},
) {
  const pubsub = new ControlledLeasePubSub();
  const runtime = new AgentThreadStreamRuntime();
  const resourceId = `${prefix}-resource`;
  const threadId = `${prefix}-thread`;
  const key = `${resourceId}\u0000${threadId}`;
  const agent = { id: `${prefix}-agent` } as Agent<any, any, any, any>;
  agent.stream = vi.fn(async (_signal, streamOptions) => ({ runId: streamOptions.runId })) as any;
  const winnerRunId = `${prefix}-winner`;
  // A real encoded owner token, as a remote winner holds it; never the bare run id.
  const winnerToken = encodedOwner(winnerRunId, `${prefix}-winner-source`);

  let finishRun = () => {};
  if (options.afterFinishingRun) {
    // A local run that has produced its final part but not yet released its lease: the
    // next idle wake takes the lease-transfer route (#acquireOrTransferThreadLease).
    const finishing = new Promise<void>(resolve => {
      finishRun = resolve;
    });
    const finishingOutput = {
      runId: `${prefix}-finishing`,
      status: 'success',
      fullStream: new ReadableStream({
        start(controller) {
          controller.close();
        },
      }),
      _waitUntilFinished: () => finishing,
    } as any;
    const completion = runtime.registerRun(
      agent,
      finishingOutput,
      { memory: { resource: resourceId, thread: threadId } } as any,
      pubsub,
    );
    void completion?.catch(() => {});
    await waitForCondition(
      () => runtime.getRunOutput(finishingOutput.runId, pubsub) !== undefined && pubsub.owners.has(key),
    );
    // Its lease lapsed and the winner took the key, so the transfer fails and the wake
    // falls back to acquisition, which loses.
    pubsub.owners.set(key, winnerToken);
  }

  let releaseAcquire!: () => void;
  pubsub.acquireLeaseWait = new Promise<void>(resolve => {
    releaseAcquire = resolve;
  });
  let markAcquireStarted!: () => void;
  const acquireStarted = new Promise<void>(resolve => {
    markAcquireStarted = resolve;
  });
  pubsub.onAcquireLease = markAcquireStarted;

  const wakeId = `${prefix}-wake`;
  const wake = runtime.sendSignal(
    agent,
    { id: wakeId, type: 'user-message', contents: 'wake' },
    { resourceId, threadId },
    pubsub,
  );
  void wake.accepted.catch(() => {});
  await acquireStarted;
  expect(runtime.getActiveThreadRunId({ resourceId, threadId }, pubsub)).toBe(wake.runId);

  const followUpIds = Array.from({ length: followUpCount }, (_, index) => `${prefix}-follow-up-${index + 1}`);
  for (const id of followUpIds) {
    const followUp = runtime.sendSignal(
      agent,
      { id, type: 'user-message', contents: id },
      { resourceId, threadId },
      pubsub,
    );
    expect(followUp.runId).toBe(wake.runId);
    // Unchanged by this fix: the acknowledgement is still immediate.
    await expect(followUp.accepted).resolves.toMatchObject({ action: 'deliver', runId: wake.runId });
  }
  const sibling = options.queuedIdleSibling
    ? runtime.queueMessage(agent, `${prefix} sibling`, { resourceId, threadId }, pubsub)
    : undefined;
  void sibling?.accepted.catch(() => {});

  const forwardedTo = (runId: string) =>
    pubsub.publishedData
      .filter(data => data?.type === 'signal-enqueued' && data.runId === runId)
      .map(data => data.signal?.id);
  const loseLease = () => {
    pubsub.owners.set(key, winnerToken);
    releaseAcquire();
  };
  // Starts the next local run on the thread and returns the ids its first model request
  // would take from the pre-run queue.
  const nextLocalRunPreRunIds = async () => {
    pubsub.ownerReadDelayMs = 0;
    pubsub.owners.delete(key);
    const streamCalls = vi.mocked(agent.stream).mock.calls.length;
    const next = runtime.sendSignal(
      agent,
      { id: `${prefix}-next`, type: 'user-message', contents: 'next' },
      { resourceId, threadId },
      pubsub,
    );
    void next.accepted.catch(() => {});
    await waitForCondition(() => vi.mocked(agent.stream).mock.calls.length === streamCalls + 1);
    const ids = runtime.drainPendingSignals(next.runId, pubsub, 'pre-run').map(signal => signal.id);
    const owner = pubsub.owners.get(key);
    if (owner) await pubsub.releaseLease(key, owner);
    return ids;
  };
  return {
    pubsub,
    runtime,
    agent,
    resourceId,
    threadId,
    key,
    wake,
    wakeId,
    followUpIds,
    sibling,
    winnerRunId,
    winnerToken,
    forwardedTo,
    loseLease,
    /** Lets the held lease acquisition proceed without changing the owner. */
    releaseAcquire: () => releaseAcquire(),
    nextLocalRunPreRunIds,
    finishRun,
  };
}

export type IdleWakeContext = Awaited<ReturnType<typeof startIdleWakeWithFollowUps>>;

// Loses the wake's lease, then holds the queued idle sibling's own lease acquisition and
// queues one follow-up behind the sibling's reservation.
export async function queueFollowUpBehindSiblingDrain(ctx: IdleWakeContext) {
  const sibling = ctx.sibling!;
  let releaseSiblingAcquire!: () => void;
  let failSiblingAcquire!: (error: Error) => void;
  ctx.pubsub.acquireLeaseWait = new Promise<void>((resolve, reject) => {
    releaseSiblingAcquire = resolve;
    failSiblingAcquire = reject;
  });
  let markSiblingAcquireStarted!: () => void;
  const siblingAcquireStarted = new Promise<void>(resolve => {
    markSiblingAcquireStarted = resolve;
  });
  ctx.pubsub.onAcquireLease = markSiblingAcquireStarted;
  ctx.loseLease();
  await expect(ctx.wake.accepted).resolves.toMatchObject({ action: 'deliver', runId: ctx.winnerRunId });
  await siblingAcquireStarted;
  const scope = { resourceId: ctx.resourceId, threadId: ctx.threadId };
  expect(ctx.runtime.getActiveThreadRunId(scope, ctx.pubsub)).toBe(sibling.runId);
  const followUpSignal = { id: `${ctx.wakeId}-sibling-follow-up`, type: 'user-message', contents: 'f' } as const;
  const followUp = ctx.runtime.sendSignal(ctx.agent, followUpSignal, scope, ctx.pubsub);
  expect(followUp.runId).toBe(sibling.runId);
  await expect(followUp.accepted).resolves.toMatchObject({ action: 'deliver', runId: sibling.runId });
  return { sibling, scope, followUpSignal, releaseSiblingAcquire, failSiblingAcquire };
}

// How often a follow-up reached the winner plus how often the next local run takes it.
export async function deliveriesOf(ctx: IdleWakeContext, id: string) {
  const forwarded = ctx.forwardedTo(ctx.winnerRunId).filter(forwardedId => forwardedId === id).length;
  const kept = (await ctx.nextLocalRunPreRunIds()).filter(keptId => keptId === id).length;
  return forwarded + kept;
}

/**
 * The real-winner cell: a winner Agent in another runtime holds the lease, the sender's
 * idle wake loses to it with a follow-up queued behind it. With `retry`, the sender and
 * another instance retry the follow-up before the winner runs.
 */
export async function runRealWinnerFollowUp(retry: boolean) {
  const scope = { resourceId: `pf6944-min-real-${retry}-user`, threadId: `pf6944-min-real-${retry}-thread` };
  const key = `${scope.resourceId}\u0000${scope.threadId}`;
  const pubsub = new ControlledLeasePubSub();
  const sender = new AgentThreadStreamRuntime();
  const memory = new MockMemory();
  const blocking = createBlockingFirstTextStreamModel('winner answer', 'winner answer');
  const agent = new Agent({
    id: `pf6944-min-real-${retry}-agent`,
    name: 'PF-6944 winner',
    instructions: 'Test',
    model: blocking.model,
    memory,
    pubsub,
  });
  const winnerSubscription = await agent.subscribeToThread(scope);
  const winnerRunId = `pf6944-min-real-${retry}-run`;
  const wakeText = 'pf6944 wake input';
  const followUpText = 'pf6944 follow-up input';
  const storedTexts = async () => {
    const { messages } = await memory.recall(scope);
    return messages.flatMap(message =>
      message.content.parts.flatMap(part => (part.type === 'text' ? [part.text] : [])),
    );
  };
  const countStored = async (text: string) => (await storedTexts()).filter(stored => stored === text).length;
  try {
    // The winner runs in another runtime and holds the lease under its encoded owner token.
    const winning = agent.stream('winner input', {
      runId: winnerRunId,
      memory: { resource: scope.resourceId, thread: scope.threadId },
    });
    await vi.waitFor(() => expect(blocking.getStreamCount()).toBe(1));
    await vi.waitFor(() =>
      expect(pubsub.publishedData.some(data => data.type === 'run-registered' && data.runId === winnerRunId)).toBe(
        true,
      ),
    );
    expect(decodeLeaseOwnerRunId(pubsub.owners.get(key))).toBe(winnerRunId);

    // The sender has not observed the winner: its wake reserves the thread, and the
    // follow-up (sent while that reservation is admitting) is queued behind it.
    const wake = sender.sendSignal(
      agent,
      { id: 'pf6944-min-real-wake', type: 'user-message', contents: wakeText },
      scope,
      pubsub,
    );
    const followUpSignal = {
      id: 'pf6944-min-real-follow-up',
      type: 'user-message',
      contents: followUpText,
    } as const;
    const followUp = sender.sendSignal(agent, followUpSignal, scope, pubsub);
    expect(followUp.runId).toBe(wake.runId);
    await expect(followUp.accepted).resolves.toMatchObject({ action: 'deliver', runId: wake.runId });
    await expect(wake.accepted).resolves.toMatchObject({ action: 'deliver', runId: winnerRunId });
    if (retry) {
      await vi.waitFor(() =>
        expect(
          pubsub.publishedData.some(
            data =>
              data.type === 'signal-enqueued' && data.runId === winnerRunId && data.signal.id === followUpSignal.id,
          ),
        ).toBe(true),
      );
      // The sender still holds the follow-up's admission record: its retry is a duplicate.
      const senderRetry = sender.sendSignal(agent, followUpSignal, scope, pubsub);
      await expect(senderRetry.accepted).resolves.toMatchObject({ action: 'deliver' });
      // Another instance has no record: its wake loses the lease and is forwarded to the
      // winner, which admits a signal id once.
      const otherInstanceRetry = new AgentThreadStreamRuntime().sendSignal(agent, followUpSignal, scope, pubsub);
      await expect(otherInstanceRetry.accepted).resolves.toMatchObject({ action: 'deliver', runId: winnerRunId });
      // The winner received the follow-up's id twice: the forward and the retry.
      expect(
        pubsub.publishedData.filter(
          data => data.type === 'signal-enqueued' && data.runId === winnerRunId && data.signal.id === followUpSignal.id,
        ),
      ).toHaveLength(2);
    }
    blocking.releaseFirst();
    const output = await winning;
    await output.text;
    // The winner runs the follow-up exactly once ...
    await vi.waitFor(async () => expect(await countStored(followUpText)).toBe(1));
    await new Promise(resolve => setTimeout(resolve, 50));
    expect(await countStored(followUpText)).toBe(1);
    expect(await countStored(wakeText)).toBe(1);
    // ... after the wake: in its stored history and in the model input that carries it.
    const texts = await storedTexts();
    expect(texts.indexOf(wakeText)).toBeLessThan(texts.indexOf(followUpText));
    const prompt = blocking.model.doStreamCalls
      .map(call => JSON.stringify(call?.prompt))
      .find(serialized => serialized.includes(followUpText));
    expect(prompt).toBeDefined();
    expect(prompt!.indexOf(wakeText)).toBeGreaterThan(-1);
    expect(prompt!.indexOf(wakeText)).toBeLessThan(prompt!.indexOf(followUpText));
    await vi.waitFor(() => expect(pubsub.owners.get(key)).toBeUndefined());
  } finally {
    blocking.releaseFirst();
    winnerSubscription.unsubscribe();
  }
}

/**
 * Loses the wake's lease; the queued idle sibling's lease acquisition then reports a loss
 * without an owner, after one follow-up was queued behind the sibling's reservation, so the
 * sibling requeues it for the next local run.
 */
export async function requeueFollowUpBehindOwnerlessSibling(ctx: IdleWakeContext) {
  const sibling = ctx.sibling!;
  const scope = { resourceId: ctx.resourceId, threadId: ctx.threadId };
  let releaseSiblingAcquire!: () => void;
  const siblingAcquireHeld = new Promise<void>(resolve => {
    releaseSiblingAcquire = resolve;
  });
  const acquireSpy = vi.spyOn(ctx.pubsub, 'acquireLease').mockImplementationOnce(async () => {
    await siblingAcquireHeld;
    return { acquired: false };
  });
  ctx.loseLease();
  await expect(ctx.wake.accepted).resolves.toMatchObject({ action: 'deliver', runId: ctx.winnerRunId });
  await waitForCondition(() => acquireSpy.mock.calls.length === 1);
  const followUpSignal = { id: `${ctx.wakeId}-sibling-follow-up`, type: 'user-message', contents: 'f' } as const;
  const followUp = ctx.runtime.sendSignal(ctx.agent, followUpSignal, scope, ctx.pubsub);
  expect(followUp.runId).toBe(sibling.runId);
  releaseSiblingAcquire();
  await new Promise(resolve => setTimeout(resolve, 50));
  return { sibling, scope, followUpSignal };
}

/**
 * This instance observes a remote run R of the same agent holding the thread's lease (as it
 * would from R's `run-registered`), while the context's own reservation may still be acquiring.
 * `finish()` publishes R's completion and frees the lease.
 */
export async function observeRemoteRun(ctx: IdleWakeContext, remoteRunId: string) {
  const scope = { resourceId: ctx.resourceId, threadId: ctx.threadId };
  const topic = `agent.thread-stream.${encodeURIComponent(ctx.key)}`;
  const remoteOwner = encodedOwner(remoteRunId, 'pf6944-remote-source');
  const subscription = await ctx.runtime.subscribeToThread(ctx.agent, scope, ctx.pubsub);
  ctx.pubsub.owners.set(ctx.key, remoteOwner);
  await ctx.pubsub.publish(topic, {
    type: 'run-registered',
    runId: remoteRunId,
    data: {
      type: 'run-registered',
      runId: remoteRunId,
      streamId: `${remoteRunId}-stream`,
      streamSeq: 1,
      leaseOwner: remoteOwner,
      agentId: ctx.agent.id,
    },
  });
  await ctx.pubsub.flush();
  await new Promise(resolve => setTimeout(resolve, 20));
  return {
    unsubscribe: () => subscription.unsubscribe(),
    async finish() {
      if (ctx.pubsub.owners.get(ctx.key) === remoteOwner) ctx.pubsub.owners.delete(ctx.key);
      await ctx.pubsub.publish(topic, {
        type: 'run-completed',
        runId: remoteRunId,
        data: { type: 'run-completed', runId: remoteRunId, streamId: `${remoteRunId}-stream`, leaseOwner: remoteOwner },
      });
      await ctx.pubsub.flush();
      await new Promise(resolve => setTimeout(resolve, 20));
    },
  };
}

/** Holds every publish of the given signal ids to `runId`; `rejectAll()` rejects them, oldest first, before delivery. */
export function holdPublishesTo(ctx: IdleWakeContext, runId: string, signalIds: readonly string[]) {
  const pending: Array<{ id: string; reject: () => void }> = [];
  const realPublish = ctx.pubsub.publish.bind(ctx.pubsub);
  const spy = vi.spyOn(ctx.pubsub, 'publish').mockImplementation(async (topic, event) => {
    const id = event.data?.signal?.id;
    if (event.data?.type === 'signal-enqueued' && event.data?.runId === runId && signalIds.includes(id)) {
      await new Promise<void>((_resolve, reject) => {
        pending.push({ id, reject: () => reject(new Error(`injected publish failure before delivery: ${id}`)) });
      });
    }
    await realPublish(topic, event);
  });
  return {
    heldIds: () => pending.map(entry => entry.id),
    rejectAll() {
      for (const entry of pending.splice(0)) entry.reject();
    },
    restore: () => spy.mockRestore(),
  };
}
