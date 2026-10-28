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

// ---------------------------------------------------------------------------------------------
// Model-based check: generated schedules over the three lease-loss routes, counting how often the
// follow-up F executes, compared with fork main's count for the same schedule.

export type FollowUpSchedule = {
  /** Which reservation F is queued behind when it loses the lease. */
  route: 'direct' | 'transfer' | 'queued-idle';
  /** That reservation's lease loss: to a winning run, or without any owner. */
  lease: 'winner' | 'no-winner';
  /** A further queued idle sibling (direct/transfer: behind the wake; queued-idle: behind S). */
  sibling: 'none' | 'queued';
  /** Publish of the signal F is queued behind (only with a winner). */
  wakePublish: 'ok' | 'fail';
  /** Publish of the further sibling's own signal (only with a sibling). */
  siblingPublish: 'ok' | 'fail';
  /** First forward publish of F to the winner (only when that applies). */
  forward: 'ok' | 'reject-before-delivery' | 'reject-after-delivery';
  /** A retry of F (same id and contents). */
  retry: 'none' | 'idle-winner-alive' | 'idle-lease-free' | 'during-local-run' | 'other-instance';
  /** Whether the retry is sent at once after the loss or after the hand-over settled. */
  retryTiming: 'settled' | 'early';
};

const scheduleDimensions = {
  route: ['direct', 'transfer', 'queued-idle'],
  lease: ['winner', 'no-winner'],
  sibling: ['none', 'queued'],
  wakePublish: ['ok', 'fail'],
  siblingPublish: ['ok', 'fail'],
  forward: ['ok', 'reject-before-delivery', 'reject-after-delivery'],
  retry: ['none', 'idle-winner-alive', 'idle-lease-free', 'during-local-run', 'other-instance'],
  retryTiming: ['settled', 'early'],
} as const;

/** Sets dimensions that cannot matter to a canonical value, so equal behaviours share a key. */
function normalizeSchedule(schedule: FollowUpSchedule): FollowUpSchedule {
  const normalized = { ...schedule };
  if (normalized.lease === 'no-winner') normalized.wakePublish = 'ok';
  if (normalized.lease === 'no-winner' || normalized.wakePublish === 'fail') normalized.forward = 'ok';
  if (normalized.sibling === 'none') normalized.siblingPublish = 'ok';
  if (normalized.retry === 'none') normalized.retryTiming = 'settled';
  return normalized;
}

export function followUpScheduleKey(schedule: FollowUpSchedule): string {
  return [
    schedule.route,
    schedule.lease,
    `sibling=${schedule.sibling}`,
    `wakePublish=${schedule.wakePublish}`,
    `siblingPublish=${schedule.siblingPublish}`,
    `forward=${schedule.forward}`,
    `retry=${schedule.retry}`,
    `timing=${schedule.retryTiming}`,
  ].join(' ');
}

/**
 * The disclosed at-least-once cases, where F may run once more than the bound. They are labelled
 * here explicitly, never inferred from an outcome:
 * - F's forward rejects after it was delivered (the queued idle route then also requeues it);
 * - a retry after the winner finished (lease free) or from another instance (no shared caller
 *   cache or admission record), the L17 P3 residual.
 */
export function followUpScheduleAllowsOneExtraRun(schedule: FollowUpSchedule): boolean {
  const forwardRejectsAfterDelivery =
    schedule.lease === 'winner' && schedule.wakePublish === 'ok' && schedule.forward === 'reject-after-delivery';
  const retryWithoutSharedRecord =
    schedule.retry === 'other-instance' ||
    (schedule.lease === 'winner' && (schedule.retry === 'idle-lease-free' || schedule.retry === 'during-local-run'));
  return forwardRejectsAfterDelivery || retryWithoutSharedRecord;
}

/** Deterministic schedules from a fixed seed (mulberry32), normalized. */
export function generateFollowUpSchedules(seed: number, count: number): FollowUpSchedule[] {
  let state = seed >>> 0;
  const next = () => {
    state = (state + 0x6d2b79f5) >>> 0;
    let t = state;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
  const pick = <T>(values: readonly T[]): T => values[Math.floor(next() * values.length)]!;
  return Array.from({ length: count }, () =>
    normalizeSchedule({
      route: pick(scheduleDimensions.route),
      lease: pick(scheduleDimensions.lease),
      sibling: pick(scheduleDimensions.sibling),
      wakePublish: pick(scheduleDimensions.wakePublish),
      siblingPublish: pick(scheduleDimensions.siblingPublish),
      forward: pick(scheduleDimensions.forward),
      retry: pick(scheduleDimensions.retry),
      retryTiming: pick(scheduleDimensions.retryTiming),
    }),
  );
}

const settle = (ms = 30) => new Promise(resolve => setTimeout(resolve, ms));

/**
 * Runs one schedule on the runtime under test and returns how often F executed: once if the
 * winning run received it (the winner admits a signal id once), plus every local execution
 * (a local run started with F, or F drained from a local run's queues). Input still queued at
 * the end is counted by draining it in one more local run.
 */
export async function countFollowUpRuns(schedule: FollowUpSchedule, index: number): Promise<number> {
  const queuedIdle = schedule.route === 'queued-idle';
  const ctx = await startIdleWakeWithFollowUps(`pf6944-prop-${index}`, queuedIdle ? 0 : 1, {
    afterFinishingRun: schedule.route === 'transfer',
    queuedIdleSibling: queuedIdle || schedule.sibling === 'queued',
  });
  const scope = { resourceId: ctx.resourceId, threadId: ctx.threadId };
  const followUpId = queuedIdle ? `${ctx.wakeId}-sibling-follow-up` : ctx.followUpIds[0]!;
  const followUpSignal = { id: followUpId, type: 'user-message', contents: 'f' } as const;
  const furtherSibling =
    queuedIdle && schedule.sibling === 'queued'
      ? ctx.runtime.queueMessage(ctx.agent, `pf6944-prop-${index} later sibling`, scope, ctx.pubsub)
      : undefined;
  void furtherSibling?.accepted.catch(() => {});
  const behindId = queuedIdle ? ctx.sibling!.signal.id : ctx.wakeId;
  const furtherSiblingId = queuedIdle ? furtherSibling?.signal.id : ctx.sibling?.signal.id;

  let forwardFailureUsed = false;
  const realPublish = ctx.pubsub.publish.bind(ctx.pubsub);
  const publishSpy = vi.spyOn(ctx.pubsub, 'publish').mockImplementation(async (topic, event) => {
    const data = event.data;
    if (data?.type === 'signal-enqueued') {
      const id = data.signal?.id;
      if (id === behindId && schedule.wakePublish === 'fail') throw new Error('schedule: wake publish fails');
      if (furtherSiblingId !== undefined && id === furtherSiblingId && schedule.siblingPublish === 'fail') {
        throw new Error('schedule: sibling publish fails');
      }
      if (id === followUpId && data.runId === ctx.winnerRunId && schedule.forward !== 'ok' && !forwardFailureUsed) {
        forwardFailureUsed = true;
        if (schedule.forward === 'reject-after-delivery') await realPublish(topic, event);
        throw new Error(`schedule: forward ${schedule.forward}`);
      }
    }
    await realPublish(topic, event);
  });

  const streamCalls = () =>
    vi.mocked(ctx.agent.stream).mock.calls as unknown as Array<[unknown, { runId?: string } | undefined]>;
  const otherInstance = new AgentThreadStreamRuntime();
  let retryRuntime: AgentThreadStreamRuntime | undefined;
  const sendRetry = async () => {
    if (schedule.retry === 'none') return;
    if (schedule.retry === 'other-instance') {
      retryRuntime = otherInstance;
    } else {
      retryRuntime = ctx.runtime;
      if (schedule.retry !== 'idle-winner-alive') ctx.pubsub.owners.delete(ctx.key);
      if (schedule.retry === 'during-local-run') {
        const calls = streamCalls().length;
        const local = ctx.runtime.sendSignal(
          ctx.agent,
          { id: `${ctx.wakeId}-local`, type: 'user-message', contents: 'local' },
          scope,
          ctx.pubsub,
        );
        void local.accepted.catch(() => {});
        await waitForCondition(() => streamCalls().length > calls, 1000).catch(() => {});
      }
    }
    try {
      const retry = retryRuntime.sendSignal(ctx.agent, followUpSignal, scope, ctx.pubsub);
      void retry.accepted.catch(() => {});
    } catch {
      // A conflicting or rejected retry runs nothing.
    }
  };

  if (queuedIdle) {
    const gate = { release: () => {} };
    ctx.pubsub.acquireLeaseWait = new Promise<void>(resolve => {
      gate.release = resolve;
    });
    ctx.loseLease();
    await ctx.wake.accepted.catch(() => {});
    await waitForCondition(() => ctx.runtime.getActiveThreadRunId(scope, ctx.pubsub) === ctx.sibling!.runId, 1000);
    const followUp = ctx.runtime.sendSignal(ctx.agent, followUpSignal, scope, ctx.pubsub);
    void followUp.accepted.catch(() => {});
    if (schedule.lease === 'no-winner') ctx.pubsub.ownerlessLosses = 1;
    gate.release();
  } else {
    if (schedule.lease === 'no-winner') ctx.pubsub.ownerlessLosses = 1;
    ctx.loseLease();
  }
  if (schedule.retryTiming === 'early') await sendRetry();
  await settle();
  if (schedule.retryTiming === 'settled') await sendRetry();
  await settle();
  publishSpy.mockRestore();
  ctx.finishRun();

  // One more local run takes whatever is still queued, unless a local run already holds it.
  if (!streamCalls().some(([, options]) => options?.runId !== undefined)) {
    ctx.pubsub.owners.delete(ctx.key);
    const flush = ctx.runtime.sendSignal(
      ctx.agent,
      { id: `${ctx.wakeId}-flush`, type: 'user-message', contents: 'flush' },
      scope,
      ctx.pubsub,
    );
    void flush.accepted.catch(() => {});
    await waitForCondition(() => streamCalls().length > 0, 1000).catch(() => {});
  }
  let local = 0;
  for (const [signal, options] of streamCalls()) {
    if ((signal as { id?: string }).id === followUpId) local += 1;
    const runId = options?.runId;
    if (runId === undefined) continue;
    for (const runtime of [ctx.runtime, otherInstance]) {
      for (const scopeName of ['pre-run', 'pending'] as const) {
        local += runtime
          .drainPendingSignals(runId, ctx.pubsub, scopeName)
          .filter(signal => signal.id === followUpId).length;
      }
    }
  }
  const winner = ctx.forwardedTo(ctx.winnerRunId).includes(followUpId) ? 1 : 0;
  const owner = ctx.pubsub.owners.get(ctx.key);
  if (owner) await ctx.pubsub.releaseLease(ctx.key, owner);
  return winner + local;
}
