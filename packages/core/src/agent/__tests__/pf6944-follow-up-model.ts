// PF-6944 (PF-6993): model-based check for follow-ups queued behind an idle wake that loses its
// lease. Each schedule is a sequence of lease, publish, retry and run-lifecycle events over the
// three lease-loss routes. The model counts how often the follow-up F executes and compares it
// with fork main's count for the same schedule (a committed table generated from a run).
import { vi } from 'vitest';

import type { Agent } from '../agent';
import { AgentThreadStreamRuntime } from '../thread-stream-runtime';
import { encodedOwner, startIdleWakeWithFollowUps } from './pf6944-idle-wake-fixtures';
import type { ControlledLeasePubSub } from './pf6944-idle-wake-fixtures';

type PublishMode = 'ok' | 'fail' | 'held-ok' | 'held-fail';

export type FollowUpSchedule = {
  /** Which reservation F is queued behind when it loses the lease. */
  route: 'direct' | 'transfer' | 'queued-idle';
  /** That reservation's lease loss: to a winning run, or without any owner. */
  lease: 'winner' | 'no-winner';
  /** A further queued idle sibling (direct/transfer: behind the wake; queued-idle: behind S). */
  sibling: 'none' | 'queued';
  /** Publish of the signal F is queued behind. `held-*` stays pending until the retry was sent. */
  wakePublish: PublishMode;
  /** Publish of the further sibling's own signal. `held-*` as above. */
  siblingPublish: PublishMode;
  /** First forward publish of F to the winner. */
  forward: 'ok' | 'reject-before-delivery' | 'reject-after-delivery' | 'held-reject-before-delivery';
  /** A retry of F with its original payload. */
  retry:
    | 'none'
    | 'idle-winner-alive'
    | 'idle-lease-free'
    | 'during-local-run'
    | 'other-instance'
    | 'remote-observed'
    | 'winner-observed'
    | 'remote-while-acquiring';
  /** Whether the retry is sent at once after the loss or after the hand-over settled. */
  retryTiming: 'settled' | 'early';
  /** F's admission record is lost before the retry: TTL expiry, or eviction by 1001 newer admissions. */
  recordLoss: 'none' | 'ttl-expiry' | 'eviction';
  /** Before the retry, a run on another thread of this runtime reuses the run id F was queued behind. */
  runIdReuse: 'none' | 'other-thread';
  /**
   * Only for `remote-while-acquiring`: the reservation F is queued behind is still acquiring the
   * lease when this instance observes a remote run and the retry of F is routed to it (addressed
   * to the thread or to that run), the retry's publish settles as given, and the reservation then
   * wins the lease.
   */
  retryPublish?: 'ok' | 'reject-before-delivery' | 'reject-after-delivery';
  retryTarget?: 'thread' | 'remote-run';
};

export type FollowUpCount = {
  /** Executions of F by the end of the retry phase (all local runs completed). */
  settled: number;
  /** Executions including one later local run on the thread, which takes anything still queued. */
  total: number;
};

const dimensions = {
  route: ['direct', 'transfer', 'queued-idle'],
  lease: ['winner', 'no-winner'],
  sibling: ['none', 'queued'],
  wakePublish: ['ok', 'fail'],
  siblingPublish: ['ok', 'fail'],
  forward: ['ok', 'reject-before-delivery', 'reject-after-delivery'],
  retry: ['none', 'idle-winner-alive', 'idle-lease-free', 'during-local-run', 'other-instance'],
  retryTiming: ['settled', 'early'],
  recordLoss: ['none'],
  runIdReuse: ['none'],
} as const satisfies { [K in keyof FollowUpSchedule]: readonly FollowUpSchedule[K][] };

type Dimensions = {
  [K in Exclude<keyof FollowUpSchedule, 'retryPublish' | 'retryTarget'>]: readonly FollowUpSchedule[K][];
} & {
  retryPublish?: readonly NonNullable<FollowUpSchedule['retryPublish']>[];
  retryTarget?: readonly NonNullable<FollowUpSchedule['retryTarget']>[];
};

const anyRetry = [
  'idle-winner-alive',
  'idle-lease-free',
  'during-local-run',
  'other-instance',
  'remote-observed',
] as const;

/**
 * The enumerated schedule families. `base` is the full original space (405 normalized
 * schedules); each extension crosses one new dimension with the original ones.
 */
export const followUpScheduleFamilies: Record<string, Dimensions> = {
  base: dimensions,
  'pending wake publish': {
    ...dimensions,
    wakePublish: ['held-ok', 'held-fail'],
    retry: anyRetry,
    retryTiming: ['settled'],
  },
  'pending sibling publish': {
    ...dimensions,
    sibling: ['queued'],
    siblingPublish: ['held-ok', 'held-fail'],
    retry: anyRetry,
    retryTiming: ['settled'],
  },
  'retry to an observed remote run': { ...dimensions, retry: ['remote-observed'] },
  // This instance observes the winning run itself, still live, and the retry is routed to it.
  'remote retry while the local reservation acquires': {
    ...dimensions,
    route: ['direct', 'queued-idle'],
    lease: ['winner'],
    sibling: ['none'],
    wakePublish: ['ok'],
    siblingPublish: ['ok'],
    forward: ['ok'],
    retry: ['remote-while-acquiring'],
    retryTiming: ['settled'],
    retryPublish: ['ok', 'reject-before-delivery', 'reject-after-delivery'],
    retryTarget: ['thread', 'remote-run'],
  },
  'retry to the observed live winner': {
    ...dimensions,
    forward: ['ok', 'reject-before-delivery', 'reject-after-delivery', 'held-reject-before-delivery'],
    retry: ['winner-observed'],
  },
  'record lost by TTL': {
    ...dimensions,
    retry: ['idle-winner-alive', 'idle-lease-free', 'during-local-run', 'other-instance'],
    retryTiming: ['settled'],
    recordLoss: ['ttl-expiry'],
  },
  'record lost by eviction': {
    ...dimensions,
    siblingPublish: ['ok', 'fail', 'held-fail'],
    retry: ['during-local-run'],
    retryTiming: ['settled'],
    recordLoss: ['eviction'],
  },
  'run id reused on another thread': {
    ...dimensions,
    retry: ['idle-winner-alive', 'idle-lease-free'],
    retryTiming: ['settled'],
    runIdReuse: ['other-thread'],
  },
};

/** Sets dimensions that cannot matter to a canonical value, so equal behaviours share a key. */
function normalizeSchedule(schedule: FollowUpSchedule): FollowUpSchedule | undefined {
  const s = { ...schedule };
  if (s.retry !== 'remote-while-acquiring') {
    delete s.retryPublish;
    delete s.retryTarget;
  }
  if (s.lease === 'no-winner') s.wakePublish = 'ok';
  if (s.lease === 'no-winner' || s.wakePublish === 'fail' || s.wakePublish === 'held-fail') s.forward = 'ok';
  if (s.sibling === 'none') s.siblingPublish = 'ok';
  if (s.retry === 'none') {
    if (s.forward === 'held-reject-before-delivery') s.forward = 'reject-before-delivery';
    s.retryTiming = 'settled';
    s.recordLoss = 'none';
    s.runIdReuse = 'none';
    s.wakePublish = s.wakePublish === 'held-ok' ? 'ok' : s.wakePublish === 'held-fail' ? 'fail' : s.wakePublish;
    s.siblingPublish =
      s.siblingPublish === 'held-ok' ? 'ok' : s.siblingPublish === 'held-fail' ? 'fail' : s.siblingPublish;
  }
  if (s.recordLoss === 'eviction' && s.retry !== 'during-local-run') s.recordLoss = 'none';
  if (s.runIdReuse === 'other-thread' && s.retry !== 'idle-winner-alive' && s.retry !== 'idle-lease-free') {
    s.runIdReuse = 'none';
  }
  const held = (mode: PublishMode) => mode === 'held-ok' || mode === 'held-fail';
  const heldCount =
    (held(s.wakePublish) ? 1 : 0) +
    (held(s.siblingPublish) ? 1 : 0) +
    (s.forward === 'held-reject-before-delivery' ? 1 : 0);
  if (heldCount > 1) return undefined;
  if (heldCount === 1) s.retryTiming = 'settled';
  return s;
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
    `recordLoss=${schedule.recordLoss}`,
    `runIdReuse=${schedule.runIdReuse}`,
    ...(schedule.retryPublish === undefined ? [] : [`retryPublish=${schedule.retryPublish}`]),
    ...(schedule.retryTarget === undefined ? [] : [`retryTarget=${schedule.retryTarget}`]),
  ].join(' ');
}

/** Every normalized schedule of one family, in a fixed order. */
export function enumerateFollowUpSchedules(family: Dimensions): FollowUpSchedule[] {
  const keys = Object.keys(family) as (keyof FollowUpSchedule)[];
  const schedules = new Map<string, FollowUpSchedule>();
  const visit = (index: number, partial: Partial<FollowUpSchedule>) => {
    if (index === keys.length) {
      const normalized = normalizeSchedule(partial as FollowUpSchedule);
      if (normalized) schedules.set(followUpScheduleKey(normalized), normalized);
      return;
    }
    const dimension = keys[index]!;
    for (const value of family[dimension] ?? []) visit(index + 1, { ...partial, [dimension]: value });
  };
  visit(0, {});
  return [...schedules.values()];
}

/**
 * How many extra runs of F (beyond the bound) a schedule's disclosed at-least-once cases allow.
 * The cases are labelled here explicitly from the schedule, never inferred from an outcome:
 * 1. the winning run may hold F while it is also handed on locally or again; this needs a forward
 *    of F that can reach the winner (through the wake's own forward, which is not rejected before
 *    delivery, or through a further sibling that took the requeued F):
 *    - F's forward rejects after it was delivered (the queued idle route then also requeues it);
 *    - a retry after the winner finished (lease free, during a local run, or routed to another
 *      remote run), or from another instance (which also counts when F is kept locally): no
 *      shared record (L17 P3);
 *    - a remote retry while the local reservation still acquires, whose publish rejects after it
 *      delivered (the local copy is restored);
 * 2. F was kept locally (requeued) and its admission record is lost (TTL or eviction) before the
 *    retry; this needs F actually requeued and not dropped by a further sibling: requeued without
 *    a winner, or by the queued idle route's forward chain after a rejected forward, unless a
 *    further sibling takes it and drops it on its own failed publish; the local copy may already
 *    have run when the record is lost. This is the TTL class fork main has for every retried
 *    signal.
 * Each class allows one extra run; a schedule in both allows two (the literal sum, never more).
 */
export function followUpScheduleExtraRunsAllowed(schedule: FollowUpSchedule): number {
  // A remote retry while the local reservation still acquires: a retry publish that rejects after
  // it delivered leaves F at the remote run while its local copy is restored (class 1).
  if (schedule.retry === 'remote-while-acquiring') return schedule.retryPublish === 'reject-after-delivery' ? 1 : 0;
  const delivered = (mode: PublishMode) => mode === 'ok' || mode === 'held-ok';
  const rejectedBeforeDelivery =
    schedule.forward === 'reject-before-delivery' || schedule.forward === 'held-reject-before-delivery';
  // A further sibling that takes the requeued F (after a loss without a winner, or after the queued
  // idle forward rejected before delivery) forwards it to the winner when its own publish succeeds.
  const siblingForwardsRequeuedF =
    schedule.sibling === 'queued' &&
    delivered(schedule.siblingPublish) &&
    (schedule.lease === 'no-winner' ||
      (schedule.route === 'queued-idle' && delivered(schedule.wakePublish) && rejectedBeforeDelivery));
  const winnerMayHoldF =
    (schedule.lease === 'winner' && delivered(schedule.wakePublish) && !rejectedBeforeDelivery) ||
    siblingForwardsRequeuedF;
  const forwardRejectsAfterDelivery =
    schedule.lease === 'winner' && delivered(schedule.wakePublish) && schedule.forward === 'reject-after-delivery';
  // Kept locally: requeued without a winner, or requeued by the queued idle route's detached forward
  // chain after a rejected forward; a further sibling whose own publish fails drops it if it takes
  // it, which it does for a requeue without a winner or after a forward rejected before delivery
  // (a forward rejected after delivery requeues after the sibling has taken the queue).
  const furtherSiblingDrops = schedule.sibling === 'queued' && !delivered(schedule.siblingPublish);
  const keptLocally =
    (schedule.lease === 'no-winner' && !furtherSiblingDrops) ||
    (schedule.route === 'queued-idle' &&
      schedule.lease === 'winner' &&
      delivered(schedule.wakePublish) &&
      (schedule.forward === 'reject-after-delivery' || (rejectedBeforeDelivery && !furtherSiblingDrops)));
  // A retry without a shared record (another instance, or after the winner finished) needs the
  // original F to be able to reach the winner, or (another instance) to be kept locally.
  const retryWithoutSharedRecord =
    (schedule.retry === 'other-instance' && (winnerMayHoldF || keptLocally)) ||
    (winnerMayHoldF &&
      (schedule.retry === 'idle-lease-free' ||
        schedule.retry === 'during-local-run' ||
        schedule.retry === 'remote-observed'));
  const keptLocallyAndRecordLost = keptLocally && schedule.recordLoss !== 'none';
  return (forwardRejectsAfterDelivery || retryWithoutSharedRecord ? 1 : 0) + (keptLocallyAndRecordLost ? 1 : 0);
}

const tick = () => new Promise(resolve => setTimeout(resolve, 0));
const settle = async (ticks = 6) => {
  for (let index = 0; index < ticks; index += 1) await tick();
};
async function waitUntil(predicate: () => boolean, ticks = 200) {
  for (let index = 0; index < ticks && !predicate(); index += 1) await tick();
  return predicate();
}

/** Emulated agent runs: each run registers, folds its pre-run input, and completes on demand. */
function emulateRuns(
  runtime: AgentThreadStreamRuntime,
  agent: Agent<any, any, any, any>,
  pubsub: ControlledLeasePubSub,
  countExecution: (signalId: string | undefined) => void,
) {
  const open = new Map<string, { complete: () => void; key: string }>();
  agent.stream = vi.fn(async (signal: { id?: string }, streamOptions: any) => {
    const runId: string = streamOptions.runId;
    countExecution(signal?.id);
    let resolveFinished!: () => void;
    const finished = new Promise<void>(resolve => {
      resolveFinished = resolve;
    });
    let closeStream = () => {};
    const output = {
      runId,
      status: 'running',
      // The run stays live (its stream open) until the model completes it.
      fullStream: new ReadableStream({
        start(controller) {
          closeStream = () => controller.close();
        },
      }),
      _waitUntilFinished: () => finished,
    } as any;
    const finish = () => {
      output.status = 'success';
      closeStream();
      resolveFinished();
    };
    const completion = runtime.registerRun(agent, output, streamOptions, pubsub);
    void completion?.catch(() => {});
    // The first model request folds the run's pre-run input.
    for (const queued of runtime.drainPendingSignals(runId, pubsub, 'pre-run')) countExecution(queued.id);
    open.set(runId, {
      key: runId,
      complete: () => {
        // The last step takes the in-loop follow-ups before the run finishes.
        for (const queued of runtime.drainPendingSignals(runId, pubsub, 'pending')) countExecution(queued.id);
        finish();
      },
    });
    return output;
  }) as any;
  return {
    /** A model step of every open run: it takes its pre-run and pending input. */
    step() {
      for (const runId of open.keys()) {
        for (const scope of ['pre-run', 'pending'] as const) {
          for (const queued of runtime.drainPendingSignals(runId, pubsub, scope)) countExecution(queued.id);
        }
      }
    },
    hasOpenRun: () => open.size > 0,
    openRunIds: () => [...open.keys()],
    async completeAll() {
      for (let round = 0; round < 20 && open.size > 0; round += 1) {
        for (const [runId, run] of [...open]) {
          open.delete(runId);
          run.complete();
        }
        await settle(10);
      }
    },
  };
}

/**
 * The `remote-while-acquiring` schedules: the reservation L that F is queued behind (the idle wake,
 * or the queued idle sibling) is still acquiring the lease when this instance observes a remote run
 * R holding it, and a retry of F is routed to R; its publish settles as the schedule says. R then
 * finishes and L wins the lease and runs. Counts F's executions like `countFollowUpRuns`.
 */
async function countRemoteRetryWhileAcquiring(schedule: FollowUpSchedule, index: number): Promise<FollowUpCount> {
  const queuedIdle = schedule.route === 'queued-idle';
  const ctx = await startIdleWakeWithFollowUps(`pf6944-model-${index}`, queuedIdle ? 0 : 1, {
    queuedIdleSibling: queuedIdle,
  });
  const scope = { resourceId: ctx.resourceId, threadId: ctx.threadId };
  const followUpId = queuedIdle ? `${ctx.wakeId}-sibling-follow-up` : ctx.followUpIds[0]!;
  const followUpSignal = { id: followUpId, type: 'user-message', contents: followUpId } as const;
  let executions = 0;
  const runs = emulateRuns(ctx.runtime, ctx.agent, ctx.pubsub, signalId => {
    if (signalId === followUpId) executions += 1;
  });
  const topic = `agent.thread-stream.${encodeURIComponent(ctx.key)}`;
  const remoteRunId = `${ctx.wakeId}-remote-run`;
  const remoteOwner = encodedOwner(remoteRunId, 'pf6944-remote-source');
  const live = { remote: false };
  let remoteAdmitted = false;
  let retryFailureUsed = false;
  const retryErrors: unknown[] = [];
  const realPublish = ctx.pubsub.publish.bind(ctx.pubsub);

  // L: the idle wake (its acquisition is held by the fixture), or the queued idle sibling.
  let releaseReservation = () => ctx.releaseAcquire();
  const reservationRunId = queuedIdle ? ctx.sibling!.runId : ctx.wake.runId;
  if (queuedIdle) {
    const gate = { release: () => {} };
    ctx.pubsub.acquireLeaseWait = new Promise<void>(resolve => {
      gate.release = resolve;
    });
    ctx.loseLease();
    await ctx.wake.accepted.catch(() => {});
    await waitUntil(() => ctx.runtime.getActiveThreadRunId(scope, ctx.pubsub) === reservationRunId);
    const followUp = ctx.runtime.sendSignal(ctx.agent, followUpSignal, scope, ctx.pubsub);
    void followUp.accepted.catch(() => {});
    releaseReservation = () => gate.release();
  }

  const publishSpy = vi.spyOn(ctx.pubsub, 'publish').mockImplementation(async (topicName, event) => {
    const data = event.data;
    const toRemote = data?.type === 'signal-enqueued' && data.signal?.id === followUpId && data.runId === remoteRunId;
    if (toRemote && schedule.retryPublish !== 'ok' && !retryFailureUsed) {
      retryFailureUsed = true;
      if (schedule.retryPublish === 'reject-after-delivery') {
        await realPublish(topicName, event);
        if (live.remote) remoteAdmitted = true;
      }
      throw new Error(`schedule: retry publish ${schedule.retryPublish}`);
    }
    await realPublish(topicName, event);
    if (toRemote && live.remote) remoteAdmitted = true;
  });

  // This instance observes R, which holds the lease, while L still acquires.
  const subscription = await ctx.runtime.subscribeToThread(ctx.agent, scope, ctx.pubsub);
  ctx.pubsub.owners.set(ctx.key, remoteOwner);
  live.remote = true;
  await realPublish(topic, {
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
  await settle();
  const retry = ctx.runtime.sendSignal(
    ctx.agent,
    followUpSignal,
    schedule.retryTarget === 'remote-run' ? { ...scope, runId: remoteRunId } : scope,
    ctx.pubsub,
  );
  void retry.accepted.catch(error => retryErrors.push(error));
  await settle(10);

  // R finishes; L then wins the lease and runs.
  live.remote = false;
  if (ctx.pubsub.owners.get(ctx.key) === remoteOwner) ctx.pubsub.owners.delete(ctx.key);
  await realPublish(topic, {
    type: 'run-completed',
    runId: remoteRunId,
    data: { type: 'run-completed', runId: remoteRunId, streamId: `${remoteRunId}-stream`, leaseOwner: remoteOwner },
  });
  await ctx.pubsub.flush();
  await settle(10);
  if (queuedIdle) ctx.pubsub.owners.delete(ctx.key);
  releaseReservation();
  await waitUntil(() => runs.openRunIds().includes(reservationRunId));
  await settle(10);
  publishSpy.mockRestore();
  await runs.completeAll();
  const settled = executions + (remoteAdmitted ? 1 : 0);

  // A later local run on the thread takes whatever is still queued.
  const owner = ctx.pubsub.owners.get(ctx.key);
  if (owner) await ctx.pubsub.releaseLease(ctx.key, owner);
  const later = ctx.runtime.sendSignal(
    ctx.agent,
    { id: `${ctx.wakeId}-later`, type: 'user-message', contents: 'later' },
    scope,
    ctx.pubsub,
  );
  void later.accepted.catch(() => {});
  await settle(10);
  await runs.completeAll();
  const total = executions + (remoteAdmitted ? 1 : 0);
  subscription.unsubscribe();
  const unexpected = retryErrors.filter(error => !(error instanceof Error && error.message.startsWith('schedule: ')));
  if (unexpected.length > 0) {
    throw new Error(`${followUpScheduleKey(schedule)}: unexpected retry failure: ${unexpected.map(String).join('; ')}`);
  }
  return { settled, total };
}

/**
 * Runs one schedule on the runtime under test and counts F's executions: once if a live winning
 * (or observed remote) run received it, since an owner admits a signal id once, plus every local
 * execution (a run started with F, or F folded from a run's pre-run or pending queue).
 */
export async function countFollowUpRuns(schedule: FollowUpSchedule, index: number): Promise<FollowUpCount> {
  if (schedule.retry === 'remote-while-acquiring') return countRemoteRetryWhileAcquiring(schedule, index);
  const queuedIdle = schedule.route === 'queued-idle';
  const ctx = await startIdleWakeWithFollowUps(`pf6944-model-${index}`, queuedIdle ? 0 : 1, {
    afterFinishingRun: schedule.route === 'transfer',
    queuedIdleSibling: queuedIdle || schedule.sibling === 'queued',
  });
  const scope = { resourceId: ctx.resourceId, threadId: ctx.threadId };
  const followUpId = queuedIdle ? `${ctx.wakeId}-sibling-follow-up` : ctx.followUpIds[0]!;
  // Same id and payload as the original send (the fixture sends follow-ups with their id as contents).
  const followUpSignal = { id: followUpId, type: 'user-message', contents: followUpId } as const;

  let executions = 0;
  const countExecution = (signalId: string | undefined) => {
    if (signalId === followUpId) executions += 1;
  };
  const runs = emulateRuns(ctx.runtime, ctx.agent, ctx.pubsub, countExecution);
  const otherInstance = new AgentThreadStreamRuntime();
  const otherAgent = { id: ctx.agent.id } as Agent<any, any, any, any>;
  const otherRuns = emulateRuns(otherInstance, otherAgent, ctx.pubsub, countExecution);

  const furtherSibling =
    queuedIdle && schedule.sibling === 'queued'
      ? ctx.runtime.queueMessage(ctx.agent, `pf6944-model-${index} later sibling`, scope, ctx.pubsub)
      : undefined;
  void furtherSibling?.accepted.catch(() => {});
  const behindId = queuedIdle ? ctx.sibling!.signal.id : ctx.wakeId;
  const behindRunId = queuedIdle ? ctx.sibling!.runId : ctx.wake.runId;
  const furtherSiblingId = queuedIdle ? furtherSibling?.signal.id : ctx.sibling?.signal.id;

  // Owners: the lease winner, and a remote run this instance may observe later. Each admits F
  // once while it is live; a delivery to a finished run is not admitted.
  const remoteRunId = `${ctx.wakeId}-remote-run`;
  const live = { winner: true, remote: false };
  const admitted = { winner: false, remote: false };

  let releaseHold = () => {};
  const hold = new Promise<void>(resolve => {
    releaseHold = resolve;
  });
  let holdStarted = false;
  let forwardFailureUsed = false;
  // The original F object, as first published (its send to the reservation it is queued behind).
  const originalFollowUp = () =>
    ctx.pubsub.publishedData.find(data => data?.type === 'signal-enqueued' && data.signal?.id === followUpId)?.signal;
  const realPublish = ctx.pubsub.publish.bind(ctx.pubsub);
  const applyMode = async (mode: PublishMode, what: string) => {
    if (mode === 'held-ok' || mode === 'held-fail') {
      holdStarted = true;
      await hold;
    }
    if (mode === 'fail' || mode === 'held-fail') throw new Error(`schedule: ${what} publish fails`);
  };
  const publishSpy = vi.spyOn(ctx.pubsub, 'publish').mockImplementation(async (topic, event) => {
    const data = event.data;
    if (data?.type === 'signal-enqueued') {
      const id = data.signal?.id;
      if (id === behindId) await applyMode(schedule.wakePublish, 'wake');
      if (furtherSiblingId !== undefined && id === furtherSiblingId)
        await applyMode(schedule.siblingPublish, 'sibling');
      // The injected forward failure applies to the original forward of F, identified by
      // provenance: the published signal is the original F object (a retry sends a new one).
      if (
        id === followUpId &&
        data.runId === ctx.winnerRunId &&
        schedule.forward !== 'ok' &&
        !forwardFailureUsed &&
        data.signal === originalFollowUp()
      ) {
        forwardFailureUsed = true;
        if (schedule.forward === 'held-reject-before-delivery') {
          holdStarted = true;
          await hold;
        }
        if (schedule.forward === 'reject-after-delivery') {
          await realPublish(topic, event);
          if (live.winner) admitted.winner = true;
        }
        throw new Error(`schedule: forward ${schedule.forward}`);
      }
    }
    await realPublish(topic, event);
    if (data?.type === 'signal-enqueued' && data.signal?.id === followUpId) {
      if (data.runId === ctx.winnerRunId && live.winner) admitted.winner = true;
      if (data.runId === remoteRunId && live.remote) admitted.remote = true;
    }
  });

  // Retries: rejected acknowledgements are recorded and must be ones this schedule injected.
  const retryErrors: unknown[] = [];
  const sendRetryOn = (runtime: AgentThreadStreamRuntime, agent: Agent<any, any, any, any>, runId?: string) => {
    const retry = runtime.sendSignal(
      agent,
      followUpSignal,
      runId === undefined ? scope : { ...scope, runId },
      ctx.pubsub,
    );
    void retry.accepted.catch(error => retryErrors.push(error));
  };
  const finishWinner = () => {
    live.winner = false;
    if (ctx.pubsub.owners.get(ctx.key) === ctx.winnerToken) ctx.pubsub.owners.delete(ctx.key);
  };
  let releaseReuse: (() => void) | undefined;
  let localRunId: string | undefined;
  let winnerObserved = false;
  const realNow = Date.now.bind(Date);
  let dateSpy: ReturnType<typeof vi.spyOn> | undefined;
  let subscription: { unsubscribe(): void } | undefined;
  const sendRetry = async () => {
    if (schedule.retry === 'none') return;
    if (schedule.recordLoss === 'ttl-expiry') {
      dateSpy = vi.spyOn(Date, 'now').mockImplementation(() => realNow() + 6 * 60 * 1000);
    }
    if (schedule.runIdReuse === 'other-thread') {
      // Another agent's run reserves the run id F was queued behind (that reservation has
      // retired) on another thread of this runtime, as Agent.stream does, and stays live.
      releaseReuse = ctx.runtime.reserveRun(
        { runId: behindRunId, memory: { resource: `${ctx.resourceId}-other`, thread: `${ctx.threadId}-other` } } as any,
        ctx.pubsub,
        `${ctx.agent.id}-other`,
      );
    }
    switch (schedule.retry) {
      case 'idle-winner-alive':
        sendRetryOn(ctx.runtime, ctx.agent);
        break;
      case 'idle-lease-free':
        finishWinner();
        sendRetryOn(ctx.runtime, ctx.agent);
        break;
      case 'during-local-run': {
        finishWinner();
        const local = ctx.runtime.sendSignal(
          ctx.agent,
          { id: `${ctx.wakeId}-local`, type: 'user-message', contents: 'local' },
          scope,
          ctx.pubsub,
        );
        void local.accepted.catch(error => retryErrors.push(error));
        localRunId = local.runId;
        await waitUntil(() => runs.openRunIds().includes(local.runId));
        if (schedule.recordLoss === 'eviction') {
          for (let evict = 0; evict < 1001; evict += 1) {
            ctx.runtime.sendSignal(
              ctx.agent,
              { id: `${ctx.wakeId}-evict-${evict}`, type: 'user-message', contents: 'e' },
              scope,
              ctx.pubsub,
            );
          }
        }
        sendRetryOn(ctx.runtime, ctx.agent);
        break;
      }
      case 'other-instance':
        sendRetryOn(otherInstance, otherAgent);
        break;
      case 'winner-observed': {
        subscription = await ctx.runtime.subscribeToThread(ctx.agent, scope, ctx.pubsub);
        winnerObserved = true;
        await realPublish(`agent.thread-stream.${encodeURIComponent(ctx.key)}`, {
          type: 'run-registered',
          runId: ctx.winnerRunId,
          data: {
            type: 'run-registered',
            runId: ctx.winnerRunId,
            streamId: `${ctx.winnerRunId}-stream`,
            streamSeq: 1,
            leaseOwner: ctx.winnerToken,
            agentId: ctx.agent.id,
          },
        });
        await ctx.pubsub.flush();
        await settle();
        sendRetryOn(ctx.runtime, ctx.agent);
        break;
      }
      case 'remote-observed': {
        finishWinner();
        subscription = await ctx.runtime.subscribeToThread(ctx.agent, scope, ctx.pubsub);
        const remoteOwner = encodedOwner(remoteRunId, 'pf6944-remote-source');
        ctx.pubsub.owners.set(ctx.key, remoteOwner);
        live.remote = true;
        await realPublish(`agent.thread-stream.${encodeURIComponent(ctx.key)}`, {
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
        await settle();
        sendRetryOn(ctx.runtime, ctx.agent);
        break;
      }
    }
  };

  // The loss.
  if (queuedIdle) {
    const gate = { release: () => {} };
    ctx.pubsub.acquireLeaseWait = new Promise<void>(resolve => {
      gate.release = resolve;
    });
    ctx.loseLease();
    await ctx.wake.accepted.catch(() => {});
    await waitUntil(() => ctx.runtime.getActiveThreadRunId(scope, ctx.pubsub) === ctx.sibling!.runId);
    const followUp = ctx.runtime.sendSignal(ctx.agent, followUpSignal, scope, ctx.pubsub);
    void followUp.accepted.catch(() => {});
    if (schedule.lease === 'no-winner') ctx.pubsub.ownerlessLosses = 1;
    gate.release();
  } else {
    if (schedule.lease === 'no-winner') ctx.pubsub.ownerlessLosses = 1;
    ctx.loseLease();
  }
  const held =
    schedule.wakePublish.startsWith('held') ||
    schedule.siblingPublish.startsWith('held') ||
    schedule.forward === 'held-reject-before-delivery';
  if (held) {
    // The retry arrives while the held publish is pending; the publish settles afterwards.
    await waitUntil(() => holdStarted);
    await settle();
    await sendRetry();
    await settle();
    // Live local runs take a step (consuming what the retry queued) before the publish settles.
    runs.step();
    releaseHold();
  } else if (schedule.retryTiming === 'early') {
    await sendRetry();
  }
  await settle(10);
  if (!held && schedule.retryTiming === 'settled') await sendRetry();
  await settle(10);
  releaseHold();
  // A retry during a local run is repeated once more, addressed to that still-live run.
  if (schedule.retry === 'during-local-run') sendRetryOn(ctx.runtime, ctx.agent, localRunId);
  await settle(10);
  publishSpy.mockRestore();
  ctx.finishRun();

  // Every run completes: local runs on both instances, the reused-id run, and the observed remote run.
  releaseReuse?.();
  await runs.completeAll();
  await otherRuns.completeAll();
  if (live.remote) {
    live.remote = false;
    if (ctx.pubsub.owners.get(ctx.key)?.includes(remoteRunId)) ctx.pubsub.owners.delete(ctx.key);
    await realPublish(`agent.thread-stream.${encodeURIComponent(ctx.key)}`, {
      type: 'run-completed',
      runId: remoteRunId,
      data: {
        type: 'run-completed',
        runId: remoteRunId,
        streamId: `${remoteRunId}-stream`,
        leaseOwner: encodedOwner(remoteRunId, 'pf6944-remote-source'),
      },
    });
    await ctx.pubsub.flush();
    await settle(10);
    await runs.completeAll();
  }
  if (winnerObserved) {
    finishWinner();
    await realPublish(`agent.thread-stream.${encodeURIComponent(ctx.key)}`, {
      type: 'run-completed',
      runId: ctx.winnerRunId,
      data: {
        type: 'run-completed',
        runId: ctx.winnerRunId,
        streamId: `${ctx.winnerRunId}-stream`,
        leaseOwner: ctx.winnerToken,
      },
    });
    await ctx.pubsub.flush();
    await settle(10);
    await runs.completeAll();
  }
  const settledCount = executions + (admitted.winner ? 1 : 0) + (admitted.remote ? 1 : 0);

  // A later local run on the thread takes whatever is still queued.
  finishWinner();
  const owner = ctx.pubsub.owners.get(ctx.key);
  if (owner) await ctx.pubsub.releaseLease(ctx.key, owner);
  const later = ctx.runtime.sendSignal(
    ctx.agent,
    { id: `${ctx.wakeId}-later`, type: 'user-message', contents: 'later' },
    scope,
    ctx.pubsub,
  );
  void later.accepted.catch(() => {});
  await waitUntil(() => !runs.hasOpenRun() || runs.openRunIds().length > 0);
  await settle(10);
  await runs.completeAll();
  const total = executions + (admitted.winner ? 1 : 0) + (admitted.remote ? 1 : 0);

  dateSpy?.mockRestore();
  subscription?.unsubscribe();
  const unexpected = retryErrors.filter(error => !(error instanceof Error && error.message.startsWith('schedule: ')));
  if (unexpected.length > 0) {
    throw new Error(`${followUpScheduleKey(schedule)}: unexpected retry failure: ${unexpected.map(String).join('; ')}`);
  }
  return { settled: settledCount, total };
}

/** Runs every schedule of a family and returns them with their counts (for the fork-main table). */
export async function countFollowUpFamily(family: string): Promise<Record<string, FollowUpCount>> {
  const counts: Record<string, FollowUpCount> = {};
  const familyIndex = Object.keys(followUpScheduleFamilies).indexOf(family);
  for (const [index, schedule] of enumerateFollowUpSchedules(followUpScheduleFamilies[family]!).entries()) {
    counts[followUpScheduleKey(schedule)] = await countFollowUpRuns(schedule, familyIndex * 10_000 + index);
  }
  return counts;
}

/**
 * Checks every schedule of a family against fork main's counts and returns one readable line per
 * failing schedule (empty when all hold):
 * - F runs at least as often as on fork main (counted up to once: repeated runs of one F are
 *   duplicates, never owed), neither by the end of the retry phase nor in total;
 * - F runs at most once in total (or as often as on fork main, if more), plus one only in the
 *   disclosed at-least-once cases that `followUpScheduleExtraRunsAllowed` labels.
 */
export async function checkFollowUpFamily(
  family: string,
  forkMain: Readonly<Record<string, FollowUpCount>>,
): Promise<string[]> {
  const failures: string[] = [];
  const familyIndex = Object.keys(followUpScheduleFamilies).indexOf(family);
  for (const [index, schedule] of enumerateFollowUpSchedules(followUpScheduleFamilies[family]!).entries()) {
    const key = followUpScheduleKey(schedule);
    const expected = forkMain[key];
    if (expected === undefined) {
      failures.push(`${key}: no fork main count recorded`);
      continue;
    }
    const count = await countFollowUpRuns(schedule, familyIndex * 10_000 + index);
    const allowed = Math.max(expected.total, 1) + followUpScheduleExtraRunsAllowed(schedule);
    const owedSettled = Math.min(expected.settled, 1);
    const owedTotal = Math.min(expected.total, 1);
    if (count.settled < owedSettled || count.total < owedTotal || count.total > allowed) {
      failures.push(
        `${key}: branch runs F ${count.settled}x by the end of the retry phase and ${count.total}x in total; ` +
          `fork main ${expected.settled}x and ${expected.total}x; allowed total ${expected.total}..${allowed}`,
      );
    }
  }
  return failures;
}
