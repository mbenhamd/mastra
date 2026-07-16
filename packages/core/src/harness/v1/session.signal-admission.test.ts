import { describe, expect, it, vi } from 'vitest';

import { createSignal } from '../../agent/signals';
import type { InMemoryHarness } from '../../storage/domains/harness/inmemory';
import { MockAgent } from './__test-utils__/mock-agent';
import { setupHarness } from './__test-utils__/setup';
import { HarnessAdmissionConflictError, HarnessSessionNotFoundError, HarnessValidationError } from './errors';
import type { HarnessEvent } from './events';
import type { Session } from './session';

async function waitForStreamCalls(agent: MockAgent, expected: number): Promise<void> {
  for (let i = 0; i < 100 && agent.streamCalls.length < expected; i++) {
    await new Promise<void>(resolve => setImmediate(resolve));
  }
}

async function waitFor(predicate: () => boolean, label: string): Promise<void> {
  for (let i = 0; i < 200; i++) {
    if (predicate()) return;
    await new Promise<void>(resolve => setTimeout(resolve, 5));
  }
  throw new Error(`timed out waiting for ${label}`);
}

async function seedPendingSignalAdmission(
  session: Session,
  storage: InMemoryHarness,
  opts: { admissionId: string; content: string; runId?: string; modeId?: string; modelId?: string },
): Promise<{ signalId: string; idleRunId: string }> {
  const internals = session as unknown as {
    _computeSignalAdmissionHash: (opts: { content: string }, attachments: [], requestContext?: undefined) => string;
    _signalAdmissionIdentity: (admissionId: string) => { signalId: string; idleRunId: string };
  };
  const admissionHash = internals._computeSignalAdmissionHash({ content: opts.content }, [], undefined);
  const identity = internals._signalAdmissionIdentity(opts.admissionId);
  const now = Date.now();
  await storage.writeMessageResultEvidence({
    status: 'pending',
    harnessName: session.getRecord().harnessName,
    sessionId: session.id,
    resourceId: session.resourceId,
    threadId: session.threadId,
    signalId: identity.signalId,
    runId: opts.runId,
    modeId: opts.modeId ?? session.getRecord().modeId,
    modelId: opts.modelId ?? session.getRecord().modelId,
    admissionId: opts.admissionId,
    admissionHash,
    createdAt: now,
    updatedAt: now,
  });
  return identity;
}

async function expectRedactedResult(result: Promise<unknown>, secret: string): Promise<void> {
  const thrown = await result.then(
    () => undefined,
    (error: unknown) => error,
  );
  expect(thrown).toMatchObject({ name: 'HarnessExecutionError', message: 'An internal harness error occurred' });
  expect((thrown as Error).message).not.toContain(secret);
}

describe('Session.signal() admissionId', () => {
  it('replays an accepted idle wake with one stable signal/run/result and one terminal event', async () => {
    const agent = new MockAgent({ id: 'default' });
    let release!: () => void;
    agent.enqueueRun({
      holdUntil: new Promise<void>(resolve => {
        release = resolve;
      }),
      text: 'accepted once',
    });
    const { harness } = setupHarness({ agents: { default: agent } });
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const events: HarnessEvent[] = [];
    session.subscribe(event => events.push(event));

    const first = await session.signal({ content: 'hello', admissionId: 'idle-once' });
    const duplicate = await session.signal({ content: 'hello', admissionId: 'idle-once' });

    expect(duplicate).toMatchObject({
      id: first.id,
      runId: first.runId,
      accepted: true,
      willInterleave: false,
    });
    expect(agent.streamCalls).toHaveLength(1);

    release();
    const [firstResult, duplicateResult] = await Promise.all([first.result, duplicate.result]);
    expect(duplicateResult).toEqual(firstResult);
    expect(events.filter(event => event.type === 'signal_completed' && event.signalId === first.id)).toHaveLength(1);

    const evidence = await session.lookupMessageResult(first.id);
    expect(evidence).toMatchObject({
      status: 'completed',
      signalId: first.id,
      runId: first.runId,
      admissionId: 'idle-once',
    });
  });

  it('deduplicates concurrent exact admissions before a second idle dispatch', async () => {
    const agent = new MockAgent({ id: 'default' });
    let release!: () => void;
    agent.enqueueRun({
      holdUntil: new Promise<void>(resolve => {
        release = resolve;
      }),
    });
    const { harness } = setupHarness({ agents: { default: agent } });
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });

    const [first, duplicate] = await Promise.all([
      session.signal({ content: 'hello', admissionId: 'concurrent-once' }),
      session.signal({ content: 'hello', admissionId: 'concurrent-once' }),
    ]);

    expect(duplicate.id).toBe(first.id);
    expect(duplicate.runId).toBe(first.runId);
    expect(agent.streamCalls).toHaveLength(1);
    release();
    await Promise.all([first.result, duplicate.result]);
  });

  it('replays an active interleave without appending a second signal or provider run', async () => {
    const agent = new MockAgent({ id: 'default' });
    let release!: () => void;
    agent.enqueueRun({
      holdUntil: new Promise<void>(resolve => {
        release = resolve;
      }),
      text: 'shared terminal',
    });
    const { harness } = setupHarness({ agents: { default: agent } });
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const firstTurn = session.message({ content: 'first' });
    await waitForStreamCalls(agent, 1);

    const events: HarnessEvent[] = [];
    session.subscribe(event => events.push(event));
    const first = await session.signal({ content: 'steer once', admissionId: 'active-once' });
    const duplicate = await session.signal({ content: 'steer once', admissionId: 'active-once' });

    expect(first.willInterleave).toBe(true);
    expect(duplicate).toMatchObject({ id: first.id, runId: first.runId, willInterleave: true });
    expect(agent.streamCalls).toHaveLength(1);

    release();
    const original = await firstTurn;
    const [firstResult, duplicateResult] = await Promise.all([first.result, duplicate.result]);
    expect(first.runId).toBe(original.runId);
    expect(duplicateResult).toEqual(firstResult);
    expect(events.filter(event => event.type === 'signal_completed' && event.signalId === first.id)).toHaveLength(1);
  });

  it('rechecks routing after durable admission when the previously active run finishes', async () => {
    const agent = new MockAgent({ id: 'default' });
    let releaseActive!: () => void;
    agent.enqueueRun({
      holdUntil: new Promise<void>(resolve => {
        releaseActive = resolve;
      }),
      text: 'first terminal',
    });
    agent.enqueueRun({ text: 'idle recovery terminal' });
    const { harness, storage } = setupHarness({ agents: { default: agent } });
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const active = session.message({ content: 'active first' });
    await waitForStreamCalls(agent, 1);

    const realWrite = storage.writeMessageResultEvidence.bind(storage);
    let reservationStarted!: () => void;
    const reservationObserved = new Promise<void>(resolve => {
      reservationStarted = resolve;
    });
    let releaseReservation!: () => void;
    const reservationGate = new Promise<void>(resolve => {
      releaseReservation = resolve;
    });
    storage.writeMessageResultEvidence = async record => {
      if (record.admissionId === 'routing-race' && record.status === 'pending' && record.runId === undefined) {
        reservationStarted();
        await reservationGate;
      }
      return realWrite(record);
    };

    const signal = session.signal({ content: 'route after admission', admissionId: 'routing-race' });
    await reservationObserved;
    releaseActive();
    await active;
    releaseReservation();

    const handle = await signal;
    expect(handle.willInterleave).toBe(false);
    await expect(handle.result).resolves.toMatchObject({ text: 'idle recovery terminal' });
    expect(agent.streamCalls).toHaveLength(2);
  });

  it('uses the native accepted run id for an admitted active delivery', async () => {
    const agent = new MockAgent({ id: 'default' });
    let release!: () => void;
    agent.enqueueRun({
      holdUntil: new Promise<void>(resolve => {
        release = resolve;
      }),
      text: 'authoritative terminal',
    });
    const { harness } = setupHarness({ agents: { default: agent } });
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const active = session.message({ content: 'active first' });
    await waitForStreamCalls(agent, 1);

    const realSendSignal = agent.sendSignal.bind(agent);
    agent.sendSignal = ((signal: any, target: any) => {
      const dispatched = realSendSignal(signal, target);
      return { ...dispatched, runId: 'stale-provisional-run' };
    }) as typeof agent.sendSignal;

    const handle = await session.signal({ content: 'land once', admissionId: 'authoritative-active' });
    expect(handle.willInterleave).toBe(true);
    expect(handle.runId).not.toBe('stale-provisional-run');

    release();
    const activeResult = await active;
    expect(handle.runId).toBe(activeResult.runId);
    await expect(handle.result).resolves.toEqual(activeResult);
  });

  it('reroutes an admitted active candidate through an owned turn when native admission discards it', async () => {
    const agent = new MockAgent({ id: 'default' });
    let releaseActive!: () => void;
    agent.enqueueRun({
      holdUntil: new Promise<void>(resolve => {
        releaseActive = resolve;
      }),
      text: 'active terminal',
    });
    agent.enqueueRun({ text: 'rerouted owned terminal' });
    const { harness } = setupHarness({ agents: { default: agent } });
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const active = session.message({ content: 'active first' });
    await waitForStreamCalls(agent, 1);

    const realSendSignal = agent.sendSignal.bind(agent);
    let discarded = false;
    agent.sendSignal = ((signal: any, target: any) => {
      if (!discarded && target.ifIdle?.behavior === 'discard') {
        discarded = true;
        return {
          signal: createSignal({ ...signal, acceptedAt: new Date() }),
          runId: 'discarded-provisional-run',
          accepted: (async () => {
            releaseActive();
            await active;
            return { action: 'discard' as const };
          })(),
        };
      }
      return realSendSignal(signal, target);
    }) as typeof agent.sendSignal;

    const handle = await session.signal({ content: 'reroute once', admissionId: 'active-discard-race' });
    expect(handle).toMatchObject({ willInterleave: false, accepted: true });
    expect(handle.runId).not.toBe('discarded-provisional-run');
    await expect(handle.result).resolves.toMatchObject({ text: 'rerouted owned terminal' });
    expect(agent.streamCalls).toHaveLength(2);
  });

  it('rejects payload conflicts and non-hash-safe options before another dispatch', async () => {
    const { harness, agent } = setupHarness();
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });

    const first = await session.signal({
      content: 'hello',
      admissionId: 'conflict',
      requestContext: { app: { tenant: 'one' } },
    });
    await first.result;

    await expect(
      session.signal({
        content: 'changed',
        admissionId: 'conflict',
        requestContext: { app: { tenant: 'one' } },
      }),
    ).rejects.toBeInstanceOf(HarnessAdmissionConflictError);
    await expect(
      session.signal({
        content: 'hello',
        admissionId: 'conflict',
        requestContext: { app: { tenant: 'two' } },
      }),
    ).rejects.toBeInstanceOf(HarnessAdmissionConflictError);
    await expect(
      session.signal({ content: 'hello', admissionId: 'unsafe-tools', additionalTools: { local: {} as never } }),
    ).rejects.toBeInstanceOf(HarnessValidationError);
    await expect(session.signal({ content: 'hello', admissionId: '' })).rejects.toBeInstanceOf(HarnessValidationError);
    expect(agent.streamCalls).toHaveLength(1);
  });

  it('resolves a completed retry from its original topology even while another run is active', async () => {
    const agent = new MockAgent({ id: 'default' });
    const { harness } = setupHarness({
      agents: { default: agent },
      modes: [
        { id: 'default', agentId: 'default' },
        { id: 'other', agentId: 'default' },
      ],
    });
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const original = await session.signal({ content: 'original', admissionId: 'topology', mode: 'other' });
    await original.result;

    let release!: () => void;
    agent.enqueueRun({
      holdUntil: new Promise<void>(resolve => {
        release = resolve;
      }),
    });
    const active = session.message({ content: 'new active run' });
    await waitForStreamCalls(agent, 2);

    const duplicate = await session.signal({ content: 'original', admissionId: 'topology', mode: 'other' });
    expect(duplicate).toMatchObject({
      id: original.id,
      runId: original.runId,
      willInterleave: false,
    });
    expect(agent.streamCalls).toHaveLength(2);

    release();
    await active;
    await duplicate.result;
  });

  it('recovers a durable pre-dispatch reservation after restart under the same stable identity', async () => {
    const first = setupHarness();
    const session = await first.harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const admissionId = 'restart-before-dispatch';
    const admissionHash = (session as any)._computeSignalAdmissionHash(
      { content: 'recover me' },
      [],
      undefined,
    ) as string;
    const identity = (session as any)._signalAdmissionIdentity(admissionId) as {
      signalId: string;
      idleRunId: string;
    };
    const now = Date.now();
    await first.storage.writeMessageResultEvidence({
      status: 'pending',
      harnessName: session.getRecord().harnessName,
      sessionId: session.id,
      resourceId: session.resourceId,
      threadId: session.threadId,
      signalId: identity.signalId,
      modeId: 'default',
      modelId: session.getRecord().modelId,
      admissionId,
      admissionHash,
      createdAt: now,
      updatedAt: now,
    });
    const sessionId = session.id;
    await first.harness.shutdown();

    const second = setupHarness({ sessions: { storage: first.storage } });
    const recovered = await second.harness.session({ sessionId, resourceId: 'u1' });
    const handle = await recovered.signal({ content: 'recover me', admissionId });

    expect(handle).toMatchObject({
      id: identity.signalId,
      runId: identity.idleRunId,
      willInterleave: false,
    });
    await handle.result;
    expect(second.agent.streamCalls).toHaveLength(1);
    expect(handle.runId).not.toBe(handle.id);
  });

  it('redispatches a pending admission with its persisted mode after the session default changes', async () => {
    const defaultAgent = new MockAgent({ id: 'default' });
    const recoveredAgent = new MockAgent({ id: 'recovered' });
    const { harness, storage } = setupHarness({
      agents: { default: defaultAgent, recovered: recoveredAgent },
      modes: [
        { id: 'default', agentId: 'default' },
        { id: 'recovered', agentId: 'recovered' },
      ],
    });
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const admissionId = 'persisted-mode-recovery';
    const identity = await seedPendingSignalAdmission(session, storage, {
      admissionId,
      content: 'recover original mode',
      modeId: 'recovered',
    });
    const pending = await session.lookupMessageResult(identity.signalId);
    expect(pending).toMatchObject({ status: 'pending', runId: undefined, modeId: 'recovered' });

    const handle = await session.signal({ content: 'recover original mode', admissionId });
    expect(handle).toMatchObject({ id: identity.signalId, runId: identity.idleRunId, willInterleave: false });
    await handle.result;
    expect(recoveredAgent.streamCalls).toHaveLength(1);
    expect(defaultAgent.streamCalls).toHaveLength(0);
  });

  it('replays a terminal result after restart without dispatching a provider run', async () => {
    const first = setupHarness();
    const session = await first.harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const original = await session.signal({ content: 'persist result', admissionId: 'restart-terminal' });
    const result = await original.result;
    const sessionId = session.id;
    await first.harness.shutdown();

    const second = setupHarness({ sessions: { storage: first.storage } });
    const recovered = await second.harness.session({ sessionId, resourceId: 'u1' });
    const duplicate = await recovered.signal({ content: 'persist result', admissionId: 'restart-terminal' });

    expect(duplicate).toMatchObject({ id: original.id, runId: original.runId, willInterleave: false });
    await expect(duplicate.result).resolves.toEqual(result);
    expect(second.agent.streamCalls).toHaveLength(0);
    await expect(second.harness.session({ sessionId, resourceId: 'foreign' })).rejects.toBeInstanceOf(
      HarnessSessionNotFoundError,
    );
  });

  it('replays terminal evidence before resolving a mode that no longer exists', async () => {
    const { harness, storage, agent } = setupHarness();
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const admissionId = 'removed-mode-terminal';
    const content = 'stored answer';
    const admissionHash = (session as any)._computeSignalAdmissionHash(
      { content, mode: 'removed-mode' },
      [],
      undefined,
    ) as string;
    const identity = (session as any)._signalAdmissionIdentity(admissionId) as {
      signalId: string;
      idleRunId: string;
    };
    const result = {
      runId: identity.idleRunId,
      text: 'terminal from removed mode',
      finishReason: 'stop',
      usage: {},
    } as any;
    const now = Date.now();
    await storage.writeMessageResultEvidence({
      status: 'completed',
      harnessName: session.getRecord().harnessName,
      sessionId: session.id,
      resourceId: session.resourceId,
      threadId: session.threadId,
      signalId: identity.signalId,
      runId: identity.idleRunId,
      modeId: 'removed-mode',
      modelId: session.getRecord().modelId,
      admissionId,
      admissionHash,
      result,
      createdAt: now,
      updatedAt: now,
    });

    const duplicate = await session.signal({ content, admissionId, mode: 'removed-mode' });
    expect(duplicate).toMatchObject({ id: identity.signalId, runId: identity.idleRunId });
    await expect(duplicate.result).resolves.toEqual(result);
    expect(agent.streamCalls).toHaveLength(0);
  });

  it('keeps an admitted suspended signal pending across restart and settles it on resume', async () => {
    const first = setupHarness();
    first.agent.enqueueRun({
      finishReason: 'suspended',
      suspendPayload: { toolCallId: 'tc-restart', toolName: 'shell', args: { cmd: 'ls' } },
    });
    const session = await first.harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const original = await session.signal({ content: 'approve me', admissionId: 'restart-suspended' });
    let originalSettled = false;
    const originalResult = original.result.finally(() => {
      originalSettled = true;
    });
    void originalResult.catch(() => {});
    await waitFor(() => session.getRecord().pendingResume !== undefined, 'initial suspension');
    expect(originalSettled).toBe(false);
    const sameProcessDuplicate = await session.signal({ content: 'approve me', admissionId: 'restart-suspended' });
    let sameProcessDuplicateSettled = false;
    const sameProcessDuplicateResult = sameProcessDuplicate.result.finally(() => {
      sameProcessDuplicateSettled = true;
    });
    void sameProcessDuplicateResult.catch(() => {});
    await new Promise<void>(resolve => setImmediate(resolve));
    expect(sameProcessDuplicateSettled).toBe(false);
    const now = Date.now();
    const dateNow = vi.spyOn(Date, 'now').mockReturnValue(now + 60_000);
    await new Promise<void>(resolve => setTimeout(resolve, 150));
    expect(originalSettled).toBe(false);
    expect(sameProcessDuplicateSettled).toBe(false);
    dateNow.mockRestore();
    const sessionId = session.id;
    await first.harness.shutdown();

    const secondAgent = new MockAgent({ id: 'default' });
    secondAgent.enqueueRun({ finishReason: 'stop', text: 'resumed once' });
    const second = setupHarness({
      agents: { default: secondAgent },
      sessions: { storage: first.storage },
    });
    const recovered = await second.harness.session({ sessionId, resourceId: 'u1' });
    const events: HarnessEvent[] = [];
    recovered.subscribe(event => events.push(event));
    const duplicate = await recovered.signal({ content: 'approve me', admissionId: 'restart-suspended' });

    expect(duplicate).toMatchObject({ id: original.id, runId: original.runId, willInterleave: false });
    expect(secondAgent.streamCalls).toHaveLength(0);
    await recovered.respondToToolApproval({ approved: true });
    await expect(duplicate.result).resolves.toMatchObject({ finishReason: 'stop', text: 'resumed once' });
    await expect(originalResult).resolves.toMatchObject({ finishReason: 'stop', text: 'resumed once' });
    await expect(sameProcessDuplicateResult).resolves.toMatchObject({ finishReason: 'stop', text: 'resumed once' });
    expect(secondAgent.resumeCalls).toHaveLength(1);
    expect(events.filter(event => event.type === 'signal_completed' && event.signalId === original.id)).toHaveLength(1);
    await expect(recovered.lookupMessageResult(original.id)).resolves.toMatchObject({
      status: 'completed',
      admissionId: 'restart-suspended',
    });
  });

  it('keeps admitted signal evidence pending when a retryable resume fails, then settles on retry', async () => {
    const agent = new MockAgent({ id: 'default' });
    agent.enqueueRuns([
      {
        finishReason: 'suspended',
        suspendPayload: { toolCallId: 'tc-retryable', toolName: 'shell', args: { cmd: 'pwd' } },
      },
      { finishReason: 'stop', text: 'resume retry succeeded' },
    ]);
    const realResume = agent.resumeStream.bind(agent);
    let rejectFirstResume = true;
    agent.resumeStream = (async (resumeData: any, options?: any) => {
      if (rejectFirstResume) {
        rejectFirstResume = false;
        agent.resumeCalls.push({ resumeData, options });
        throw new Error('transient resume provider failure');
      }
      return realResume(resumeData, options);
    }) as typeof agent.resumeStream;
    const { harness } = setupHarness({ agents: { default: agent } });
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const events: HarnessEvent[] = [];
    session.subscribe(event => events.push(event));
    const handle = await session.signal({ content: 'retry my approval', admissionId: 'retryable-resume' });
    let resultSettled = false;
    const result = handle.result.finally(() => {
      resultSettled = true;
    });
    void result.catch(() => {});
    await waitFor(() => session.getRecord().pendingResume !== undefined, 'retryable suspension');

    await expect(session.respondToToolApproval({ approved: true })).rejects.toMatchObject({
      name: 'HarnessExecutionError',
      message: 'An internal harness error occurred',
    });
    await waitFor(
      () => session.getRecord().pendingResume?.resumedAt === undefined,
      'failed resume marker to become retryable',
    );
    await expect(session.lookupMessageResult(handle.id)).resolves.toMatchObject({ status: 'pending' });
    await new Promise<void>(resolve => setTimeout(resolve, 150));
    expect(resultSettled).toBe(false);
    expect(events.filter(event => event.type === 'signal_failed' && event.signalId === handle.id)).toEqual([]);

    await session.respondToToolApproval({ approved: true });
    await expect(result).resolves.toMatchObject({ finishReason: 'stop', text: 'resume retry succeeded' });
    await expect(session.lookupMessageResult(handle.id)).resolves.toMatchObject({
      status: 'completed',
      result: expect.objectContaining({ text: 'resume retry succeeded' }),
    });
    expect(agent.resumeCalls).toHaveLength(2);
    expect(events.filter(event => event.type === 'signal_completed' && event.signalId === handle.id)).toHaveLength(1);
  });

  it('fails closed and redacts a reservation storage error before dispatch', async () => {
    const { harness, storage, agent } = setupHarness();
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const realWrite = storage.writeMessageResultEvidence.bind(storage);
    let fail = true;
    storage.writeMessageResultEvidence = async record => {
      if (fail && record.admissionId === 'storage-failure') {
        fail = false;
        throw new Error('postgres://secret@db reservation failed');
      }
      return realWrite(record);
    };

    const thrown = await session.signal({ content: 'hello', admissionId: 'storage-failure' }).then(
      () => undefined,
      (error: unknown) => error,
    );
    expect(thrown).toMatchObject({ name: 'HarnessExecutionError', message: 'An internal harness error occurred' });
    expect((thrown as Error).message).not.toContain('secret');
    expect(agent.streamCalls).toHaveLength(0);

    const retried = await session.signal({ content: 'hello', admissionId: 'storage-failure' });
    await retried.result;
    expect(agent.streamCalls).toHaveLength(1);
  });

  it('redacts cached, live, and durable duplicate result failures', async () => {
    const scenarios = ['cached', 'live', 'durable'] as const;
    for (const scenario of scenarios) {
      const { harness, storage, agent } = setupHarness();
      const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
      const admissionId = `duplicate-${scenario}-failure`;
      const runId = `run-${scenario}-failure`;
      await seedPendingSignalAdmission(session, storage, {
        admissionId,
        content: 'replay failure',
        runId,
      });
      const secret = `${scenario}-dsn-secret`;

      let rejectLive: ((error: unknown) => void) | undefined;
      if (scenario === 'cached') {
        (session as any)._completedRuns.set(runId, { ok: false, err: new Error(secret) });
      } else if (scenario === 'live') {
        const promise = new Promise<never>((_, reject) => {
          rejectLive = reject;
        });
        void promise.catch(() => {});
        (session as any)._runCompletionPromises.set(runId, {
          promise,
          resolve: () => {},
          reject: rejectLive,
        });
      } else {
        storage.loadMessageResultEvidence = async () => {
          throw new Error(secret);
        };
      }

      const duplicate = await session.signal({ content: 'replay failure', admissionId });
      rejectLive?.(new Error(secret));
      await expectRedactedResult(duplicate.result, secret);
      if (scenario === 'cached') {
        await expect(session.lookupMessageResult(duplicate.id)).resolves.toMatchObject({ status: 'failed' });
      }
      expect(agent.streamCalls).toHaveLength(0);
      await harness.shutdown();
    }
  });

  it('settles durable evidence from a cached completed run exactly once', async () => {
    const { harness, storage, agent } = setupHarness();
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const runId = 'cached-completed-run';
    const identity = await seedPendingSignalAdmission(session, storage, {
      admissionId: 'cached-completed-admission',
      content: 'recover cached completion',
      runId,
    });
    const full = { runId, text: 'cached terminal', finishReason: 'stop', usage: {} } as any;
    (session as any)._completedRuns.set(runId, { ok: true, full });
    const events: HarnessEvent[] = [];
    session.subscribe(event => events.push(event));

    const [first, second] = await Promise.all([
      session.signal({ content: 'recover cached completion', admissionId: 'cached-completed-admission' }),
      session.signal({ content: 'recover cached completion', admissionId: 'cached-completed-admission' }),
    ]);
    await expect(first.result).resolves.toEqual(full);
    await expect(second.result).resolves.toEqual(full);
    await expect(session.lookupMessageResult(identity.signalId)).resolves.toMatchObject({
      status: 'completed',
      result: full,
    });
    expect(
      events.filter(event => event.type === 'signal_completed' && event.signalId === identity.signalId),
    ).toHaveLength(1);
    expect(agent.streamCalls).toHaveLength(0);
  });

  it('redacts an async duplicate subscription failure before returning a handle', async () => {
    const { harness, storage, agent } = setupHarness();
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    await seedPendingSignalAdmission(session, storage, {
      admissionId: 'subscription-failure',
      content: 'recover subscription',
      runId: 'subscription-failure-run',
    });
    const secret = 'redis://duplicate-subscription-secret';
    (session as any)._ensureThreadSubscription = async () => {
      throw new Error(secret);
    };

    const thrown = await session.signal({ content: 'recover subscription', admissionId: 'subscription-failure' }).then(
      () => undefined,
      (error: unknown) => error,
    );
    expect(thrown).toMatchObject({ name: 'HarnessExecutionError', message: 'An internal harness error occurred' });
    expect((thrown as Error).message).not.toContain(secret);
    expect(agent.streamCalls).toHaveLength(0);
  });

  it('redacts a fresh signal subscription failure for admitted and optimistic callers', async () => {
    for (const admitted of [false, true]) {
      const { harness, agent } = setupHarness();
      const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
      const secret = `redis://fresh-subscription-${admitted}-secret`;
      (session as any)._ensureThreadSubscription = async () => {
        throw new Error(secret);
      };

      const thrown = await session
        .signal({
          content: 'fresh subscription',
          ...(admitted ? { admissionId: 'fresh-subscription-admission' } : {}),
        })
        .then(
          () => undefined,
          (error: unknown) => error,
        );
      expect(thrown).toMatchObject({ name: 'HarnessExecutionError', message: 'An internal harness error occurred' });
      expect((thrown as Error).message).not.toContain(secret);
      expect(agent.streamCalls).toHaveLength(0);
      await harness.shutdown();
    }
  });

  it('emits no terminal event when durable evidence reports another actor won settlement', async () => {
    const { harness, storage } = setupHarness();
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const identity = await seedPendingSignalAdmission(session, storage, {
      admissionId: 'settlement-loser',
      content: 'settle once across actors',
      runId: 'settlement-loser-run',
    });
    const events: HarnessEvent[] = [];
    session.subscribe(event => events.push(event));
    const realWrite = storage.writeMessageResultEvidence.bind(storage);
    storage.writeMessageResultEvidence = async record => {
      const result = await realWrite(record);
      return record.signalId === identity.signalId && record.status === 'completed'
        ? { ...result, applied: false }
        : result;
    };
    const result = {
      runId: 'settlement-loser-run',
      text: 'won elsewhere',
      finishReason: 'stop',
      usage: {},
    } as any;

    await (session as any)._settleSignalResult(identity.signalId, {
      status: 'completed',
      runId: result.runId,
      result,
    });

    await expect(session.lookupMessageResult(identity.signalId)).resolves.toMatchObject({
      status: 'completed',
      result,
    });
    expect(events.filter(event => event.type === 'signal_completed' && event.signalId === identity.signalId)).toEqual(
      [],
    );
  });

  it('scopes admission identity to the owning session/thread and keeps queue admission separate', async () => {
    const { harness, agent } = setupHarness();
    const a = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const b = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });

    const signalA = await a.signal({ content: 'same', admissionId: 'shared-key' });
    const signalB = await b.signal({ content: 'same', admissionId: 'shared-key' });
    await Promise.all([signalA.result, signalB.result]);
    expect(signalB.id).not.toBe(signalA.id);

    const queued = a.queue({ content: 'same', admissionId: 'shared-key' });
    await expect(queued).resolves.toMatchObject({ finishReason: 'stop' });
    expect(agent.streamCalls).toHaveLength(3);
  });
});
