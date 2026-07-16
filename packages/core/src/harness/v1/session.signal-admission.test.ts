import { describe, expect, it } from 'vitest';

import { MockAgent } from './__test-utils__/mock-agent';
import { setupHarness } from './__test-utils__/setup';
import { HarnessAdmissionConflictError, HarnessSessionNotFoundError, HarnessValidationError } from './errors';
import type { HarnessEvent } from './events';

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

  it('keeps an admitted suspended signal pending across restart and settles it on resume', async () => {
    const first = setupHarness();
    first.agent.enqueueRun({
      finishReason: 'suspended',
      suspendPayload: { toolCallId: 'tc-restart', toolName: 'shell', args: { cmd: 'ls' } },
    });
    const session = await first.harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const original = await session.signal({ content: 'approve me', admissionId: 'restart-suspended' });
    await original.result;
    await waitFor(() => session.getRecord().pendingResume !== undefined, 'initial suspension');
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
    expect(secondAgent.resumeCalls).toHaveLength(1);
    expect(events.filter(event => event.type === 'signal_completed' && event.signalId === original.id)).toHaveLength(1);
    await expect(recovered.lookupMessageResult(original.id)).resolves.toMatchObject({
      status: 'completed',
      admissionId: 'restart-suspended',
    });
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
