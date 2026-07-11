import { describe, expect, it, vi } from 'vitest';
import { z } from 'zod/v4';
import { createStep, createWorkflow } from '..';
import { EventEmitterPubSub } from '../../../events/event-emitter';
import { PubSub } from '../../../events/pubsub';
import type { Event, EventCallback } from '../../../events/types';
import { Mastra } from '../../../mastra';
import { MockStore } from '../../../storage/mock';
import { createEmptyWorkflowSnapshot } from '../../../storage/workflow-snapshot';
import { WorkflowEventProcessor } from '.';

function makeStartEvent(workflowId: string, runId: string, deliveryAttempt?: number): Event {
  return {
    id: `event-${runId}`,
    type: 'workflow.start',
    runId,
    createdAt: new Date(),
    deliveryAttempt,
    data: {
      workflowId,
      runId,
      executionPath: [0],
      stepResults: {},
      prevResult: { status: 'success', output: {} },
      activeStepsPath: {},
      requestContext: {},
    },
  } as Event;
}

function makeWorkflow(id: string) {
  return createWorkflow({
    id,
    inputSchema: z.object({}),
    outputSchema: z.object({}),
  })
    .then(
      createStep({
        id: 'noop',
        inputSchema: z.object({}),
        outputSchema: z.object({}),
        execute: async () => ({}),
      }) as any,
    )
    .commit();
}

async function persistRun(mastra: Mastra, workflowId: string, runId: string, status: 'running' | 'success' = 'running') {
  const workflowsStore = await mastra.getStorage()?.getStore('workflows');
  await workflowsStore?.persistWorkflowSnapshot({
    workflowName: workflowId,
    runId,
    snapshot: { ...createEmptyWorkflowSnapshot(runId), status },
  });
}

class AlwaysThrowsProcessor extends WorkflowEventProcessor {
  static dispatchCalls = 0;

  override async loadData(): Promise<undefined> {
    AlwaysThrowsProcessor.dispatchCalls += 1;
    throw Object.assign(new Error('SQLITE_BUSY: database is locked (test)'), { code: 'SQLITE_BUSY' });
  }
}

class FailingTerminalPubSub extends EventEmitterPubSub {
  failPublishesRemaining = 0;
  terminalPublishes = 0;

  override async publish(
    topic: string,
    event: Parameters<EventEmitterPubSub['publish']>[1],
    options?: { localOnly?: boolean },
  ): Promise<void> {
    if (topic === 'workflows' && event.type === 'workflow.fail') {
      this.terminalPublishes += 1;
      if (this.failPublishesRemaining > 0) {
        this.failPublishesRemaining -= 1;
        throw new Error('terminal transport unavailable');
      }
    }
    return super.publish(topic, event, options);
  }
}

class ManualPushPubSub extends PubSub {
  callback?: EventCallback;

  override get supportedModes() {
    return ['push'] as const;
  }

  async publish(): Promise<void> {}

  async subscribe(_topic: string, cb: EventCallback): Promise<void> {
    this.callback = cb;
  }

  async unsubscribe(): Promise<void> {
    this.callback = undefined;
  }

  async flush(): Promise<void> {}

  deliver(event: Event, ack: () => Promise<void>, nack: () => Promise<void>) {
    this.callback?.(event, ack, nack);
  }
}

describe('WorkflowEventProcessor transport-owned retry budget', () => {
  it('keeps the source budget across processor replacement', async () => {
    const pubsub = new EventEmitterPubSub();
    const mastra = new Mastra({
      logger: false,
      storage: new MockStore(),
      workflows: { wf: makeWorkflow('wf') } as any,
      pubsub,
    });
    const failEvents: Event[] = [];
    await pubsub.subscribe('workflows', async event => {
      if (event.type === 'workflow.fail') failEvents.push(event);
    });
    await persistRun(mastra, 'wf', 'rotated-run');
    AlwaysThrowsProcessor.dispatchCalls = 0;

    for (const [attempt, expected] of [
      [1, { ok: false, retry: true }],
      [2, { ok: false, retry: true }],
      [3, { ok: false, retry: false }],
    ] as const) {
      const processor = new AlwaysThrowsProcessor({ mastra });
      await expect(processor.handle(makeStartEvent('wf', 'rotated-run', attempt))).resolves.toEqual(expected);
    }

    expect(AlwaysThrowsProcessor.dispatchCalls).toBe(3);
    expect(failEvents).toHaveLength(1);

    // A later broker delivery goes directly to terminal propagation. A fresh
    // processor cannot grant the source event another execution budget.
    const replacement = new AlwaysThrowsProcessor({ mastra });
    await expect(replacement.handle(makeStartEvent('wf', 'rotated-run', 4))).resolves.toEqual({
      ok: false,
      retry: false,
    });
    expect(AlwaysThrowsProcessor.dispatchCalls).toBe(3);

    await mastra.shutdown();
  });

  it('retries failed terminal publication without re-entering source dispatch', async () => {
    const pubsub = new FailingTerminalPubSub();
    pubsub.failPublishesRemaining = 1;
    const mastra = new Mastra({
      logger: false,
      storage: new MockStore(),
      workflows: { wf: makeWorkflow('wf') } as any,
      pubsub,
    });
    await persistRun(mastra, 'wf', 'terminal-recovery');
    AlwaysThrowsProcessor.dispatchCalls = 0;

    const exhaustingWorker = new AlwaysThrowsProcessor({ mastra });
    await expect(exhaustingWorker.handle(makeStartEvent('wf', 'terminal-recovery', 3))).resolves.toEqual({
      ok: false,
      retry: true,
    });
    expect(AlwaysThrowsProcessor.dispatchCalls).toBe(1);

    const recoveryWorker = new AlwaysThrowsProcessor({ mastra });
    await expect(recoveryWorker.handle(makeStartEvent('wf', 'terminal-recovery', 4))).resolves.toEqual({
      ok: false,
      retry: false,
    });
    expect(AlwaysThrowsProcessor.dispatchCalls).toBe(1);
    expect(pubsub.terminalPublishes).toBe(2);

    await mastra.shutdown();
  });

  it('never republishes workflow.fail when terminal processing itself fails', async () => {
    const pubsub = new FailingTerminalPubSub();
    const mastra = new Mastra({
      logger: false,
      storage: new MockStore(),
      workflows: { wf: makeWorkflow('wf') } as any,
      pubsub,
    });
    AlwaysThrowsProcessor.dispatchCalls = 0;
    const processor = new AlwaysThrowsProcessor({ mastra });
    const event = {
      ...makeStartEvent('wf', 'failed-terminal', 9),
      type: 'workflow.fail',
    } as Event;

    await expect(processor.handle(event)).resolves.toEqual({ ok: false, retry: true });
    expect(AlwaysThrowsProcessor.dispatchCalls).toBe(1);
    expect(pubsub.terminalPublishes).toBe(0);

    await mastra.shutdown();
  });

  it.each([-1, 1.5, Number.NaN, Number.POSITIVE_INFINITY])(
    'fails closed for invalid deliveryAttempt %s',
    async deliveryAttempt => {
      const pubsub = new FailingTerminalPubSub();
      const mastra = new Mastra({
        logger: false,
        storage: new MockStore(),
        workflows: { wf: makeWorkflow('wf') } as any,
        pubsub,
      });
      await persistRun(mastra, 'wf', 'invalid-attempt');
      AlwaysThrowsProcessor.dispatchCalls = 0;
      const processor = new AlwaysThrowsProcessor({ mastra });

      await expect(processor.handle(makeStartEvent('wf', 'invalid-attempt', deliveryAttempt))).resolves.toEqual({
        ok: false,
        retry: false,
      });
      expect(AlwaysThrowsProcessor.dispatchCalls).toBe(0);
      expect(pubsub.terminalPublishes).toBe(1);

      await mastra.shutdown();
    },
  );

  it.each([undefined, 0])('treats untracked deliveryAttempt %s as the first delivery', async deliveryAttempt => {
    const pubsub = new EventEmitterPubSub();
    const mastra = new Mastra({
      logger: false,
      storage: new MockStore(),
      workflows: { wf: makeWorkflow('wf') } as any,
      pubsub,
    });
    await persistRun(mastra, 'wf', 'untracked-attempt');
    AlwaysThrowsProcessor.dispatchCalls = 0;

    const processor = new AlwaysThrowsProcessor({ mastra });
    await expect(processor.handle(makeStartEvent('wf', 'untracked-attempt', deliveryAttempt))).resolves.toEqual({
      ok: false,
      retry: true,
    });
    expect(AlwaysThrowsProcessor.dispatchCalls).toBe(1);

    await mastra.shutdown();
  });

  it('does not overwrite a run that completed before a stale exhausted delivery', async () => {
    const pubsub = new FailingTerminalPubSub();
    const mastra = new Mastra({
      logger: false,
      storage: new MockStore(),
      workflows: { wf: makeWorkflow('wf') } as any,
      pubsub,
    });
    await persistRun(mastra, 'wf', 'completed-run', 'success');
    AlwaysThrowsProcessor.dispatchCalls = 0;

    const processor = new AlwaysThrowsProcessor({ mastra });
    await expect(processor.handle(makeStartEvent('wf', 'completed-run', 4))).resolves.toEqual({
      ok: false,
      retry: false,
    });
    expect(AlwaysThrowsProcessor.dispatchCalls).toBe(0);
    expect(pubsub.terminalPublishes).toBe(0);

    const snapshot = await (await mastra.getStorage()?.getStore('workflows'))?.loadWorkflowSnapshot({
      workflowName: 'wf',
      runId: 'completed-run',
    });
    expect(snapshot?.status).toBe('success');

    await mastra.shutdown();
  });

  it('keeps the event retryable when the terminalization state check is unavailable', async () => {
    const pubsub = new FailingTerminalPubSub();
    const mastra = new Mastra({
      logger: false,
      storage: new MockStore(),
      workflows: { wf: makeWorkflow('wf') } as any,
      pubsub,
    });
    const workflowsStore = await mastra.getStorage()?.getStore('workflows');
    vi.spyOn(workflowsStore!, 'loadWorkflowSnapshot').mockRejectedValueOnce(new Error('storage unavailable'));
    AlwaysThrowsProcessor.dispatchCalls = 0;

    const processor = new AlwaysThrowsProcessor({ mastra });
    await expect(processor.handle(makeStartEvent('wf', 'state-check-run', 4))).resolves.toEqual({
      ok: false,
      retry: true,
    });
    expect(AlwaysThrowsProcessor.dispatchCalls).toBe(0);
    expect(pubsub.terminalPublishes).toBe(0);

    await mastra.shutdown();
  });
});

describe('Mastra push workflow retry routing', () => {
  it('nacks retryable results and acks terminal results', async () => {
    const pubsub = new ManualPushPubSub();
    const mastra = new Mastra({ logger: false, storage: new MockStore(), workflows: {} as any, pubsub });
    await mastra.startWorkers();
    const ack = vi.fn(async () => {});
    const nack = vi.fn(async () => {});
    const event = makeStartEvent('wf', 'push-run', 1);

    vi.spyOn(mastra, 'handleWorkflowEvent').mockResolvedValueOnce({ ok: false, retry: true });
    pubsub.deliver(event, ack, nack);
    await vi.waitFor(() => expect(nack).toHaveBeenCalledTimes(1));
    expect(ack).not.toHaveBeenCalled();

    vi.mocked(mastra.handleWorkflowEvent).mockResolvedValueOnce({ ok: false, retry: false });
    pubsub.deliver(event, ack, nack);
    await vi.waitFor(() => expect(ack).toHaveBeenCalledTimes(1));
    expect(nack).toHaveBeenCalledTimes(1);

    await mastra.shutdown();
  });
});
