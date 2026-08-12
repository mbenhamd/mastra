import { afterEach, describe, expect, it, vi } from 'vitest';
import { EventEmitterPubSub } from '../../../../events/event-emitter';
import { PUBSUB_SYMBOL } from '../../../../workflows/constants';
import { globalRunRegistry } from '../../run-registry';
import { createDurableSignalDrainStep } from './signal-drain';

vi.mock('../../../../workflows', () => ({
  createStep: (config: unknown) => config,
}));

const RUN_ID = 'signal-drain-remote-abort';

afterEach(() => {
  globalRunRegistry.delete(RUN_ID);
});

describe('createDurableSignalDrainStep', () => {
  it('installs the replayed abort listener before reading the run registry', async () => {
    const pubsub = new EventEmitterPubSub();
    const input = { stepResult: { isContinued: false } };
    globalRunRegistry.set(RUN_ID, {
      isPlaceholder: true,
      runtimeBindingId: 'signal-drain-binding',
    } as any);

    try {
      const step = createDurableSignalDrainStep();
      const result = await (step as any).execute({
        inputData: input,
        getInitData: () => ({ runId: RUN_ID, runtimeBindingId: 'signal-drain-binding' }),
        [PUBSUB_SYMBOL]: pubsub,
      });

      expect(result).toBe(input);
      expect(globalRunRegistry.get(RUN_ID)?.remoteAbortListenerInstalled).toBe(true);
    } finally {
      globalRunRegistry.delete(RUN_ID);
      await pubsub.close();
    }
  });

  it('revalidates the binding after listener setup before draining signals', async () => {
    let releaseSubscription!: () => void;
    let markSubscriptionStarted!: () => void;
    const subscriptionStarted = new Promise<void>(resolve => {
      markSubscriptionStarted = resolve;
    });
    const subscriptionReleased = new Promise<void>(resolve => {
      releaseSubscription = resolve;
    });
    const pubsub = {
      subscribeWithReplay: vi.fn(async () => {
        markSubscriptionStarted();
        await subscriptionReleased;
      }),
      unsubscribe: vi.fn().mockResolvedValue(undefined),
    };
    const newDrain = vi.fn(() => []);
    globalRunRegistry.set(RUN_ID, {
      runtimeBindingId: 'old-binding',
      tools: {},
      model: {} as any,
      drainPendingSignals: vi.fn(() => []),
    } as any);

    const step = createDurableSignalDrainStep();
    const execution = (step as any).execute({
      inputData: { stepResult: { isContinued: false } },
      getInitData: () => ({ runId: RUN_ID, runtimeBindingId: 'old-binding' }),
      [PUBSUB_SYMBOL]: pubsub,
    });
    await subscriptionStarted;
    globalRunRegistry.set(RUN_ID, {
      runtimeBindingId: 'new-binding',
      tools: {},
      model: {} as any,
      drainPendingSignals: newDrain,
    } as any);
    releaseSubscription();

    await expect(execution).rejects.toThrow(/no longer matches its registered runtime dependencies/);
    expect(newDrain).not.toHaveBeenCalled();
  });

  it('rejects a stale runtime binding before reading the reused run entry', async () => {
    const pubsub = new EventEmitterPubSub();
    globalRunRegistry.set(RUN_ID, {
      runtimeBindingId: 'new-binding',
      tools: {},
      model: {} as any,
    } as any);

    try {
      const step = createDurableSignalDrainStep();
      await expect(
        (step as any).execute({
          inputData: { stepResult: { isContinued: false } },
          getInitData: () => ({ runId: RUN_ID, runtimeBindingId: 'stale-binding' }),
          [PUBSUB_SYMBOL]: pubsub,
        }),
      ).rejects.toThrow(/no longer matches its registered runtime dependencies/);
      expect(globalRunRegistry.get(RUN_ID)?.remoteAbortListenerInstalled).not.toBe(true);
    } finally {
      globalRunRegistry.delete(RUN_ID);
      await pubsub.close();
    }
  });
});
