/**
 * Reproduction test for mc "send message does nothing" bug.
 * Tests the complete flow: dynamic model + controller + evented workflow engine.
 *
 * Root cause: PR #17534 (e9cf1743) removed currentModelId/modeId from the state
 * schema, so Zod stripped them during setState — getDynamicModel then threw
 * "No model selected" which was silently swallowed by the idle-start .catch()
 * in thread-stream-runtime.ts.
 *
 * Fix 1 (PR #17676): Restored currentModelId/modeId to stateSchema.
 * Fix 2 (PR #17676 era): idle-start errors were meant to reach the subscription
 *         stream via `run-failed` so the controller surfaces an error event.
 *         That half does NOT currently work — the terminal is published after
 *         the adopted reservation's lease is released, so the subscriber cannot
 *         authenticate it and drops it. Tracked as PF-3393. The error is still
 *         surfaced to the caller: `sendMessage()` rejects.
 */
import { MockLanguageModelV2, convertArrayToReadableStream } from '@internal/ai-sdk-v5/test';
import { describe, expect, it } from 'vitest';
import { z } from 'zod/v4';

import { Agent } from '../agent';
import { EventEmitterPubSub } from '../events/event-emitter';
import type { PubSubDeliveryMode } from '../events/pubsub';
import type { RequestContext } from '../request-context';
import { InMemoryStore } from '../storage/mock';
import { AgentController } from './agent-controller';
import { createMockWorkspace } from './test-utils';
import type { AgentControllerEvent } from './types';

/** Push-only wrapper around EventEmitterPubSub — mimics mc's SignalsPubSub. */
class PushOnlyPubSub extends EventEmitterPubSub {
  override get supportedModes(): ReadonlyArray<PubSubDeliveryMode> {
    return ['push'];
  }
}

function createTextStreamModel(responseText: string) {
  return new MockLanguageModelV2({
    doStream: async () => ({
      rawCall: { rawPrompt: null, rawSettings: {} },
      warnings: [],
      stream: convertArrayToReadableStream([
        { type: 'stream-start', warnings: [] },
        { type: 'response-metadata', id: 'id-0', modelId: 'mock-model-id', timestamp: new Date(0) },
        { type: 'text-start', id: 'text-1' },
        { type: 'text-delta', id: 'text-1', delta: responseText },
        { type: 'text-end', id: 'text-1' },
        {
          type: 'finish',
          finishReason: 'stop',
          usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
        },
      ]),
    }),
  });
}

describe('mc send-message reproduction', () => {
  it('produces assistant response with dynamic model + init + startWorkers', async () => {
    const storage = new InMemoryStore();

    function getDynamicModel({ requestContext }: { requestContext: RequestContext }) {
      const controllerContext = requestContext.get('controller') as any;
      const modelId = controllerContext?.session?.modelId;
      if (!modelId) {
        throw new Error('No model selected');
      }
      return createTextStreamModel('Hello from the agent!');
    }

    const stateSchema = z.object({
      currentModelId: z.string().optional(),
      modeId: z.string().optional(),
    });

    const agent = new Agent({
      id: 'test-agent',
      name: 'test-agent',
      model: getDynamicModel as any,
      instructions: 'You are a test agent.',
    });

    const controller = new AgentController({
      workspace: createMockWorkspace(),
      id: 'test-controller',
      storage,
      resourceId: 'test-resource',
      modes: [{ id: 'build', agent, defaultModelId: 'anthropic/claude-opus-4-7' }],
      defaultModeId: 'build',
      stateSchema,
    });

    await controller.init();
    const session = await controller.createSession({ id: 'test-session', ownerId: 'test-owner' });
    await controller.getMastra()?.startWorkers();
    await session.thread.create();

    const events: AgentControllerEvent[] = [];
    session.subscribe((event: AgentControllerEvent) => {
      events.push(event);
    });

    expect(session.model.get()).toBe('anthropic/claude-opus-4-7');

    await session.sendMessage({ content: 'Hello!' });

    const assistantEnd = events.find(
      (e): e is Extract<AgentControllerEvent, { type: 'message_end' }> =>
        e.type === 'message_end' && e.message.role === 'assistant',
    );
    expect(assistantEnd).toBeDefined();
    expect(assistantEnd!.message.content.parts).toEqual([{ type: 'text', text: 'Hello from the agent!' }]);
  }, 30000);

  it('rejects when model function throws during idle-start', async () => {
    const storage = new InMemoryStore();

    function throwingModel() {
      throw new Error('No model selected');
    }

    const agent = new Agent({
      id: 'test-agent',
      name: 'test-agent',
      model: throwingModel as any,
      instructions: 'You are a test agent.',
    });

    const controller = new AgentController({
      workspace: createMockWorkspace(),
      id: 'test-controller',
      storage,
      resourceId: 'test-resource',
      modes: [{ id: 'build', agent, defaultModelId: 'mock-model' }],
      defaultModeId: 'build',
    });

    await controller.init();
    const session = await controller.createSession({ id: 'test-session', ownerId: 'test-owner' });
    await controller.getMastra()?.startWorkers();
    await session.thread.create();

    const events: AgentControllerEvent[] = [];
    session.subscribe((event: AgentControllerEvent) => {
      events.push(event);
    });

    // `sendMessage()` rejects, and that rejection is the contract other code depends on:
    // `accepted` rejects deliberately for routing and startup failures including FGA
    // denial, `drainFollowUpQueue()` relies on it to requeue a dequeued follow-up rather
    // than drop it, and `__tests__/signal-messages.test.ts` pins it directly. Swallowing
    // it would turn an authorization denial into a resolved call and lose queued work.
    await expect(session.sendMessage({ content: 'Hello!' })).rejects.toThrow('No model selected');

    // NOTE: a subscriber currently does NOT also receive an `error` event for this
    // pre-registration failure, and this test deliberately does not assert that it does —
    // nor does it assert the absence, which would codify the bug.
    //
    // Cause (PF-3393): `Agent.stream()` adopts the caller's `_threadRunReservationOwner`
    // reservation and releases it — along with its lease — before the idle-wake catch in
    // thread-stream-runtime publishes `run-failed`. That terminal carries no `streamId`,
    // so the subscriber authenticates it against the live lease, finds none, and drops it.
    // The conversion path is correct but unreachable. Fixing it means changing reservation
    // cleanup ownership for every externally reserved start, which is concurrency-sensitive
    // and does not belong in an upstream-sync merge.
    void events;
  }, 30000);

  it('produces assistant response with push-only pubsub (like mc SignalsPubSub)', async () => {
    const storage = new InMemoryStore();
    const pushOnlyPubSub = new PushOnlyPubSub();

    function getDynamicModel({ requestContext }: { requestContext: RequestContext }) {
      const controllerContext = requestContext.get('controller') as any;
      const modelId = controllerContext?.session?.modelId;
      if (!modelId) {
        throw new Error('No model selected');
      }
      return createTextStreamModel('Hello from push-only!');
    }

    const stateSchema = z.object({
      currentModelId: z.string().optional(),
      modeId: z.string().optional(),
    });

    const agent = new Agent({
      id: 'test-agent',
      name: 'test-agent',
      model: getDynamicModel as any,
      instructions: 'You are a test agent.',
    });

    const controller = new AgentController({
      workspace: createMockWorkspace(),
      id: 'test-controller',
      storage,
      pubsub: pushOnlyPubSub,
      resourceId: 'test-resource',
      modes: [{ id: 'build', agent, defaultModelId: 'anthropic/claude-opus-4-7' }],
      defaultModeId: 'build',
      stateSchema,
    });

    await controller.init();
    const session = await controller.createSession({ id: 'test-session', ownerId: 'test-owner' });
    await controller.getMastra()?.startWorkers();
    await session.thread.create();

    const events: AgentControllerEvent[] = [];
    session.subscribe((event: AgentControllerEvent) => {
      events.push(event);
    });

    expect(session.model.get()).toBe('anthropic/claude-opus-4-7');

    await session.sendMessage({ content: 'Hello!' });

    const assistantEnd = events.find(
      (e): e is Extract<AgentControllerEvent, { type: 'message_end' }> =>
        e.type === 'message_end' && e.message.role === 'assistant',
    );
    expect(assistantEnd).toBeDefined();
    expect(assistantEnd!.message.content.parts).toEqual([{ type: 'text', text: 'Hello from push-only!' }]);
  }, 30000);
});
