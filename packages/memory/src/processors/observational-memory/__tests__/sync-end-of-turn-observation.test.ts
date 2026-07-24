/**
 * Synchronous end-of-turn observation.
 *
 * Chat surfaces run one LLM step per turn, so the step > 0 threshold path
 * never fires for them. In synchronous mode (async buffering disabled) the
 * turn boundary is the only trigger: turn.end() must observe when the
 * threshold is crossed, otherwise the session drifts past the threshold
 * forever (live-diagnosed on the Doxa chat path: msgs 8603/150 with zero
 * observe calls before the fix).
 */
import { MockLanguageModelV2, convertArrayToReadableStream } from '@internal/ai-sdk-v5/test';
import type { MastraMessageContentV2 } from '@mastra/core/agent';
import { RequestContext } from '@mastra/core/request-context';
import { InMemoryMemory, InMemoryDB } from '@mastra/core/storage';
import { describe, it, expect } from 'vitest';

import { ObservationalMemory } from '../observational-memory';
import { ObservationalMemoryProcessor } from '../processor';
import type { MemoryContextProvider } from '../processor';

function createMemoryProvider(om: ObservationalMemory): MemoryContextProvider {
  return {
    getContext: async ({ threadId, resourceId }) => {
      const record = await om.getRecord(threadId, resourceId);
      const storage = (om as any).storage;
      const result = await storage.listMessages({
        threadId,
        orderBy: { field: 'createdAt', direction: 'ASC' },
        perPage: false,
      });
      return {
        systemMessage: undefined,
        messages: result.messages,
        hasObservations: !!record?.activeObservations,
        omRecord: record,
        continuationMessage: undefined,
        otherThreadsContext: undefined,
      };
    },
    persistMessages: async messages => {
      if (messages.length === 0) return;
      await (om as any).storage.saveMessages({ messages });
    },
  };
}

describe('synchronous end-of-turn observation', () => {
  it('observes at turn end when the threshold is crossed on a single-step turn', async () => {
    const { MessageList } = await import('@mastra/core/agent');
    const storage = new InMemoryMemory({ db: new InMemoryDB() });
    const threadId = 'sync-turn-thread';
    const resourceId = 'sync-turn-resource';

    let observerCalls = 0;
    const observerModel = new MockLanguageModelV2({
      doStream: async () => {
        observerCalls += 1;
        return {
          stream: convertArrayToReadableStream([
            { type: 'stream-start', warnings: [] },
            { type: 'response-metadata', id: `obs-${observerCalls}`, modelId: 'mock-observer', timestamp: new Date() },
            { type: 'text-start', id: 'text-1' },
            {
              type: 'text-delta',
              id: 'text-1',
              delta: '<observations>\n- 🔴 User codename is ZEPHYR-9\n</observations>',
            },
            { type: 'text-end', id: 'text-1' },
            { type: 'finish', finishReason: 'stop', usage: { inputTokens: 100, outputTokens: 40, totalTokens: 140 } },
          ]),
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
        };
      },
    });

    const om = new ObservationalMemory({
      storage: storage as any,
      scope: 'thread',
      observation: {
        model: observerModel as any,
        messageTokens: 100,
        bufferTokens: false,
      },
      reflection: {
        model: observerModel as any,
        observationTokens: 50_000,
      },
    });

    await storage.saveThread({
      thread: {
        id: threadId,
        resourceId,
        title: 'Sync observation',
        createdAt: new Date('2025-01-01T08:00:00Z'),
        updatedAt: new Date('2025-01-01T08:00:00Z'),
        metadata: {},
      },
    });

    const inputProcessor = new ObservationalMemoryProcessor(om, createMemoryProvider(om));
    const outputProcessor = new ObservationalMemoryProcessor(om, createMemoryProvider(om));
    const state: Record<string, unknown> = {};
    const abort = (() => {
      throw new Error('aborted');
    }) as any;
    const requestContext = new RequestContext();
    requestContext.set('MastraMemory', { thread: { id: threadId }, resourceId });

    const messageList = new MessageList({ threadId, resourceId });
    const filler = Array.from({ length: 120 }, (_, index) => `token${index}`).join(' ');
    messageList.add(
      {
        id: 'user-1',
        role: 'user',
        content: {
          format: 2,
          parts: [{ type: 'text', text: `My codename is ZEPHYR-9. ${filler}` }],
        } as MastraMessageContentV2,
        createdAt: new Date('2025-01-01T09:00:00Z'),
        threadId,
        resourceId,
      } as any,
      'input',
    );

    await inputProcessor.processInputStep({
      messageList,
      messages: [],
      requestContext,
      stepNumber: 0,
      state,
      steps: [],
      systemMessages: [],
      model: observerModel as any,
      retryCount: 0,
      abort,
      writer: { custom: async () => {} } as any,
    });

    messageList.add(
      {
        id: 'assistant-1',
        role: 'assistant',
        content: { format: 2, parts: [{ type: 'text', text: 'OK.' }] } as MastraMessageContentV2,
        createdAt: new Date('2025-01-01T09:00:01Z'),
        threadId,
        resourceId,
      } as any,
      'response',
    );

    // Single-step turn: no step > 0 ever runs. Before the turn-boundary
    // trigger existed, this produced NO observation no matter how far past
    // the threshold the turn was.
    await outputProcessor.processOutputResult({
      messageList,
      messages: messageList.get.response.db(),
      requestContext,
      state,
      abort,
      result: {} as any,
      retryCount: 0,
    });

    expect(observerCalls).toBeGreaterThan(0);
    const record = await om.getRecord(threadId, resourceId);
    expect(record?.activeObservations ?? '').toContain('ZEPHYR-9');
    expect(record?.lastObservedAt, 'observation cursor did not advance').toBeTruthy();
  });

  it('does not observe at turn end while under the threshold', async () => {
    const { MessageList } = await import('@mastra/core/agent');
    const storage = new InMemoryMemory({ db: new InMemoryDB() });
    const threadId = 'sync-turn-under';
    const resourceId = 'sync-turn-under-resource';

    let observerCalls = 0;
    const observerModel = new MockLanguageModelV2({
      doStream: async () => {
        observerCalls += 1;
        return {
          stream: convertArrayToReadableStream([
            { type: 'stream-start', warnings: [] },
            { type: 'response-metadata', id: 'obs-1', modelId: 'mock-observer', timestamp: new Date() },
            { type: 'text-start', id: 'text-1' },
            { type: 'text-delta', id: 'text-1', delta: '<observations>\n- noise\n</observations>' },
            { type: 'text-end', id: 'text-1' },
            { type: 'finish', finishReason: 'stop', usage: { inputTokens: 10, outputTokens: 10, totalTokens: 20 } },
          ]),
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
        };
      },
    });

    const om = new ObservationalMemory({
      storage: storage as any,
      scope: 'thread',
      observation: {
        model: observerModel as any,
        messageTokens: 10_000,
        bufferTokens: false,
      },
      reflection: {
        model: observerModel as any,
        observationTokens: 50_000,
      },
    });

    await storage.saveThread({
      thread: {
        id: threadId,
        resourceId,
        title: 'Under threshold',
        createdAt: new Date('2025-01-01T08:00:00Z'),
        updatedAt: new Date('2025-01-01T08:00:00Z'),
        metadata: {},
      },
    });

    const inputProcessor = new ObservationalMemoryProcessor(om, createMemoryProvider(om));
    const outputProcessor = new ObservationalMemoryProcessor(om, createMemoryProvider(om));
    const state: Record<string, unknown> = {};
    const abort = (() => {
      throw new Error('aborted');
    }) as any;
    const requestContext = new RequestContext();
    requestContext.set('MastraMemory', { thread: { id: threadId }, resourceId });

    const messageList = new MessageList({ threadId, resourceId });
    messageList.add(
      {
        id: 'user-1',
        role: 'user',
        content: { format: 2, parts: [{ type: 'text', text: 'Short hello.' }] } as MastraMessageContentV2,
        createdAt: new Date('2025-01-01T09:00:00Z'),
        threadId,
        resourceId,
      } as any,
      'input',
    );

    await inputProcessor.processInputStep({
      messageList,
      messages: [],
      requestContext,
      stepNumber: 0,
      state,
      steps: [],
      systemMessages: [],
      model: observerModel as any,
      retryCount: 0,
      abort,
      writer: { custom: async () => {} } as any,
    });

    messageList.add(
      {
        id: 'assistant-1',
        role: 'assistant',
        content: { format: 2, parts: [{ type: 'text', text: 'Hi.' }] } as MastraMessageContentV2,
        createdAt: new Date('2025-01-01T09:00:01Z'),
        threadId,
        resourceId,
      } as any,
      'response',
    );

    await outputProcessor.processOutputResult({
      messageList,
      messages: messageList.get.response.db(),
      requestContext,
      state,
      abort,
      result: {} as any,
      retryCount: 0,
    });

    expect(observerCalls).toBe(0);
    const record = await om.getRecord(threadId, resourceId);
    expect(record?.activeObservations ?? '').toBe('');
  });
});
