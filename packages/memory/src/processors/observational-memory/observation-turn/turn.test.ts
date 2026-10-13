import { APICallError } from '@internal/ai-sdk-v5';
import { MockLanguageModelV2 } from '@internal/ai-sdk-v5/test';
import { MessageList } from '@mastra/core/agent';
import { RequestContext } from '@mastra/core/di';
import { InMemoryDB, InMemoryMemory } from '@mastra/core/storage';
import { describe, expect, it, vi } from 'vitest';

import { OmModelExecutionError } from '../error';
import { ObservationalMemory } from '../observational-memory';
import { ObservationalMemoryProcessor } from '../processor';

describe('synchronous end-of-turn observation', () => {
  it.each([
    ['abort', 'turn end'],
    ['continue', 'turn end'],
    ['abort', 'MessageList replacement'],
    ['continue', 'MessageList replacement'],
    ['abort', 'state reset'],
    ['continue', 'state reset'],
  ] as const)(
    'honors the %s observer failure policy at %s without losing the reply',
    async (failurePolicy, boundary) => {
      const storage = new InMemoryMemory({ db: new InMemoryDB() });
      const threadId = `observer-${failurePolicy}`;
      const resourceId = 'observer-resource';
      const createdAt = new Date('2026-10-13T00:00:00Z');
      await storage.saveThread({
        thread: { id: threadId, resourceId, title: 'Observer failure', createdAt, updatedAt: createdAt },
      });
      const providerFailure = new APICallError({
        message: 'Observer credential rejected',
        url: 'https://observer.invalid',
        requestBodyValues: {},
        statusCode: 401,
        isRetryable: false,
      });
      const doStream = vi.fn(async () => {
        throw providerFailure;
      });
      const om = new ObservationalMemory({
        storage,
        scope: 'thread',
        observation: {
          model: new MockLanguageModelV2({ doStream }),
          messageTokens: 1,
          bufferTokens: false,
          maxRetries: 0,
          failurePolicy,
        },
      });
      const messageList = new MessageList({ threadId, resourceId });
      const processor = new ObservationalMemoryProcessor(om, {
        getContext: async () => ({
          systemMessage: undefined,
          messages: [],
          hasObservations: false,
          omRecord: null,
          continuationMessage: undefined,
          otherThreadsContext: undefined,
        }),
        persistMessages: async messages => {
          await storage.saveMessages({ messages });
        },
      });
      const requestContext = new RequestContext();
      requestContext.set('MastraMemory', { thread: { id: threadId }, resourceId });
      const state: Record<string, unknown> = {};
      const inputStep = (list: MessageList, stepState: Record<string, unknown>, stepNumber: number) =>
        processor.processInputStep({
          messageList: list,
          messages: list.get.all.db(),
          requestContext,
          stepNumber,
          state: stepState,
          steps: [],
          systemMessages: [],
          model: 'mock/actor',
          retryCount: 0,
          abort: reason => {
            throw new Error(reason);
          },
        });
      // Start below the threshold, then add the completed messages to the live turn.
      if (boundary !== 'turn end') {
        await inputStep(messageList, state, 0);
        expect(doStream).not.toHaveBeenCalled();
      }
      messageList.add(
        {
          id: 'user-message',
          threadId,
          resourceId,
          role: 'user',
          createdAt,
          content: { format: 2, parts: [{ type: 'text', text: 'Remember this important conversational detail.' }] },
        },
        'input',
      );
      messageList.add(
        {
          id: 'assistant-message',
          threadId,
          resourceId,
          role: 'assistant',
          createdAt,
          content: { format: 2, parts: [{ type: 'text', text: 'The reply is durable even when observation fails.' }] },
        },
        'response',
      );
      const nextMessageList =
        boundary === 'MessageList replacement' ? new MessageList({ threadId, resourceId }) : messageList;
      let completion;
      if (boundary === 'turn end') {
        const turn = om.beginTurn({ threadId, resourceId, messageList });
        await turn.start();
        completion = turn.end();
      } else {
        completion = inputStep(nextMessageList, boundary === 'state reset' ? {} : state, 1);
      }
      if (failurePolicy === 'abort') {
        await expect(completion).rejects.toBeInstanceOf(OmModelExecutionError);
      } else if (boundary === 'turn end') {
        await expect(completion).resolves.toHaveProperty('record');
      } else {
        await expect(completion).resolves.toBe(nextMessageList);
      }
      if (failurePolicy === 'abort' || boundary === 'turn end') {
        expect(doStream).toHaveBeenCalledOnce();
      } else {
        expect(doStream).toHaveBeenCalled();
      }
      const persisted = await storage.listMessages({ threadId, resourceId, perPage: false });
      expect(persisted.messages.map(message => message.id)).toEqual(['user-message', 'assistant-message']);
      expect(persisted.messages[1]?.content.parts.filter(part => part.type === 'text').map(part => part.text)).toEqual([
        'The reply is durable even when observation fails.',
      ]);
      expect(await storage.getObservationalMemory(threadId, resourceId)).toMatchObject({ activeObservations: '' });
    },
  );
});
