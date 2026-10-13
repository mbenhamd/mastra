import { APICallError } from '@internal/ai-sdk-v5';
import { MockLanguageModelV2 } from '@internal/ai-sdk-v5/test';
import { MessageList } from '@mastra/core/agent';
import { InMemoryDB, InMemoryMemory } from '@mastra/core/storage';
import { describe, expect, it, vi } from 'vitest';

import { OmModelExecutionError } from '../error';
import { ObservationalMemory } from '../observational-memory';

describe('synchronous end-of-turn observation', () => {
  it.each(['abort', 'continue'] as const)(
    'honors the %s observer failure policy without losing the reply',
    async failurePolicy => {
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
      const turn = om.beginTurn({ threadId, resourceId, messageList });
      await turn.start();
      if (failurePolicy === 'abort') {
        await expect(turn.end()).rejects.toBeInstanceOf(OmModelExecutionError);
      } else {
        await expect(turn.end()).resolves.toHaveProperty('record');
      }
      expect(doStream).toHaveBeenCalledOnce();
      const persisted = await storage.listMessages({ threadId, resourceId, perPage: false });
      expect(persisted.messages.map(message => message.id)).toEqual(['user-message', 'assistant-message']);
      expect(await storage.getObservationalMemory(threadId, resourceId)).toMatchObject({ activeObservations: '' });
    },
  );
});
