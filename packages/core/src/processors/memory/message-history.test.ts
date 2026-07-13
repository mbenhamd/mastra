import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { MastraDBMessage } from '../../agent';
import { MessageList } from '../../agent';
import type { MemoryRuntimeContext } from '../../memory';
import { RequestContext } from '../../request-context';
import { MemoryStorage } from '../../storage';
import type { StorageListThreadsInput, StorageListThreadsOutput } from '../../storage/types';

import { MessageHistory } from './message-history.js';

// Helper to create RequestContext with memory context
function createRuntimeContextWithMemory(threadId: string, resourceId?: string): RequestContext {
  const requestContext = new RequestContext();
  const memoryContext: MemoryRuntimeContext = {
    thread: { id: threadId },
    resourceId,
  };
  requestContext.set('MastraMemory', memoryContext);
  return requestContext;
}

// Mock storage implementation
class MockStorage extends MemoryStorage {
  private messages: MastraDBMessage[] = [];

  async listMessages(params: any): Promise<any> {
    const { threadId, perPage = false, page = 1, orderBy } = params;
    const threadMessages = this.messages.filter(m => m.threadId === threadId);

    // Sort by createdAt if orderBy is specified
    let sortedMessages = threadMessages;
    if (orderBy?.field === 'createdAt') {
      sortedMessages = [...threadMessages].sort((a, b) => {
        const aTime = a.createdAt instanceof Date ? a.createdAt.getTime() : new Date(a.createdAt).getTime();
        const bTime = b.createdAt instanceof Date ? b.createdAt.getTime() : new Date(b.createdAt).getTime();
        return orderBy.direction === 'DESC' ? bTime - aTime : aTime - bTime;
      });
    }

    let resultMessages = sortedMessages;
    if (typeof perPage === 'number' && perPage > 0) {
      resultMessages = sortedMessages.slice(0, perPage);
    }

    return {
      messages: resultMessages,
      total: threadMessages.length,
      page,
      perPage,
      hasMore: false,
    };
  }

  async listMessagesById({ messageIds }: { messageIds: string[] }): Promise<{ messages: MastraDBMessage[] }> {
    return { messages: this.messages.filter(m => m.id && messageIds.includes(m.id)) };
  }

  setMessages(messages: MastraDBMessage[]) {
    this.messages = messages;
  }

  // Implement other required abstract methods with stubs
  async getThreadById(_args: { threadId: string }) {
    return null;
  }
  async saveThread(args: any) {
    return args.thread || args;
  }
  async updateThread(args: { id: string; title: string; metadata: Record<string, unknown> }) {
    return {
      id: args.id,
      resourceId: 'resource-1',
      title: args.title,
      metadata: args.metadata,
      createdAt: new Date(),
      updatedAt: new Date(),
    };
  }
  async deleteThread(_args: { threadId: string }) {}
  async saveMessages(args: { messages: MastraDBMessage[] }) {
    return { messages: args.messages };
  }
  async updateMessages(args: any) {
    return args.messages || [];
  }
  async listThreads(args: StorageListThreadsInput): Promise<StorageListThreadsOutput> {
    return {
      threads: [],
      total: 0,
      page: args.page ?? 0,
      perPage: args.perPage ?? 100,
      hasMore: false,
    };
  }
}

describe('MessageHistory', () => {
  let mockStorage: MockStorage;
  let processor: MessageHistory;
  const mockAbort = vi.fn(() => {
    throw new Error('Aborted');
  }) as any;

  beforeEach(() => {
    mockStorage = new MockStorage();
    vi.clearAllMocks();
  });

  describe('processInput', () => {
    it('should fetch last N messages from storage', async () => {
      const historicalMessages: MastraDBMessage[] = [
        {
          id: 'msg-1',
          role: 'user',
          content: { format: 2, parts: [{ type: 'text', text: 'Hello' }] },
          threadId: 'thread-1',
          createdAt: new Date(Date.now() - 3000), // 3 seconds ago
        },
        {
          id: 'msg-2',
          role: 'assistant',
          content: { format: 2, parts: [{ type: 'text', text: 'Hi there!' }] },
          threadId: 'thread-1',
          createdAt: new Date(Date.now() - 2000), // 2 seconds ago
        },
        {
          id: 'msg-3',
          role: 'user',
          content: { format: 2, parts: [{ type: 'text', text: 'How are you?' }] },
          threadId: 'thread-1',
          createdAt: new Date(Date.now() - 1000), // 1 second ago
        },
      ];

      mockStorage.setMessages(historicalMessages);

      processor = new MessageHistory({
        storage: mockStorage,
        lastMessages: 2,
      });

      const newMessages: MastraDBMessage[] = [
        {
          id: 'msg-4',
          role: 'user',
          content: { format: 2, content: 'New message', parts: [{ type: 'text', text: 'New message' }] },
          threadId: 'thread-1',
          createdAt: new Date(),
        },
      ];

      const requestContext = createRuntimeContextWithMemory('thread-1');
      const messageList = new MessageList();
      messageList.add(newMessages, 'input');

      const result = await processor.processInput({
        messages: newMessages,
        messageList,
        abort: mockAbort,
        requestContext,
      });

      // Should have last 2 historical messages + 1 new message
      const resultMessages = result instanceof MessageList ? result.get.all.db() : result;
      expect(resultMessages).toHaveLength(3);
      expect(resultMessages[0].id).toBe('msg-2');
      expect(resultMessages[1].id).toBe('msg-3');
      expect(resultMessages[2].id).toBe('msg-4');
    });

    it('should merge historical messages with new messages', async () => {
      const historicalMessages: MastraDBMessage[] = [
        {
          id: 'msg-1',
          role: 'user',
          content: { format: 2, content: 'Historical', parts: [{ type: 'text', text: 'Historical' }] },
          threadId: 'thread-1',
          createdAt: new Date(Date.now() - 10000), // 10 seconds ago
        },
      ];

      mockStorage.setMessages(historicalMessages);

      processor = new MessageHistory({
        storage: mockStorage,
      });

      const newMessages: MastraDBMessage[] = [
        {
          id: 'msg-2',
          role: 'user',
          content: { format: 2, content: 'New', parts: [{ type: 'text', text: 'New' }] },
          threadId: 'thread-1',
          createdAt: new Date(), // now
        },
      ];

      const messageList = new MessageList();
      messageList.add(newMessages, 'input');

      const result = await processor.processInput({
        messages: newMessages,
        messageList,
        abort: mockAbort,
        requestContext: createRuntimeContextWithMemory('thread-1'),
      });

      const resultMessages = result instanceof MessageList ? result.get.all.db() : result;
      expect(resultMessages).toHaveLength(2);
      expect(resultMessages[0].content.content).toBe('Historical');
      expect(resultMessages[1].content.content).toBe('New');
    });

    it('should avoid duplicate message IDs', async () => {
      const baseTime = Date.now();
      const historicalMessages: MastraDBMessage[] = [
        {
          id: 'msg-1',
          role: 'user',
          content: { format: 2, content: 'Message 1', parts: [{ type: 'text', text: 'Message 1' }] },
          threadId: 'thread-1',
          createdAt: new Date(baseTime - 3000), // 3 seconds ago
        },
        {
          id: 'msg-2',
          role: 'assistant',
          content: { format: 2, content: 'Message 2', parts: [{ type: 'text', text: 'Message 2' }] },
          threadId: 'thread-1',
          createdAt: new Date(baseTime - 2000), // 2 seconds ago
        },
      ];

      mockStorage.setMessages(historicalMessages);

      processor = new MessageHistory({
        storage: mockStorage,
      });

      const newMessages: MastraDBMessage[] = [
        {
          id: 'msg-2', // Duplicate ID
          role: 'assistant',
          content: { format: 2, content: 'Message 2 (new)', parts: [{ type: 'text', text: 'Message 2 (new)' }] },
          threadId: 'thread-1',
          createdAt: new Date(baseTime - 1000), // 1 second ago
        },
        {
          id: 'msg-3',
          role: 'user',
          content: { format: 2, content: 'Message 3', parts: [{ type: 'text', text: 'Message 3' }] },
          threadId: 'thread-1',
          createdAt: new Date(baseTime), // now
        },
      ];

      const messageList = new MessageList();
      messageList.add(newMessages, 'input');

      const result = await processor.processInput({
        messages: newMessages,
        messageList,
        abort: mockAbort,
        requestContext: createRuntimeContextWithMemory('thread-1'),
      });

      const resultMessages = result instanceof MessageList ? result.get.all.db() : result;
      // msg-1 from history, msg-2 from new (duplicate filtered), msg-3 from new
      expect(resultMessages).toHaveLength(3);
      expect(resultMessages[0].id).toBe('msg-1');
      expect(resultMessages[1].id).toBe('msg-2');
      expect(resultMessages[1].content.content).toBe('Message 2 (new)'); // New version kept
      expect(resultMessages[2].id).toBe('msg-3');
    });

    it('should handle empty storage', async () => {
      processor = new MessageHistory({
        storage: mockStorage,
      });

      const newMessages: MastraDBMessage[] = [
        {
          id: 'msg-1',
          role: 'user',
          content: { format: 2, content: 'New', parts: [{ type: 'text', text: 'New' }] },
          threadId: 'thread-1',
          createdAt: new Date(),
        },
      ];

      const messageList = new MessageList();
      messageList.add(newMessages, 'input');

      const result = await processor.processInput({
        messages: newMessages,
        messageList,
        abort: mockAbort,
        requestContext: createRuntimeContextWithMemory('thread-1'),
      });

      const resultMessages = result instanceof MessageList ? result.get.all.db() : result;
      expect(resultMessages).toHaveLength(1);
      expect(resultMessages[0].id).toBe('msg-1');
    });

    it('should propagate storage errors', async () => {
      const errorStorage = new MockStorage();
      errorStorage.listMessages = vi.fn().mockRejectedValue(new Error('Storage error'));

      processor = new MessageHistory({
        storage: errorStorage,
      });

      const newMessages: MastraDBMessage[] = [
        {
          id: 'msg-1',
          role: 'user',
          content: { format: 2, parts: [{ type: 'text', text: 'New' }] },
          threadId: 'thread-1',
          createdAt: new Date(),
        },
      ];

      const messageList = new MessageList();
      messageList.add(newMessages, 'input');

      // Should propagate the error instead of silently failing
      await expect(
        processor.processInput({
          messages: newMessages,
          messageList,
          abort: mockAbort,
          requestContext: createRuntimeContextWithMemory('thread-1'),
        }),
      ).rejects.toThrow('Storage error');
    });

    it('should return original messages when no threadId', async () => {
      processor = new MessageHistory({
        storage: mockStorage,
        // No threadId
      });

      const newMessages: MastraDBMessage[] = [
        {
          id: 'msg-1',
          role: 'user',
          content: { format: 2, content: 'New', parts: [{ type: 'text', text: 'New' }] },
          threadId: 'thread-1',
          createdAt: new Date(),
        },
      ];

      const messageList = new MessageList();
      messageList.add(newMessages, 'input');

      // Don't pass requestContext to simulate no threadId
      const result = await processor.processInput({
        messages: newMessages,
        messageList,
        abort: mockAbort,
      });

      const resultMessages = result instanceof MessageList ? result.get.all.db() : result;
      expect(resultMessages).toEqual(newMessages);
    });

    it('should handle assistant messages with tool calls', async () => {
      const historicalMessages: MastraDBMessage[] = [
        {
          id: 'msg-1',
          role: 'assistant' as const,
          content: {
            format: 2,
            parts: [
              { type: 'text', text: 'Let me calculate that' },
              {
                type: 'tool-invocation',
                toolInvocation: {
                  state: 'call',
                  toolCallId: 'call-1',
                  toolName: 'calculator',
                  args: { a: 1, b: 2 },
                },
              },
            ],
          },
          threadId: 'thread-1',
          createdAt: new Date(),
        },
      ];

      mockStorage.setMessages(historicalMessages);

      processor = new MessageHistory({
        storage: mockStorage,
      });

      const messageList1 = new MessageList();

      const result = await processor.processInput({
        messages: [],
        messageList: messageList1,
        abort: mockAbort,
        requestContext: createRuntimeContextWithMemory('thread-1'),
      });

      const resultMessages = result instanceof MessageList ? result.get.all.db() : result;
      expect(resultMessages).toHaveLength(1);
      expect(resultMessages[0].role).toBe('assistant');
      expect(resultMessages[0].content.parts).toHaveLength(2);
      expect(resultMessages[0].content.parts?.[1].type).toBe('tool-invocation');
    });

    it('should handle tool result messages', async () => {
      const historicalMessages: MastraDBMessage[] = [
        {
          id: 'msg-1',
          role: 'assistant' as const,
          content: {
            format: 2,
            parts: [
              {
                type: 'tool-invocation',
                toolInvocation: {
                  state: 'result',
                  toolCallId: 'call-1',
                  toolName: 'calculator',
                  args: {},
                  result: { result: 3 },
                },
              },
            ],
          },
          threadId: 'thread-1',
          createdAt: new Date(),
        },
      ];

      mockStorage.setMessages(historicalMessages);

      processor = new MessageHistory({
        storage: mockStorage,
      });

      const messageList2 = new MessageList();

      const result = await processor.processInput({
        messages: [],
        messageList: messageList2,
        abort: mockAbort,
        requestContext: createRuntimeContextWithMemory('thread-1'),
      });

      const resultMessages = result instanceof MessageList ? result.get.all.db() : result;
      expect(resultMessages).toHaveLength(1);
      expect(resultMessages[0].role).toBe('assistant');
      expect(resultMessages[0].content.parts?.[0].type).toBe('tool-invocation');
    });
  });

  describe('processOutputResult', () => {
    it('should save user, assistant, and tool messages', async () => {
      const mockStorage = {
        saveMessages: vi.fn().mockResolvedValue(undefined),
        getThreadById: vi.fn().mockResolvedValue({
          id: 'thread-1',
          title: 'Test Thread',
          metadata: {},
        }),
        listMessages: vi.fn().mockResolvedValue({ messages: [], total: 0 }),
        updateThread: vi.fn().mockResolvedValue(undefined),
      } as unknown as MemoryStorage;

      const processor = new MessageHistory({
        storage: mockStorage,
      });

      const messages: MastraDBMessage[] = [
        {
          role: 'user',
          content: { format: 2, parts: [{ type: 'text', text: 'Hello' }] },
          id: 'msg-1',
          createdAt: new Date('2024-01-01T00:00:01Z'),
        },
        {
          role: 'assistant',
          content: {
            format: 2,
            parts: [
              { type: 'text', text: 'Hi there!' },
              {
                type: 'tool-invocation',
                toolInvocation: {
                  state: 'result',
                  toolCallId: 'tool-1',
                  toolName: 'search',
                  args: {},
                  result: 'Tool result',
                },
              },
            ],
          },
          id: 'msg-2',
          createdAt: new Date('2024-01-01T00:00:02Z'),
        },
      ];

      const messageList = new MessageList().add(messages, `response`).addSystem({
        role: 'system',
        content: 'You are a helpful assistant',
        id: 'msg-0',
        createdAt: new Date('2024-01-01T00:00:00Z'),
      });
      const result = await processor.processOutputResult({
        messageList,
        messages,
        abort: ((reason?: string) => {
          throw new Error(reason || 'Aborted');
        }) as (reason?: string) => never,
        requestContext: createRuntimeContextWithMemory('thread-1'),
      });

      expect(result.get.response.db()).toEqual(messages);
      expect(mockStorage.saveMessages).toHaveBeenCalledWith({
        messages: expect.arrayContaining([
          expect.objectContaining({
            id: 'msg-1',
            role: 'user',
            content: expect.objectContaining({
              format: 2,
              parts: expect.arrayContaining([expect.objectContaining({ type: 'text', text: 'Hello' })]),
            }),
            createdAt: expect.any(Date),
          }),
          expect.objectContaining({
            id: 'msg-2',
            role: 'assistant',
            content: expect.objectContaining({
              format: 2,
              parts: expect.arrayContaining([
                expect.objectContaining({ type: 'text', text: 'Hi there!' }),
                expect.objectContaining({
                  type: 'tool-invocation',
                  toolInvocation: expect.objectContaining({
                    state: 'result',
                  }),
                }),
              ]),
            }),
            createdAt: expect.any(Date),
          }),
        ]),
      });
      // System message should NOT be saved
      expect(mockStorage.saveMessages).toHaveBeenCalledWith({
        messages: expect.not.arrayContaining([expect.objectContaining({ role: 'system' })]),
      });
    });

    it('should filter out ONLY system messages', async () => {
      const mockStorage = {
        saveMessages: vi.fn().mockResolvedValue(undefined),
        getThreadById: vi.fn().mockResolvedValue({
          id: 'thread-1',
          title: 'Test Thread',
          metadata: {},
        }),
        listMessages: vi.fn().mockResolvedValue({ messages: [], total: 0 }),
        updateThread: vi.fn().mockResolvedValue(undefined),
      } as unknown as MemoryStorage;

      const processor = new MessageHistory({
        storage: mockStorage,
      });

      const messages: MastraDBMessage[] = [
        {
          role: 'user',
          content: { format: 2, parts: [{ type: 'text', text: 'User message' }] },
          id: 'msg-2',
          createdAt: new Date(),
        },
        {
          role: 'assistant',
          content: { format: 2, parts: [{ type: 'text', text: 'Assistant response' }] },
          id: 'msg-4',
          createdAt: new Date(),
        },
      ];

      const messageList = new MessageList().add(messages, `input`).addSystem('System prompt 3');
      await processor.processOutputResult({
        messageList,
        messages,
        abort: ((reason?: string) => {
          throw new Error(reason || 'Aborted');
        }) as (reason?: string) => never,
        requestContext: createRuntimeContextWithMemory('thread-1'),
      });

      const savedMessages = (mockStorage.saveMessages as any).mock.calls[0][0].messages;
      expect(savedMessages).toHaveLength(2);
      expect(savedMessages.every((m: any) => m.role !== 'system')).toBe(true);
    });

    it('should not persist system messages even when passed directly to persistMessages', async () => {
      const mockStorage = {
        saveMessages: vi.fn().mockResolvedValue(undefined),
        getThreadById: vi.fn().mockResolvedValue({
          id: 'thread-1',
          title: 'Test Thread',
          metadata: {},
        }),
        updateThread: vi.fn().mockResolvedValue(undefined),
      } as unknown as MemoryStorage;

      const processor = new MessageHistory({
        storage: mockStorage,
      });

      const messages: MastraDBMessage[] = [
        {
          role: 'system',
          content: { format: 2, parts: [{ type: 'text', text: 'Runtime-only system instruction' }] },
          id: 'msg-system',
          createdAt: new Date(),
        },
        {
          role: 'user',
          content: { format: 2, parts: [{ type: 'text', text: 'User message' }] },
          id: 'msg-user',
          createdAt: new Date(),
        },
      ];

      await processor.persistMessages({ messages, threadId: 'thread-1' });

      expect(mockStorage.saveMessages).toHaveBeenCalledWith({
        messages: [expect.objectContaining({ id: 'msg-user', role: 'user' })],
      });
    });

    it('should preserve dynamic system reminders in persisted non-system messages to avoid cache invalidation and re-injection', async () => {
      const mockStorage = {
        saveMessages: vi.fn().mockResolvedValue(undefined),
        getThreadById: vi.fn().mockResolvedValue({
          id: 'thread-1',
          title: 'Test Thread',
          metadata: {},
        }),
        listMessages: vi.fn().mockResolvedValue({ messages: [], total: 0 }),
        updateThread: vi.fn().mockResolvedValue(undefined),
      } as unknown as MemoryStorage;

      const processor = new MessageHistory({
        storage: mockStorage,
      });

      const reminderMarkup =
        '<system-reminder type="dynamic-agents-md" path="/repo/packages/core/AGENTS.md">Core guidance</system-reminder>';

      const messages: MastraDBMessage[] = [
        {
          role: 'user',
          content: { format: 2, parts: [{ type: 'text', text: reminderMarkup }] },
          id: 'msg-reminder',
          createdAt: new Date(),
        },
      ];

      const messageList = new MessageList().add(messages, `input`);
      await processor.processOutputResult({
        messageList,
        messages,
        abort: ((reason?: string) => {
          throw new Error(reason || 'Aborted');
        }) as (reason?: string) => never,
        requestContext: createRuntimeContextWithMemory('thread-1'),
      });

      const savedMessages = (mockStorage.saveMessages as any).mock.calls[0][0].messages as MastraDBMessage[];
      expect(savedMessages).toHaveLength(1);
      expect(savedMessages[0]).toEqual(
        expect.objectContaining({
          role: 'user',
          content: expect.objectContaining({
            parts: [expect.objectContaining({ type: 'text', text: reminderMarkup })],
          }),
        }),
      );
    });

    it('should update thread metadata', async () => {
      const mockStorage = {
        saveMessages: vi.fn().mockResolvedValue(undefined),
        getThreadById: vi.fn().mockResolvedValue({
          id: 'thread-1',
          title: 'Test Thread',
          metadata: { createdAt: new Date('2024-01-01') },
        }),
        updateThread: vi.fn().mockResolvedValue(undefined),
      } as unknown as MemoryStorage;

      const processor = new MessageHistory({
        storage: mockStorage,
      });

      const messages: MastraDBMessage[] = [
        {
          id: 'msg-1',
          role: 'user' as const,
          content: { format: 2, parts: [{ type: 'text', text: 'Hello' }] },
          createdAt: new Date(),
        },
      ];

      const messageList = new MessageList().add(messages, `input`);

      await processor.processOutputResult({
        messages,
        abort: ((reason?: string) => {
          throw new Error(reason || 'Aborted');
        }) as (reason?: string) => never,
        requestContext: createRuntimeContextWithMemory('thread-1'),
        messageList,
      });

      expect(mockStorage.updateThread).toHaveBeenCalledWith({
        id: 'thread-1',
        title: 'Test Thread',
        metadata: expect.objectContaining({
          createdAt: expect.any(Date),
        }),
      });
    });

    it('should return original messages when no threadId', async () => {
      const mockStorage = {
        saveMessages: vi.fn(),
      } as unknown as MemoryStorage;

      const processor = new MessageHistory({
        storage: mockStorage,
        // No threadId
      });

      const messages: MastraDBMessage[] = [
        {
          id: 'msg-1',
          role: 'user' as const,
          content: { format: 2, parts: [{ type: 'text', text: 'Hello' }] },
          createdAt: new Date(),
        },
      ];

      const messageList = new MessageList().add(messages, `input`);
      const result = await processor.processOutputResult({
        messageList,
        messages,
        abort: ((reason?: string) => {
          throw new Error(reason || 'Aborted');
        }) as (reason?: string) => never,
        // No requestContext, so no threadId
      });

      expect(result.get.input.db()).toEqual(messages);
      expect(mockStorage.saveMessages).not.toHaveBeenCalled();
    });

    it('should handle messages with only system messages', async () => {
      const mockStorage = {
        saveMessages: vi.fn(),
      } as unknown as MemoryStorage;

      const processor = new MessageHistory({
        storage: mockStorage,
      });

      const messageList = new MessageList().addSystem(['System message 1', 'System message 2']);
      await processor.processOutputResult({
        messageList,
        messages: [],
        abort: ((reason?: string) => {
          throw new Error(reason || 'Aborted');
        }) as (reason?: string) => never,
        requestContext: createRuntimeContextWithMemory('thread-1'),
      });

      expect(mockStorage.saveMessages).not.toHaveBeenCalled();
    });

    it('should preserve existing message IDs', async () => {
      const mockStorage = {
        saveMessages: vi.fn().mockResolvedValue(undefined),
        getThreadById: vi.fn().mockResolvedValue({
          id: 'thread-1',
          title: 'Test Thread',
          metadata: {},
        }),
        listMessages: vi.fn().mockResolvedValue({ messages: [], total: 0 }),
        updateThread: vi.fn().mockResolvedValue(undefined),
      } as unknown as MemoryStorage;

      const processor = new MessageHistory({
        storage: mockStorage,
      });

      const messages: MastraDBMessage[] = [
        {
          role: 'user' as const,
          content: { format: 2, parts: [{ type: 'text', text: 'Hello' }] },
          id: 'existing-id-123',
          createdAt: new Date(),
        },
      ];

      const messageList = new MessageList().add(messages, `input`);
      await processor.processOutputResult({
        messageList,
        messages,
        abort: ((reason?: string) => {
          throw new Error(reason || 'Aborted');
        }) as (reason?: string) => never,
        requestContext: createRuntimeContextWithMemory('thread-1'),
      });

      const savedMessages = (mockStorage.saveMessages as any).mock.calls[0][0].messages;
      expect(savedMessages[0].id).toBe('existing-id-123');
    });

    it('should preserve leading/trailing whitespace in text parts that have no working memory tags', async () => {
      const mockStorage = {
        saveMessages: vi.fn().mockResolvedValue(undefined),
        getThreadById: vi.fn().mockResolvedValue({
          id: 'thread-1',
          title: 'Test Thread',
          metadata: {},
        }),
        listMessages: vi.fn().mockResolvedValue({ messages: [], total: 0 }),
        updateThread: vi.fn().mockResolvedValue(undefined),
      } as unknown as MemoryStorage;

      const processor = new MessageHistory({
        storage: mockStorage,
      });

      // Token-boundary splits produce parts with meaningful leading whitespace
      // (e.g. ' access'). Trimming these corrupts the concatenated output.
      const messages: MastraDBMessage[] = [
        {
          role: 'assistant',
          content: {
            format: 2,
            parts: [
              { type: 'text', text: 'You can' },
              { type: 'text', text: ' access' },
              { type: 'text', text: ' the data.' },
            ],
          },
          id: 'msg-1',
          createdAt: new Date('2024-01-01T00:00:01Z'),
        },
      ];

      const messageList = new MessageList().add(messages, `response`);
      await processor.processOutputResult({
        messageList,
        messages,
        abort: ((reason?: string) => {
          throw new Error(reason || 'Aborted');
        }) as (reason?: string) => never,
        requestContext: createRuntimeContextWithMemory('thread-1'),
      });

      const savedMessages = (mockStorage.saveMessages as any).mock.calls[0][0].messages;
      const savedParts = savedMessages[0].content.parts.filter((p: any) => p.type === 'text');
      expect(savedParts.map((p: any) => p.text)).toEqual(['You can', ' access', ' the data.']);
      expect(savedParts.map((p: any) => p.text).join('')).toBe('You can access the data.');
    });

    it('should strip working memory tags and trim only the parts that contained tags', async () => {
      const mockStorage = {
        saveMessages: vi.fn().mockResolvedValue(undefined),
        getThreadById: vi.fn().mockResolvedValue({
          id: 'thread-1',
          title: 'Test Thread',
          metadata: {},
        }),
        listMessages: vi.fn().mockResolvedValue({ messages: [], total: 0 }),
        updateThread: vi.fn().mockResolvedValue(undefined),
      } as unknown as MemoryStorage;

      const processor = new MessageHistory({
        storage: mockStorage,
      });

      const messages: MastraDBMessage[] = [
        {
          role: 'assistant',
          content: {
            format: 2,
            parts: [
              { type: 'text', text: 'Saved.\n<working_memory>secret</working_memory>' },
              { type: 'text', text: ' untouched ' },
            ],
          },
          id: 'msg-1',
          createdAt: new Date('2024-01-01T00:00:01Z'),
        },
      ];

      const messageList = new MessageList().add(messages, `response`);
      await processor.processOutputResult({
        messageList,
        messages,
        abort: ((reason?: string) => {
          throw new Error(reason || 'Aborted');
        }) as (reason?: string) => never,
        requestContext: createRuntimeContextWithMemory('thread-1'),
      });

      const savedMessages = (mockStorage.saveMessages as any).mock.calls[0][0].messages;
      const savedParts = savedMessages[0].content.parts.filter((p: any) => p.type === 'text');
      // The part with a tag is stripped and trimmed; the untouched part keeps its whitespace.
      expect(savedParts.map((p: any) => p.text)).toEqual(['Saved.', ' untouched ']);
    });
  });

  describe('toolCallFilter persistence policy', () => {
    const createPersistenceStorage = () =>
      ({
        saveMessages: vi.fn().mockResolvedValue(undefined),
        getThreadById: vi.fn().mockResolvedValue({
          id: 'thread-1',
          title: 'Test Thread',
          metadata: {},
        }),
        updateThread: vi.fn().mockResolvedValue(undefined),
      }) as unknown as MemoryStorage;

    const createToolResultMessage = (): MastraDBMessage => ({
      role: 'assistant',
      content: {
        format: 2,
        content: 'Final answer',
        providerMetadata: {
          mastra: { rawToolResult: 'CONTENT_PROVIDER_SECRET' },
        },
        parts: [
          { type: 'text', text: 'Final answer' },
          {
            type: 'tool-invocation',
            toolInvocation: {
              state: 'result',
              toolCallId: 'call-search',
              toolName: 'search',
              args: { query: 'RAW_ARGS_SENTINEL' },
              result: { hits: ['RAW_RESULT_SENTINEL'] },
              rawInput: { query: 'RAW_INPUT_SENTINEL' },
              errorText: 'ERROR_TEXT_SENTINEL',
              approval: { id: 'APPROVAL_ID_SENTINEL', reason: 'APPROVAL_REASON_SENTINEL' },
            },
            title: 'PART_TITLE_SENTINEL',
            providerExecuted: true,
            providerMetadata: {
              mastra: {
                modelOutput: {
                  type: 'content',
                  value: [
                    { type: 'text', text: 'Compact result' },
                    { type: 'media', data: 'BASE64_SENTINEL', mediaType: 'image/png' },
                  ],
                },
                rawProviderPayload: 'PROVIDER_METADATA_SENTINEL',
              },
            },
          },
        ],
        toolInvocations: [
          {
            state: 'result',
            toolCallId: 'call-search',
            toolName: 'search',
            args: { query: 'TOP_LEVEL_ARGS_SENTINEL' },
            result: 'TOP_LEVEL_RESULT_SENTINEL',
          },
        ],
      },
      id: 'msg-tool-result',
      createdAt: new Date('2024-01-01T00:00:01Z'),
    });

    const createPrefilteredToolMessage = (toolName: string, state: 'call' | 'partial-call'): MastraDBMessage =>
      ({
        role: 'assistant',
        content: {
          format: 2,
          content: 'Keep this answer',
          providerMetadata: {
            mastra: { rawToolPayload: 'PREFILTERED_PROVIDER_SECRET' },
          },
          parts: [
            { type: 'text', text: 'Keep this answer' },
            {
              type: 'tool-invocation',
              toolInvocation: {
                state,
                toolCallId: `call-${toolName}`,
                toolName,
                args: { secret: 'PREFILTERED_ARGS_SECRET' },
              },
            },
          ],
        },
        id: `msg-${toolName}`,
        createdAt: new Date('2024-01-01T00:00:01Z'),
      }) as MastraDBMessage;

    it('filters direct persistMessages writes without mutating the source message', async () => {
      const storage = createPersistenceStorage();
      const processor = new MessageHistory({
        storage,
        toolCallFilter: {
          preserveModelOutput: true,
          maxModelOutputBytes: 128,
        },
      });
      const message = createToolResultMessage();
      const sourceBefore = JSON.stringify(message);

      await processor.persistMessages({ messages: [message], threadId: 'thread-1' });

      expect(JSON.stringify(message)).toBe(sourceBefore);
      const savedMessages = (storage.saveMessages as any).mock.calls[0][0].messages as MastraDBMessage[];
      const serialized = JSON.stringify(savedMessages);
      expect(savedMessages).toHaveLength(1);
      expect(serialized).toContain('Final answer');
      expect(serialized).toContain('search result:\\nCompact result');
      expect(serialized).not.toContain('RAW_ARGS_SENTINEL');
      expect(serialized).not.toContain('RAW_RESULT_SENTINEL');
      expect(serialized).not.toContain('RAW_INPUT_SENTINEL');
      expect(serialized).not.toContain('ERROR_TEXT_SENTINEL');
      expect(serialized).not.toContain('APPROVAL_ID_SENTINEL');
      expect(serialized).not.toContain('APPROVAL_REASON_SENTINEL');
      expect(serialized).not.toContain('PART_TITLE_SENTINEL');
      expect(serialized).not.toContain('PROVIDER_METADATA_SENTINEL');
      expect(serialized).not.toContain('CONTENT_PROVIDER_SECRET');
      expect(serialized).not.toContain('TOP_LEVEL_ARGS_SENTINEL');
      expect(serialized).not.toContain('TOP_LEVEL_RESULT_SENTINEL');
      expect(serialized).not.toContain('BASE64_SENTINEL');
      expect(savedMessages[0]!.content.toolInvocations).toBeUndefined();
    });

    it('applies the same policy through processOutputResult', async () => {
      const storage = createPersistenceStorage();
      const processor = new MessageHistory({
        storage,
        toolCallFilter: { preserveModelOutput: true },
      });
      const message = createToolResultMessage();
      const messageList = new MessageList().add(message, 'response');

      await processor.processOutputResult({
        messageList,
        messages: [message],
        abort: mockAbort,
        requestContext: createRuntimeContextWithMemory('thread-1'),
      });

      const savedMessages = (storage.saveMessages as any).mock.calls[0][0].messages as MastraDBMessage[];
      const serialized = JSON.stringify(savedMessages);
      expect(serialized).toContain('search result:\\nCompact result');
      expect(serialized).not.toContain('RAW_RESULT_SENTINEL');
      expect(messageList.get.response.db()).toEqual([message]);
    });

    it.each([
      ['streaming tool call', 'search', 'partial-call'],
      ['working-memory tool call', 'updateWorkingMemory', 'call'],
    ] as const)('strips provider metadata when the policy covers a prefiltered %s', async (_label, toolName, state) => {
      const storage = createPersistenceStorage();
      const processor = new MessageHistory({ storage, toolCallFilter: {} });
      const message = createPrefilteredToolMessage(toolName, state);

      await processor.persistMessages({ messages: [message], threadId: 'thread-1' });

      const savedMessages = (storage.saveMessages as any).mock.calls[0][0].messages as MastraDBMessage[];
      const serialized = JSON.stringify(savedMessages);
      expect(serialized).toContain('Keep this answer');
      expect(serialized).not.toContain('PREFILTERED_PROVIDER_SECRET');
      expect(serialized).not.toContain('PREFILTERED_ARGS_SECRET');
    });

    it('preserves existing metadata behavior when exclude is an empty no-op policy', async () => {
      const storage = createPersistenceStorage();
      const processor = new MessageHistory({ storage, toolCallFilter: { exclude: [] } });
      const message = createPrefilteredToolMessage('search', 'partial-call');

      await processor.persistMessages({ messages: [message], threadId: 'thread-1' });

      const savedMessages = (storage.saveMessages as any).mock.calls[0][0].messages as MastraDBMessage[];
      const serialized = JSON.stringify(savedMessages);
      expect(serialized).toContain('PREFILTERED_PROVIDER_SECRET');
      expect(serialized).not.toContain('PREFILTERED_ARGS_SECRET');
    });

    it('applies a finite model-output bound when persistence filtering omits one', async () => {
      const storage = createPersistenceStorage();
      const processor = new MessageHistory({
        storage,
        toolCallFilter: { preserveModelOutput: true },
      });
      const message = createToolResultMessage();
      const toolPart = message.content.parts.find(part => part.type === 'tool-invocation');
      if (!toolPart || toolPart.type !== 'tool-invocation') throw new Error('expected tool invocation');
      toolPart.providerMetadata = { mastra: { modelOutput: 'x'.repeat(1024 * 1024) } };

      const encodeSpy = vi.spyOn(TextEncoder.prototype, 'encode');
      let encodedInputLengths: number[];
      try {
        await processor.persistMessages({ messages: [message], threadId: 'thread-1' });
        encodedInputLengths = encodeSpy.mock.calls.map(([input]) => input.length);
      } finally {
        encodeSpy.mockRestore();
      }

      const savedMessages = (storage.saveMessages as any).mock.calls[0][0].messages as MastraDBMessage[];
      const serialized = JSON.stringify(savedMessages);
      expect(Math.max(...encodedInputLengths!)).toBeLessThanOrEqual(16 * 1024 + 1);
      expect(serialized).toContain('[truncated]');
      expect(new TextEncoder().encode(serialized).byteLength).toBeLessThan(17 * 1024);
    });

    it.each(['array', 'legacy wrapper'] as const)('omits circular %s model output', async shape => {
      const storage = createPersistenceStorage();
      const processor = new MessageHistory({
        storage,
        toolCallFilter: { preserveModelOutput: true },
      });
      const message = createToolResultMessage();
      const toolPart = message.content.parts.find(part => part.type === 'tool-invocation');
      if (!toolPart || toolPart.type !== 'tool-invocation') throw new Error('expected tool invocation');
      const circular: unknown[] | { value?: unknown } = shape === 'array' ? [] : {};
      if (Array.isArray(circular)) circular.push(circular);
      else circular.value = circular;
      toolPart.providerMetadata = { mastra: { modelOutput: circular } };

      await processor.persistMessages({ messages: [message], threadId: 'thread-1' });

      const savedMessages = (storage.saveMessages as any).mock.calls[0][0].messages as MastraDBMessage[];
      expect(JSON.stringify(savedMessages)).not.toContain('search result');
    });

    it('filters legacy messages that store tool payloads only in content.toolInvocations', async () => {
      const storage = createPersistenceStorage();
      const processor = new MessageHistory({ storage, toolCallFilter: {} });
      const message = createToolResultMessage();
      message.content.parts = [{ type: 'text', text: 'Final answer' }];

      await processor.persistMessages({ messages: [message], threadId: 'thread-1' });

      const savedMessages = (storage.saveMessages as any).mock.calls[0][0].messages as MastraDBMessage[];
      const serialized = JSON.stringify(savedMessages);
      expect(serialized).toContain('Final answer');
      expect(serialized).not.toContain('TOP_LEVEL_ARGS_SENTINEL');
      expect(serialized).not.toContain('TOP_LEVEL_RESULT_SENTINEL');
      expect(savedMessages[0]!.content.toolInvocations).toBeUndefined();
    });

    it.each([
      ['string content', 'Legacy text'],
      ['missing parts', { format: 2, content: 'Legacy object text' }],
    ])('does not crash on %s when persistence filtering is enabled', async (_label, content) => {
      const storage = createPersistenceStorage();
      const processor = new MessageHistory({ storage, toolCallFilter: {} });
      const message = {
        id: 'legacy-malformed-message',
        role: 'assistant',
        content,
        createdAt: new Date('2024-01-01T00:00:01Z'),
      } as unknown as MastraDBMessage;
      const sourceBefore = JSON.stringify(message);

      await processor.persistMessages({ messages: [message], threadId: 'thread-1' });

      expect(JSON.stringify(message)).toBe(sourceBefore);
      const savedMessages = (storage.saveMessages as any).mock.calls[0][0].messages as MastraDBMessage[];
      expect(savedMessages).toHaveLength(1);
      expect(JSON.stringify(savedMessages[0]!.content)).toContain('Legacy');
    });

    it('keeps existing persistence behavior when the policy is omitted', async () => {
      const storage = createPersistenceStorage();
      const processor = new MessageHistory({ storage });
      const message = createToolResultMessage();

      await processor.persistMessages({ messages: [message], threadId: 'thread-1' });

      const savedMessages = (storage.saveMessages as any).mock.calls[0][0].messages as MastraDBMessage[];
      const serialized = JSON.stringify(savedMessages);
      expect(serialized).toContain('RAW_ARGS_SENTINEL');
      expect(serialized).toContain('RAW_RESULT_SENTINEL');
      expect(serialized).toContain('TOP_LEVEL_ARGS_SENTINEL');
      expect(serialized).toContain('TOP_LEVEL_RESULT_SENTINEL');
    });
  });
});
