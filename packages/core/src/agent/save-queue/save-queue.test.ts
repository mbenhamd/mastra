import { beforeEach, describe, expect, it, vi } from 'vitest';
import { MessageList } from '../message-list';
import type { MastraDBMessage } from '../types';
import { SaveQueueManager } from './index';

function makeTestMessage(id: string, threadId: string, role: 'user' | 'assistant', content: string): MastraDBMessage {
  return {
    id,
    role,
    content: { content, parts: [], format: 2 },
    createdAt: new Date(),
    threadId,
  };
}

describe('SaveQueueManager', () => {
  let saved: any[];
  let saveCalls: number;
  let manager: SaveQueueManager;
  let mockMemory: any;
  beforeEach(() => {
    saved = [];
    saveCalls = 0;
    mockMemory = {
      saveMessages: vi.fn(async ({ messages }) => {
        saveCalls++;
        saved.push(...messages);
      }),
    };
    manager = new SaveQueueManager({ memory: mockMemory });
  });

  it('batches saves with debounce', async () => {
    const list = new MessageList({ threadId: 'thread-1' });
    list.add(makeTestMessage('m1', 'thread-1', 'user', 'Hello'), 'user');
    manager.batchMessages(list, 'thread-1');
    list.add(makeTestMessage('m2', 'thread-1', 'user', 'Hello'), 'user');
    manager.batchMessages(list, 'thread-1');
    await new Promise(res => setTimeout(res, manager['debounceMs'] + 10));
    expect(saveCalls).toBe(1);
    expect(saved.length).toBe(2);
  });

  it('does nothing if no unsaved messages', async () => {
    const list = new MessageList({ threadId: 'thread-4' });
    await manager.flushMessages(list, 'thread-4');
    expect(saveCalls).toBe(0);
  });

  it('handles batchMessages with stale messages (forces flush)', async () => {
    const list = new MessageList({ threadId: 'thread-5' });
    const old = Date.now() - SaveQueueManager['MAX_STALENESS_MS'] - 100;
    const msg = makeTestMessage('m1', 'thread-5', 'user', 'Hello');
    msg.createdAt = new Date(old); // Ensure createdAt is stale
    list.add(msg, 'user');
    await manager.batchMessages(list, 'thread-5');
    expect(saveCalls).toBe(1);
    expect(saved[0].id).toBe('m1');
  });

  it('clearDebounce cancels pending debounce', async () => {
    const list = new MessageList({ threadId: 'thread-6' });
    list.add(makeTestMessage('m1', 'thread-6', 'user', 'Hello'), 'user');
    manager.batchMessages(list, 'thread-6');
    manager.clearDebounce('thread-6');
    await new Promise(res => setTimeout(res, manager['debounceMs'] + 10));
    expect(saveCalls).toBe(0);
  });

  it('should serialize saves with a save queue under rapid step completion', async () => {
    let concurrent = 0;
    let maxConcurrent = 0;
    let totalSaves = 0;

    // Spy on saveMessages to track concurrency
    mockMemory.saveMessages = vi.fn(async ({ messages }) => {
      concurrent++;
      maxConcurrent = Math.max(maxConcurrent, concurrent);
      await new Promise(res => setTimeout(res, 20));
      concurrent--;
      saved.push(...messages);
      totalSaves++;
    });

    const manager = new SaveQueueManager({ memory: mockMemory });
    const list = new MessageList({ threadId: 'thread-concurrency' });
    const threadId = 'thread-concurrency';

    // Add and trigger saves rapidly
    const savePromises: Promise<void>[] = [];
    for (let i = 0; i < 10; i++) {
      list.add(makeTestMessage(`m${i}`, threadId, 'user', `message ${i}`), 'user');
      savePromises.push(manager.flushMessages(list, threadId));
    }
    await Promise.all(savePromises);

    expect(maxConcurrent).toBe(1);
    expect(totalSaves).toBeGreaterThan(0);
  });

  it('should flush buffered parts via drainUnsavedMessages before persisting', async () => {
    let savedMessages: any[] = [];

    mockMemory.saveMessages = async function (...args) {
      savedMessages.push(...args[0].messages);
    };

    const manager = new SaveQueueManager({ memory: mockMemory });
    const list = new MessageList({ threadId: 'thread-drain' });
    const threadId = 'thread-drain';

    list.add(makeTestMessage('m1', threadId, 'user', 'Hello'), 'user');
    list.add(makeTestMessage('m2', threadId, 'assistant', 'Hi there!'), 'response');
    list.add(makeTestMessage('m3', threadId, 'user', 'How are you?'), 'user');

    expect(savedMessages.length).toBe(0);

    await manager.flushMessages(list, threadId);

    expect(savedMessages.length).toBe(3);
    expect(list.drainUnsavedMessages().length).toBe(0);
  });

  it('strict flush propagates failure and retries the same snapshot exactly once', async () => {
    const persisted: MastraDBMessage[] = [];
    let attempts = 0;
    const memory = {
      saveMessages: vi.fn(async ({ messages }: { messages: MastraDBMessage[] }) => {
        attempts++;
        if (attempts === 1) throw new Error('storage unavailable');
        persisted.push(...messages);
      }),
    };
    const manager = new SaveQueueManager({ memory: memory as any });
    const list = new MessageList({ threadId: 'thread-strict-retry' });
    list.add(makeTestMessage('strict-1', 'thread-strict-retry', 'assistant', 'Recoverable answer'), 'response');

    await expect(manager.flushMessagesStrict(list, 'thread-strict-retry')).rejects.toThrow('storage unavailable');
    expect(list.snapshotUnsavedMessages().messages.map(message => message.id)).toEqual(['strict-1']);

    await manager.flushMessagesStrict(list, 'thread-strict-retry');

    expect(memory.saveMessages).toHaveBeenCalledTimes(2);
    expect(persisted.map(message => message.id)).toEqual(['strict-1']);
    expect(list.drainUnsavedMessages()).toEqual([]);
  });

  it('captures a strict snapshot before an earlier best-effort save can drain it', async () => {
    let releaseFirstSave!: () => void;
    const firstSaveBlocked = new Promise<void>(resolve => {
      releaseFirstSave = resolve;
    });
    let firstSaveStarted!: () => void;
    const firstSaveDidStart = new Promise<void>(resolve => {
      firstSaveStarted = resolve;
    });
    const persisted: MastraDBMessage[][] = [];
    let attempts = 0;
    const memory = {
      saveMessages: vi.fn(async ({ messages }: { messages: MastraDBMessage[] }) => {
        attempts++;
        if (attempts === 1) {
          firstSaveStarted();
          await firstSaveBlocked;
          persisted.push(structuredClone(messages));
          return;
        }
        if (attempts === 2) {
          throw new Error('best-effort storage failure');
        }
        persisted.push(structuredClone(messages));
      }),
    };
    const manager = new SaveQueueManager({ memory: memory as any });
    const list = new MessageList({ threadId: 'thread-strict-race' });

    list.add(makeTestMessage('ordinary-1', 'thread-strict-race', 'assistant', 'Ordinary step'), 'response');
    const firstSave = manager.flushMessages(list, 'thread-strict-race');
    await firstSaveDidStart;

    // This best-effort operation is already queued, but it has not captured a
    // snapshot yet. It will see the terminal answer after the first save exits.
    const racingBestEffortSave = manager.flushMessages(list, 'thread-strict-race');
    list.add(makeTestMessage('terminal-1', 'thread-strict-race', 'assistant', 'Terminal answer'), 'response');
    const strictSave = manager.flushMessagesStrict(list, 'thread-strict-race');

    releaseFirstSave();
    await firstSave;
    await racingBestEffortSave;
    await strictSave;

    expect(memory.saveMessages).toHaveBeenCalledTimes(3);
    // Assistant parts merge into the existing message, so the strict write is
    // an idempotent upsert of the same ID with the terminal content included.
    expect(persisted.map(messages => messages.map(message => message.id))).toEqual([['ordinary-1'], ['ordinary-1']]);
    expect(JSON.stringify(persisted[0])).not.toContain('Terminal answer');
    expect(JSON.stringify(persisted[1])).toContain('Terminal answer');
    expect(list.drainUnsavedMessages()).toEqual([]);
  });

  it('retries the identical terminal snapshot after racing best-effort and strict writes both fail', async () => {
    let releaseFirstSave!: () => void;
    const firstSaveBlocked = new Promise<void>(resolve => {
      releaseFirstSave = resolve;
    });
    let firstSaveStarted!: () => void;
    const firstSaveDidStart = new Promise<void>(resolve => {
      firstSaveStarted = resolve;
    });
    const attempts: MastraDBMessage[][] = [];
    const persisted: MastraDBMessage[][] = [];
    const memory = {
      saveMessages: vi.fn(async ({ messages }: { messages: MastraDBMessage[] }) => {
        const attempt = structuredClone(messages);
        attempts.push(attempt);
        if (attempts.length === 1) {
          firstSaveStarted();
          await firstSaveBlocked;
          persisted.push(attempt);
          return;
        }
        if (attempts.length <= 3) {
          throw new Error(attempts.length === 2 ? 'best-effort storage failure' : 'strict storage failure');
        }
        persisted.push(attempt);
      }),
    };
    const manager = new SaveQueueManager({ memory: memory as any });
    const list = new MessageList({ threadId: 'thread-strict-double-failure' });

    list.add(
      makeTestMessage('shared-assistant', 'thread-strict-double-failure', 'assistant', 'Ordinary step'),
      'response',
    );
    const firstSave = manager.flushMessages(list, 'thread-strict-double-failure');
    await firstSaveDidStart;

    const racingBestEffortSave = manager.flushMessages(list, 'thread-strict-double-failure');
    list.add(
      makeTestMessage('terminal-part', 'thread-strict-double-failure', 'assistant', 'Terminal answer'),
      'response',
    );
    const failedStrictSave = manager.flushMessagesStrict(list, 'thread-strict-double-failure');

    releaseFirstSave();
    await firstSave;
    await racingBestEffortSave;
    await expect(failedStrictSave).rejects.toThrow('strict storage failure');

    const retryableSnapshot = list.snapshotUnsavedMessages({ detached: true }).messages;
    expect(retryableSnapshot.map(message => message.id)).toEqual(['shared-assistant']);
    expect(JSON.stringify(retryableSnapshot)).toContain('Terminal answer');

    await manager.flushMessagesStrict(list, 'thread-strict-double-failure');

    expect(memory.saveMessages).toHaveBeenCalledTimes(4);
    expect(attempts[2]).toEqual(attempts[3]);
    expect(JSON.stringify(persisted[0])).not.toContain('Terminal answer');
    expect(JSON.stringify(persisted[1])).toContain('Terminal answer');
    expect(list.drainUnsavedMessages()).toEqual([]);
  });
});
