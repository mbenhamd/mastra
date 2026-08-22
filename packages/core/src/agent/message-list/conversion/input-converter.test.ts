import { describe, expect, it, vi } from 'vitest';
import type { MastraDBMessage } from '../state/types';
import type { InputConversionContext } from './input-converter';
import { hydrateMastraDBMessageFields, inputToMastraDBMessage } from './input-converter';

function createMessage({ threadId, resourceId }: { threadId?: string; resourceId?: string }): MastraDBMessage {
  return {
    id: 'msg-1',
    role: 'user',
    createdAt: new Date('2026-01-01T00:00:00.000Z'),
    ...(threadId ? { threadId } : {}),
    ...(resourceId ? { resourceId } : {}),
    content: { format: 2, parts: [] },
  };
}

function createContext(): InputConversionContext {
  return {
    memoryInfo: { threadId: 'memory-thread', resourceId: 'memory-resource' },
    newMessageId: vi.fn(() => 'generated-id'),
    generateCreatedAt: vi.fn(() => new Date('2026-01-02T00:00:00.000Z')),
    dbMessages: [],
  };
}

describe('hydrateMastraDBMessageFields memory identity', () => {
  it.each([
    {
      name: 'backfills both missing identities',
      message: {},
      expected: { threadId: 'memory-thread', resourceId: 'memory-resource' },
    },
    {
      name: 'backfills resourceId independently when threadId already exists',
      message: { threadId: 'existing-thread' },
      expected: { threadId: 'existing-thread', resourceId: 'memory-resource' },
    },
    {
      name: 'backfills threadId without replacing an existing resourceId',
      message: { resourceId: 'existing-resource' },
      expected: { threadId: 'memory-thread', resourceId: 'existing-resource' },
    },
    {
      name: 'preserves both existing identities',
      message: { threadId: 'existing-thread', resourceId: 'existing-resource' },
      expected: { threadId: 'existing-thread', resourceId: 'existing-resource' },
    },
  ])('$name', ({ message, expected }) => {
    const context = createContext();
    const result = hydrateMastraDBMessageFields(createMessage(message), context, 'memory');

    expect(result).toEqual(expect.objectContaining(expected));
    expect(context.newMessageId).not.toHaveBeenCalled();
    expect(context.generateCreatedAt).not.toHaveBeenCalled();
  });
});

describe('inputToMastraDBMessage', () => {
  const makeContext = () =>
    ({
      memoryInfo: {
        threadId: 'thread-1',
        resourceId: 'resource-1',
      },
      newMessageId: vi.fn(() => 'generated-id'),
      generateCreatedAt: vi.fn(() => new Date('2026-01-02T00:00:00.000Z')),
      dbMessages: [],
    }) satisfies InputConversionContext;

  const makeMessage = (overrides: Partial<MastraDBMessage>) =>
    ({
      id: 'msg-1',
      role: 'user',
      createdAt: new Date('2026-01-01T00:00:00.000Z'),
      threadId: 'thread-1',
      resourceId: 'resource-1',
      content: {
        format: 2,
        parts: [{ type: 'text', text: 'hello' }],
      },
      ...overrides,
    }) as MastraDBMessage;

  it('accepts memory-sourced messages whose resourceId differs from the conversation resource', () => {
    // Memory messages can carry a system resourceId — e.g. observational-memory
    // continuation messages arrive with the observer's own resourceId. The
    // threadId guard already exempts `memory`; the resourceId guard must too,
    // or a compaction mid-run throws inside input processing and aborts the turn.
    const message = makeMessage({
      threadId: 'other-thread',
      resourceId: 'structured-observer',
    });

    expect(() => inputToMastraDBMessage(message, 'memory', makeContext())).not.toThrow();
  });

  it('accepts memory-sourced messages from another thread and preserves their threadId', () => {
    // Resource-scoped observational memory loads context via listMessagesByResourceId(),
    // which returns messages from every thread of the resource. Those arrive tagged
    // `memory` while carrying their originating threadId, so the guard must exempt them —
    // and the original threadId must survive so the message is not re-homed onto the
    // current thread. See https://github.com/mastra-ai/mastra/issues/15367.
    const message = makeMessage({ id: 'cross-thread-msg', threadId: 'other-thread' });

    const converted = inputToMastraDBMessage(message, 'memory', makeContext());

    expect(converted.threadId).toBe('other-thread');
  });

  it('still rejects non-memory messages with a mismatched resourceId', () => {
    const message = makeMessage({ resourceId: 'someone-else' });

    expect(() => inputToMastraDBMessage(message, 'user', makeContext())).toThrow(/wrong resourceId/);
  });

  it('still rejects non-memory messages with a mismatched threadId', () => {
    const message = makeMessage({ threadId: 'other-thread' });

    expect(() => inputToMastraDBMessage(message, 'user', makeContext())).toThrow(/wrong threadId/);
  });
});
