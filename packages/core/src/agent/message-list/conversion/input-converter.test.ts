import { describe, expect, it, vi } from 'vitest';
import type { MastraDBMessage } from '../state/types';
import type { InputConversionContext } from './input-converter';
import { hydrateMastraDBMessageFields } from './input-converter';

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
