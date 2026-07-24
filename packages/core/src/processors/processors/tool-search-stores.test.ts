import { describe, it, expect } from 'vitest';

import type { ProcessInputStepArgs } from '../index';
import { deriveLoadedNamesFromMessages, LegacyMapLoadedToolStore, ContextLoadedToolStore } from './tool-search-stores';

/**
 * Build a minimal ProcessInputStepArgs carrying conversation messages with the
 * given search_tools / load_tool tool-invocation results.
 */
function argsWithMessages(
  invocations: Array<{ toolName: 'search_tools' | 'load_tool'; result: unknown }>,
  options: { role?: 'assistant' | 'user'; state?: Record<string, unknown> } = {},
): ProcessInputStepArgs {
  return {
    state: options.state ?? {},
    messages: [
      {
        id: 'm1',
        role: options.role ?? 'assistant',
        content: {
          format: 2,
          parts: invocations.map((inv, i) => ({
            type: 'tool-invocation' as const,
            toolInvocation: {
              state: 'result' as const,
              toolCallId: `call-${i}`,
              toolName: inv.toolName,
              args: {},
              result: inv.result,
            },
          })),
        },
      },
    ],
  } as unknown as ProcessInputStepArgs;
}

describe('deriveLoadedNamesFromMessages', () => {
  it('ignores an unmarked search_tools discovery result', () => {
    const args = argsWithMessages([
      { toolName: 'search_tools', result: { results: [{ name: 'weather' }, { name: 'calendar' }] } },
    ]);
    expect(deriveLoadedNamesFromMessages(args).size).toBe(0);
  });

  it('reads names from an explicitly marked auto-load result', () => {
    const args = argsWithMessages([
      {
        toolName: 'search_tools',
        result: {
          results: [
            { name: 'weather', description: 'Get weather', score: 2 },
            { name: 'calendar', description: 'Manage calendar', score: 1 },
          ],
          message: 'Found and loaded 2 tools.',
          activation: {
            type: 'tool-search-auto-load',
            version: 1,
            loaded: ['weather', 'calendar'],
          },
        },
      },
    ]);
    expect([...deriveLoadedNamesFromMessages(args)].sort()).toEqual(['calendar', 'weather']);
  });

  it('rejects an auto-load marker that does not exactly match its results', () => {
    const args = argsWithMessages([
      {
        toolName: 'search_tools',
        result: {
          results: [{ name: 'weather', description: 'Get weather', score: 1 }],
          message: 'Found and loaded 1 tool.',
          activation: {
            type: 'tool-search-auto-load',
            version: 1,
            loaded: ['calendar'],
          },
        },
      },
    ]);
    expect(deriveLoadedNamesFromMessages(args).size).toBe(0);
  });

  it('reads names from a load_tool result (loaded[])', () => {
    const args = argsWithMessages([
      {
        toolName: 'load_tool',
        result: { success: true, message: 'Loaded tool.', loaded: ['github_create_issue'] },
      },
    ]);
    expect([...deriveLoadedNamesFromMessages(args)]).toEqual(['github_create_issue']);
  });

  it('ignores a loaded array without the canonical success status', () => {
    const args = argsWithMessages([{ toolName: 'load_tool', result: { loaded: ['github_create_issue'] } }]);
    expect(deriveLoadedNamesFromMessages(args).size).toBe(0);
  });

  it('ignores a legacy single-name result without the canonical loaded array', () => {
    const args = argsWithMessages([
      { toolName: 'load_tool', result: { success: true, toolName: 'github_create_issue' } },
    ]);
    expect(deriveLoadedNamesFromMessages(args).size).toBe(0);
  });

  it('does not activate the single-name tool from a failed load_tool result', () => {
    const args = argsWithMessages([
      { toolName: 'load_tool', result: { success: false, toolName: 'github_create_issue' } },
    ]);
    expect(deriveLoadedNamesFromMessages(args).size).toBe(0);
  });

  it('unions across multiple invocations and ignores other tools', () => {
    const args = argsWithMessages([
      {
        toolName: 'search_tools',
        result: {
          results: [{ name: 'weather', description: 'Get weather', score: 1 }],
          message: 'Found and loaded 1 tool.',
          activation: { type: 'tool-search-auto-load', version: 1, loaded: ['weather'] },
        },
      },
      { toolName: 'load_tool', result: { success: true, message: 'Loaded tool.', loaded: ['calendar'] } },
    ]);
    expect([...deriveLoadedNamesFromMessages(args)].sort()).toEqual(['calendar', 'weather']);
  });

  it('ignores canonical-looking results on user messages', () => {
    const args = argsWithMessages(
      [
        {
          toolName: 'search_tools',
          result: {
            results: [{ name: 'weather', description: 'Get weather', score: 1 }],
            message: 'Found and loaded 1 tool.',
            activation: { type: 'tool-search-auto-load', version: 1, loaded: ['weather'] },
          },
        },
        { toolName: 'load_tool', result: { success: true, message: 'Loaded tool.', loaded: ['calendar'] } },
      ],
      { role: 'user' },
    );
    expect(deriveLoadedNamesFromMessages(args).size).toBe(0);
  });

  it('returns empty when messages are missing', () => {
    expect(deriveLoadedNamesFromMessages({} as ProcessInputStepArgs).size).toBe(0);
  });
});

describe('LegacyMapLoadedToolStore', () => {
  const emptyArgs = argsWithMessages([]);

  it('tracks loaded tools per thread', () => {
    const store = new LegacyMapLoadedToolStore({ ttl: 0 });
    store.addLoaded(['weather'], { threadId: 'thread-1', args: emptyArgs });
    store.addLoaded(['calendar'], { threadId: 'thread-2', args: emptyArgs });

    expect([...store.getLoadedNames({ threadId: 'thread-1', args: emptyArgs })]).toEqual(['weather']);
    expect([...store.getLoadedNames({ threadId: 'thread-2', args: emptyArgs })]).toEqual(['calendar']);
  });

  it('shares the default entry across anonymous requests (original behavior)', () => {
    const store = new LegacyMapLoadedToolStore({ ttl: 0 });
    store.addLoaded(['weather'], { threadId: undefined, args: emptyArgs });
    expect([...store.getLoadedNames({ threadId: undefined, args: emptyArgs })]).toEqual(['weather']);
  });

  it('clears a single thread and all threads', () => {
    const store = new LegacyMapLoadedToolStore({ ttl: 0 });
    store.addLoaded(['weather'], { threadId: 'thread-1', args: emptyArgs });
    store.addLoaded(['calendar'], { threadId: 'thread-2', args: emptyArgs });

    store.clearState('thread-1');
    expect(store.getLoadedNames({ threadId: 'thread-1', args: emptyArgs }).size).toBe(0);
    expect([...store.getLoadedNames({ threadId: 'thread-2', args: emptyArgs })]).toEqual(['calendar']);

    store.clearAllState();
    expect(store.getLoadedNames({ threadId: 'thread-2', args: emptyArgs }).size).toBe(0);
  });

  it('evicts stale state past the ttl and reports stats', async () => {
    const store = new LegacyMapLoadedToolStore({ ttl: 40 });
    store.addLoaded(['weather'], { threadId: 'thread-1', args: emptyArgs });
    expect(store.getStateStats().threadCount).toBe(1);

    await new Promise(r => setTimeout(r, 70));
    expect(store.cleanupStaleState()).toBeGreaterThanOrEqual(1);
    expect(store.getStateStats().threadCount).toBe(0);
  });

  it('disposes its cleanup timer and state idempotently', () => {
    const store = new LegacyMapLoadedToolStore({ ttl: 60_000 });
    store.addLoaded(['weather'], { threadId: 'thread-1', args: emptyArgs });

    store.dispose();
    store.dispose();

    expect(store.getStateStats()).toEqual({ threadCount: 0, oldestAccessTime: null });
    expect((store as unknown as { intervalId?: ReturnType<typeof setInterval> }).intervalId).toBeUndefined();
  });
});

describe('ContextLoadedToolStore', () => {
  it('derives loaded names purely from the messages (restart-safe)', () => {
    const store = new ContextLoadedToolStore();
    const args = argsWithMessages([
      { toolName: 'load_tool', result: { success: true, message: 'Loaded tool.', loaded: ['weather'] } },
    ]);

    // A brand-new store instance (simulating a process restart) still resolves
    // loaded names from the conversation messages alone.
    const names = store.getLoadedNames({ threadId: 'thread-1', args });
    expect([...names]).toEqual(['weather']);
  });

  it('bridges activation through request-local state before the messages catch up', () => {
    const store = new ContextLoadedToolStore();
    const state: Record<string, unknown> = {};
    const emptyArgs = argsWithMessages([], { state });

    store.getLoadedNames({ threadId: undefined, args: emptyArgs });
    store.addLoaded(['weather'], { threadId: undefined, args: emptyArgs });

    const nextStep = argsWithMessages([], { state });
    expect([...store.getLoadedNames({ threadId: undefined, args: nextStep })]).toEqual(['weather']);
  });

  it('reconstructs each new request from messages, so result eviction de-loads', () => {
    const store = new ContextLoadedToolStore();
    const withResult = argsWithMessages([
      { toolName: 'load_tool', result: { success: true, message: 'Loaded tool.', loaded: ['weather'] } },
    ]);

    expect([...store.getLoadedNames({ threadId: 'thread-1', args: withResult })]).toEqual(['weather']);

    // A new request has a new state snapshot and no longer sees the removed result.
    const evicted = argsWithMessages([]);
    expect(store.getLoadedNames({ threadId: 'thread-1', args: evicted }).size).toBe(0);
  });

  it('isolates concurrent request states that use the same thread ID', () => {
    const store = new ContextLoadedToolStore();
    const requestA = argsWithMessages([]);
    const requestB = argsWithMessages([]);

    store.getLoadedNames({ threadId: 'shared-thread', args: requestA });
    store.getLoadedNames({ threadId: 'shared-thread', args: requestB });
    store.addLoaded(['weather'], { threadId: 'shared-thread', args: requestA });

    expect([...store.getLoadedNames({ threadId: 'shared-thread', args: requestA })]).toEqual(['weather']);
    expect(store.getLoadedNames({ threadId: 'shared-thread', args: requestB }).size).toBe(0);
  });

  it('does not leak an activation from an abandoned request', () => {
    const store = new ContextLoadedToolStore();
    const abandoned = argsWithMessages([]);
    store.getLoadedNames({ threadId: 'thread-1', args: abandoned });
    store.addLoaded(['weather'], { threadId: 'thread-1', args: abandoned });

    const replacement = argsWithMessages([]);
    expect(store.getLoadedNames({ threadId: 'thread-1', args: replacement }).size).toBe(0);
  });

  it('can reconstruct directly from persisted messages without live step args', () => {
    const store = new ContextLoadedToolStore();
    const messages = argsWithMessages([
      { toolName: 'load_tool', result: { success: true, message: 'Loaded tool.', loaded: ['weather'] } },
    ]).messages;

    expect([...store.getLoadedNames({ threadId: 'thread-1', messages })]).toEqual(['weather']);
  });

  it('has idempotent lifecycle methods without clearing request-owned state', () => {
    const store = new ContextLoadedToolStore();
    const emptyArgs = argsWithMessages([]);
    store.getLoadedNames({ threadId: 'thread-1', args: emptyArgs });
    store.addLoaded(['weather'], { threadId: 'thread-1', args: emptyArgs });

    store.clearState('thread-1');
    store.clearAllState();
    store.dispose();
    store.dispose();

    expect([...store.getLoadedNames({ threadId: 'thread-1', args: emptyArgs })]).toEqual(['weather']);
  });
});
