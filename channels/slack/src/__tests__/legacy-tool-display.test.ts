import type { ToolDisplayContext, ToolDisplayEvent } from '@mastra/core/channels';
import { describe, it, expect } from 'vitest';

import { SlackProvider, mapLegacyToolDisplay } from '../provider';

const CTX: ToolDisplayContext = { mode: 'streaming', platform: 'slack' };

function resultEvent(over: Partial<Extract<ToolDisplayEvent, { kind: 'result' }>> = {}): ToolDisplayEvent {
  return {
    kind: 'result',
    toolCallId: 'tc1',
    toolName: 'lookup',
    displayName: 'Lookup',
    argsSummary: '{}',
    args: { q: 'hi' },
    result: 'answer',
    resultText: 'answer',
    durationMs: 5,
    isError: false,
    ...over,
  };
}

describe('slack legacy tool-display back-compat', () => {
  it('accepts the deprecated `cards` / `formatToolCall` fields without throwing or type-erroring', () => {
    // Compile-time: these fields must still be assignable on SlackProviderConfig.
    // Runtime: construction must not throw.
    expect(
      () =>
        new SlackProvider({
          cards: false,
          formatToolCall: ({ toolName, result }) => `${toolName}: ${String(result)}`,
        }),
    ).not.toThrow();
    // Also valid via the deprecated `adapterConfig` nesting.
    expect(() => new SlackProvider({ adapterConfig: { streaming: false, cards: true } })).not.toThrow();
  });

  it('maps `cards: false` → `text` and `cards: true` → `cards`', () => {
    expect(mapLegacyToolDisplay({ cards: false })).toBe('text');
    expect(mapLegacyToolDisplay({ cards: true })).toBe('cards');
  });

  it('shims `formatToolCall` onto a ToolDisplayFn that posts on result/error only', () => {
    const fn = mapLegacyToolDisplay({
      formatToolCall: ({ toolName, result, isError }) => `${toolName}:${String(result)}:${isError ? 'err' : 'ok'}`,
    });
    expect(typeof fn).toBe('function');
    const toolDisplayFn = fn as Extract<typeof fn, (...a: never[]) => unknown>;

    // result event → { kind: 'post', message }
    expect(toolDisplayFn(resultEvent(), CTX)).toEqual({ kind: 'post', message: 'lookup:answer:ok' });

    // error event → posts with the error value and isError true
    const errEvent: ToolDisplayEvent = {
      kind: 'error',
      toolCallId: 'tc2',
      toolName: 'lookup',
      displayName: 'Lookup',
      argsSummary: '{}',
      args: {},
      error: 'boom',
      errorText: 'boom',
      durationMs: 1,
    };
    expect(toolDisplayFn(errEvent, CTX)).toEqual({ kind: 'post', message: 'lookup:boom:err' });

    // non-result/error events defer to the built-in renderer (undefined).
    const runningEvent: ToolDisplayEvent = {
      kind: 'running',
      toolCallId: 'tc3',
      toolName: 'lookup',
      displayName: 'Lookup',
      argsSummary: '{}',
      args: {},
    };
    expect(toolDisplayFn(runningEvent, CTX)).toBeUndefined();
  });

  it('returns undefined from the shim (so the caller default wins) when no field applies', () => {
    expect(mapLegacyToolDisplay({})).toBeUndefined();
  });

  it('an explicit `toolDisplay` always wins over the legacy fields', () => {
    expect(mapLegacyToolDisplay({ toolDisplay: 'hidden', cards: true })).toBe('hidden');
    expect(mapLegacyToolDisplay({ toolDisplay: 'timeline', formatToolCall: () => 'x' })).toBe('timeline');
  });

  it('a `formatToolCall` returning null skips rendering (undefined → built-in renderer)', () => {
    const fn = mapLegacyToolDisplay({ formatToolCall: () => null });
    const toolDisplayFn = fn as Extract<typeof fn, (...a: never[]) => unknown>;
    expect(toolDisplayFn(resultEvent(), CTX)).toBeUndefined();
  });
});
