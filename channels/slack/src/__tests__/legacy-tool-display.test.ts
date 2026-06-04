import type { ToolDisplayContext, ToolDisplayEvent } from '@mastra/core/channels';
import { getChatModule } from '@mastra/core/channels';
import { describe, it, expect, beforeAll } from 'vitest';

import { SlackProvider, mapLegacyToolDisplay } from '../provider';

const CTX: ToolDisplayContext = { mode: 'streaming', platform: 'slack' };

// The built-in approval card is built via the lazily-loaded `chat` UI module
// (the static driver primes it through `AgentChannels.initialize()`); prime it
// here so the approval-card assertion can render the Block Kit card.
beforeAll(async () => {
  await getChatModule();
});

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

    // result event → { kind: 'post', message }. 1.2.x passed the stripped
    // display name ('Lookup'), not the raw tool id ('lookup').
    expect(toolDisplayFn(resultEvent(), CTX)).toEqual({ kind: 'post', message: 'Lookup:answer:ok' });

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
    expect(toolDisplayFn(errEvent, CTX)).toEqual({ kind: 'post', message: 'Lookup:boom:err' });

    // `running` defers to the built-in renderer (undefined): 1.2.x
    // `formatToolCall` fired only on result/error, so there was no running card.
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

  it('falls back to `toolName` when `displayName` is empty', () => {
    const fn = mapLegacyToolDisplay({
      formatToolCall: ({ toolName, result }) => `${toolName}:${String(result)}`,
    });
    const toolDisplayFn = fn as Extract<typeof fn, (...a: never[]) => unknown>;
    // displayName: '' must not produce an empty tool name; fall back to toolName.
    expect(toolDisplayFn(resultEvent({ displayName: '' }), CTX)).toEqual({ kind: 'post', message: 'lookup:answer' });
  });

  it('APPROVAL phase posts the built-in approval card (legacy API had no approval rendering)', () => {
    // Regression for the stuck-approval bug: core treats ANY `toolDisplayFn` as
    // approval-capable and does NOT auto-approve. The static driver SKIPS the
    // approval post when the fn returns null/undefined. So a legacy
    // `formatToolCall` (which returned undefined on approval) suppressed the
    // approval card without auto-approving → the run stayed suspended forever.
    // The shim must instead emit the built-in card so the approval IS posted.
    const fn = mapLegacyToolDisplay({
      formatToolCall: ({ toolName, result }) => `${toolName}:${String(result)}`,
    });
    const toolDisplayFn = fn as Extract<typeof fn, (...a: never[]) => unknown>;

    const approvalEvent: ToolDisplayEvent = {
      kind: 'approval',
      toolCallId: 'tc-approve',
      toolName: 'deleteFile',
      displayName: 'Delete File',
      argsSummary: '{ path: "/x" }',
      args: { path: '/x' },
    };

    const result = toolDisplayFn(approvalEvent, { mode: 'static', platform: 'slack' });
    // It must NOT skip (undefined) — that is the stuck-approval bug.
    expect(result).not.toBeUndefined();
    expect(result).toMatchObject({ kind: 'post' });
    const message = (result as { kind: 'post'; message: unknown }).message;
    expect(message).not.toBeNull();
    // The built-in card carries Approve/Deny actions keyed to the toolCallId so
    // the static driver posts a card with working buttons.
    const serialized = JSON.stringify(message);
    expect(serialized).toContain('tool_approve:tc-approve');
    expect(serialized).toContain('tool_deny:tc-approve');
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
