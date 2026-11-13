import { afterEach, describe, expect, it, vi } from 'vitest';

import { RunSpanTracker } from './session-run-span-tracker';

afterEach(() => {
  vi.useRealTimers();
});

describe('RunSpanTracker', () => {
  it('keeps the span already open for a run: a second open neither replaces it nor captures an identity', () => {
    const spans = new RunSpanTracker<string>();
    spans.open('run-1', 100, () => 'first');
    const identity = vi.fn(() => 'second');
    spans.open('run-1', 200, identity);
    expect(identity).not.toHaveBeenCalled();
    expect(spans.snapshot('run-1')).toMatchObject({ startedAt: 100, identity: 'first' });
    expect(spans.snapshot('run-2')).toBeUndefined();
  });

  it("folds ended tool calls into the run's rollup, measuring a duration from the call's recorded start", () => {
    vi.useFakeTimers({ toFake: ['Date'] });
    vi.setSystemTime(1_000);
    const spans = new RunSpanTracker<string>();
    spans.open('run-1', 1_000, () => 'id');
    spans.toolStarted('run-1', 'tc-1', 'lookup');
    vi.setSystemTime(1_040);
    expect(spans.toolEnded('run-1', { toolCallId: 'tc-1', toolName: 'lookup' })).toBe(40);
    // The call's start is forgotten once it ended: a duplicate end has no measured duration.
    expect(spans.toolEnded('run-1', { toolCallId: 'tc-1', toolName: 'lookup', isError: true })).toBeUndefined();
    expect(spans.toolEnded('run-1', { toolCallId: 'tc-2', toolName: 'fetch', durationMs: 7 })).toBe(7);
    expect(spans.snapshot('run-1')?.toolRollup).toEqual({
      count: 3,
      errors: 1,
      totalDurationMs: 47,
      maxDurationMs: 40,
      perTool: {
        lookup: { count: 2, errors: 1, totalDurationMs: 40 },
        fetch: { count: 1, errors: 0, totalDurationMs: 7 },
      },
    });
  });

  it('counts nothing for a run without an open span', () => {
    const spans = new RunSpanTracker<string>();
    spans.toolStarted('run-1', 'tc-1', 'lookup');
    expect(spans.toolEnded('run-1', { toolCallId: 'tc-1', durationMs: 5 })).toBeUndefined();
    expect(spans.toolEnded(undefined, { toolCallId: 'tc-1', durationMs: 5 })).toBeUndefined();
    spans.open('run-1', 0, () => 'id');
    expect(spans.snapshot('run-1')?.toolRollup.count).toBe(0);
  });

  it('finalizes a run once: the first call closes and returns its span, later calls change nothing', () => {
    const spans = new RunSpanTracker<string>();
    spans.open('run-1', 100, () => 'id');
    expect(spans.isFinalized('run-1')).toBe(false);
    expect(spans.finalize('run-1')).toEqual({
      span: {
        startedAt: 100,
        identity: 'id',
        toolRollup: { count: 0, errors: 0, totalDurationMs: 0, maxDurationMs: 0, perTool: {} },
      },
    });
    expect(spans.isFinalized('run-1')).toBe(true);
    expect(spans.snapshot('run-1')).toBeUndefined();
    expect(spans.finalize('run-1')).toBeUndefined();
    // A run finalized with no open span reports none.
    expect(spans.finalize('run-2')).toEqual({ span: undefined });
    expect(spans.finalize('run-2')).toBeUndefined();
  });

  it('forgets every open span and every finalized run on dispose', () => {
    const spans = new RunSpanTracker<string>();
    spans.open('run-1', 100, () => 'id');
    spans.finalize('run-2');
    spans.dispose();
    expect(spans.snapshot('run-1')).toBeUndefined();
    expect(spans.isFinalized('run-2')).toBe(false);
    expect(spans.finalize('run-2')).toEqual({ span: undefined });
  });
});
