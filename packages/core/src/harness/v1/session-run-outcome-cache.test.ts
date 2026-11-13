import { describe, expect, it } from 'vitest';

import type { FullOutput } from '../../stream/base/output';
import { MAX_COMPLETED_RUNS, RunOutcomeCache } from './session-run-outcome-cache';

const output = (finishReason: string) => ({ finishReason }) as unknown as FullOutput<unknown>;

describe('RunOutcomeCache', () => {
  it('keeps the first outcome of a run unless that outcome suspended', () => {
    const cache = new RunOutcomeCache(() => {});
    const first = new Error('first');
    cache.remember('failed', { ok: false, err: first });
    cache.remember('failed', { ok: false, err: new Error('later') });
    cache.remember('failed', { ok: true, full: output('stop') });
    expect(cache.get('failed')).toEqual({ ok: false, err: first });

    const done = output('stop');
    cache.remember('done', { ok: true, full: done });
    cache.remember('done', { ok: false, err: new Error('later') });
    expect(cache.get('done')).toEqual({ ok: true, full: done });

    const resumed = output('stop');
    cache.remember('parked', { ok: true, full: output('suspended') });
    cache.remember('parked', { ok: true, full: resumed });
    expect(cache.get('parked')).toEqual({ ok: true, full: resumed });
    expect(cache.has('parked')).toBe(true);
    expect(cache.has('never')).toBe(false);
    expect(cache.get('never')).toBeUndefined();
  });

  it('evicts the oldest run beyond the bound and reports it after it left the cache', () => {
    const evicted: Array<{ runId: string; stillCached: boolean }> = [];
    const cache: RunOutcomeCache = new RunOutcomeCache(runId => evicted.push({ runId, stillCached: cache.has(runId) }));
    cache.remember('parked', { ok: true, full: output('suspended') });
    for (let i = 1; i < MAX_COMPLETED_RUNS; i++) cache.remember(`run-${i}`, { ok: true, full: output('stop') });
    expect(evicted).toEqual([]);
    // Replacing the suspended outcome makes that run the newest entry.
    cache.remember('parked', { ok: true, full: output('stop') });
    cache.remember('one-more', { ok: true, full: output('stop') });
    expect(evicted).toEqual([{ runId: 'run-1', stillCached: false }]);
    expect(cache.has('parked')).toBe(true);
    expect(cache.has('run-1')).toBe(false);
  });

  it("marks a completed outcome's accounted resume generation, forgotten when the outcome is replaced", () => {
    const cache = new RunOutcomeCache(() => {});
    cache.noteResumeAccounted('run', 'gen-a');
    expect(cache.isResumeAccounted('run', 'gen-a')).toBe(false);

    cache.remember('run', { ok: true, full: output('suspended') });
    cache.noteResumeAccounted('run', 'gen-a');
    expect(cache.isResumeAccounted('run', 'gen-a')).toBe(true);
    expect(cache.isResumeAccounted('run', 'gen-b')).toBe(false);
    cache.remember('run', { ok: true, full: output('stop') });
    expect(cache.isResumeAccounted('run', 'gen-a')).toBe(false);

    cache.remember('failed', { ok: false, err: new Error('x') });
    cache.noteResumeAccounted('failed', 'gen-a');
    expect(cache.isResumeAccounted('failed', 'gen-a')).toBe(false);
  });

  it("demotes a run's own failure to an ordinary failure, and reports whether the run is cached", () => {
    const cache = new RunOutcomeCache(() => {});
    expect(cache.demoteRunFailure('never')).toBe(false);
    const err = new Error('provider');
    cache.remember('failed', { ok: false, err, runFailed: true });
    expect(cache.demoteRunFailure('failed')).toBe(true);
    expect(cache.get('failed')).toEqual({ ok: false, err });
    const full = output('stop');
    cache.remember('done', { ok: true, full });
    expect(cache.demoteRunFailure('done')).toBe(true);
    expect(cache.get('done')).toEqual({ ok: true, full });
  });
});
