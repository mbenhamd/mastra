import { describe, expect, it } from 'vitest';

import type { FullOutput } from '../../stream/base/output';

import { RunCompletionWaiters } from './session-run-completion-waiters';

const output = (text: string) => ({ text }) as unknown as FullOutput<unknown>;

/** 'pending' if the promise has not settled once queued microtasks ran, else how it settled. */
async function state(promise: Promise<unknown>): Promise<unknown> {
  const pending = Symbol('pending');
  const settled = await Promise.race([
    promise.then(
      value => ({ resolved: value }),
      error => ({ rejected: error }),
    ),
    new Promise(resolve => setTimeout(() => resolve(pending), 0)),
  ]);
  return settled === pending ? 'pending' : settled;
}

describe('RunCompletionWaiters', () => {
  it('shares one promise between the callers that register for the same run', () => {
    const waiters = new RunCompletionWaiters();
    const first = waiters.register('run-1');
    const second = waiters.register('run-1');
    expect(first.created).toBe(true);
    expect(second.created).toBe(false);
    expect(second.promise).toBe(first.promise);
    expect(waiters.promiseFor('run-1')).toBe(first.promise);
    expect(waiters.has('run-1')).toBe(true);
    expect(waiters.register('run-2').promise).not.toBe(first.promise);
  });

  it('rejects the waiter it removes when a run fails, and a later register starts a new one', async () => {
    const waiters = new RunCompletionWaiters();
    const { promise } = waiters.register('run-1');
    const err = new Error('provider down');
    waiters.fail('run-1', err);
    expect(waiters.has('run-1')).toBe(false);
    await expect(state(promise)).resolves.toEqual({ rejected: err });
    expect(waiters.register('run-1').created).toBe(true);
    expect(() => waiters.fail('run-unknown', err)).not.toThrow();
  });

  it('settles a named waiter only while it is still the run’s waiter', async () => {
    const waiters = new RunCompletionWaiters();
    const old = waiters.register('run-1').promise;
    waiters.fail('run-1', new Error('first attempt'));
    void old.catch(() => {});
    const current = waiters.register('run-1').promise;

    waiters.resolveIfCurrent('run-1', old, output('stale'));
    waiters.rejectIfCurrent('run-1', old, new Error('stale'));
    waiters.resolveIfCurrent('run-1', undefined, output('none'));
    expect(waiters.promiseFor('run-1')).toBe(current);
    await expect(state(current)).resolves.toBe('pending');

    const full = output('done');
    waiters.resolveIfCurrent('run-1', current, full);
    expect(waiters.has('run-1')).toBe(false);
    await expect(state(current)).resolves.toEqual({ resolved: full });

    const next = waiters.register('run-2').promise;
    const err = new Error('staging failed');
    waiters.rejectIfCurrent('run-2', next, err);
    expect(waiters.has('run-2')).toBe(false);
    await expect(state(next)).resolves.toEqual({ rejected: err });
  });

  it('drops only the waiter its caller names, leaving another registrant’s waiter alone', async () => {
    const waiters = new RunCompletionWaiters();
    const old = waiters.register('run-1').promise;
    waiters.fail('run-1', new Error('first attempt'));
    void old.catch(() => {});
    const current = waiters.register('run-1').promise;

    waiters.dropIfCurrent('run-1', old);
    waiters.dropIfCurrent('run-1', undefined);
    expect(waiters.promiseFor('run-1')).toBe(current);

    waiters.dropIfCurrent('run-1', current);
    expect(waiters.has('run-1')).toBe(false);
    await expect(state(current)).resolves.toBe('pending');
  });

  it('rejects every waiter the predicate does not keep, in registration order, with the same reason', async () => {
    const waiters = new RunCompletionWaiters();
    const a = waiters.register('run-a').promise;
    const draining = waiters.register('run-draining').promise;
    const b = waiters.register('run-b').promise;
    const order: string[] = [];
    void a.catch(() => order.push('run-a'));
    void b.catch(() => order.push('run-b'));
    const reason = new Error('session closed');

    waiters.rejectAllExcept(runId => runId === 'run-draining', reason);

    expect(waiters.has('run-a')).toBe(false);
    expect(waiters.has('run-b')).toBe(false);
    expect(waiters.promiseFor('run-draining')).toBe(draining);
    await expect(state(a)).resolves.toEqual({ rejected: reason });
    await expect(state(b)).resolves.toEqual({ rejected: reason });
    await expect(state(draining)).resolves.toBe('pending');
    expect(order).toEqual(['run-a', 'run-b']);
  });
});
