import { describe, expect, it } from 'vitest';

import { QueueHead } from './session-queue-head';
import type { QueueResolver } from './session-queue-head';
import type { AgentResult } from './types';

/** A real `queue()`-style deferred, counting how often each side is called. */
function deferredResolver(): QueueResolver & { calls: { resolve: number; reject: number } } {
  const calls = { resolve: 0, reject: 0 };
  let resolve!: (result: AgentResult) => void;
  let reject!: (err: unknown) => void;
  const promise = new Promise<AgentResult>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  void promise.catch(() => {});
  return {
    promise,
    resolve: result => {
      calls.resolve++;
      resolve(result);
    },
    reject: err => {
      calls.reject++;
      reject(err);
    },
    calls,
  };
}

/**
 * PF-8365: `QueueHead` owns the session's queue-head state. These pin the
 * invariants the session's queue, cancel and teardown paths rely on.
 */
describe('QueueHead', () => {
  it('holds one head with its source until it is released', () => {
    const head = new QueueHead();
    head.claim('q-1', 'goal');
    expect(head.currentId).toBe('q-1');
    expect(head.currentSource).toBe('goal');

    head.release();
    expect(head.currentId).toBeUndefined();
    expect(head.currentSource).toBeUndefined();
  });

  it('releaseIf clears the head only for the item that holds it', () => {
    const head = new QueueHead();
    head.claim('q-1', 'user');

    head.releaseIf('q-2');
    expect(head.currentId).toBe('q-1');
    expect(head.currentSource).toBe('user');

    head.releaseIf('q-1');
    expect(head.currentId).toBeUndefined();
    expect(head.currentSource).toBeUndefined();
  });

  it('hands out each resolver once, so a queue() caller is settled at most once', () => {
    const head = new QueueHead();
    const first = deferredResolver();
    const second = deferredResolver();
    head.register('q-1', first);
    head.register('q-2', second);

    expect(head.takeResolver('q-1')).toBe(first);
    expect(head.takeResolver('q-1')).toBeUndefined();
    expect(head.hasResolver('q-1')).toBe(false);
    expect(head.hasResolver('q-2')).toBe(true);
    expect(head.peekResolver('q-2')?.promise).toBe(second.promise);
    expect(head.resolverCount).toBe(1);
  });

  it('rejectAll rejects every waiting caller once and leaves nothing registered', async () => {
    const head = new QueueHead();
    const first = deferredResolver();
    const second = deferredResolver();
    head.register('q-1', first);
    head.register('q-2', second);
    const err = new Error('session evicted');

    head.rejectAll(err);
    head.rejectAll(new Error('second teardown'));

    await expect(first.promise).rejects.toBe(err);
    await expect(second.promise).rejects.toBe(err);
    expect(first.calls).toEqual({ resolve: 0, reject: 1 });
    expect(second.calls).toEqual({ resolve: 0, reject: 1 });
    expect(head.resolverCount).toBe(0);
    expect(head.takeResolver('q-1')).toBeUndefined();
  });
});
