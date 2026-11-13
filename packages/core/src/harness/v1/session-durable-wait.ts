/**
 * How long, and with which abortable primitives, `Session` waits for a durable
 * outcome: admission and result-evidence waits, accepted-dispatch staleness,
 * post-run finalization and orphaned-dispatch retries.
 */

import { HarnessValidationError } from './errors';

export type Deferred<T> = {
  promise: Promise<T>;
  resolve: (value: T) => void;
  reject: (reason: unknown) => void;
};

export const MESSAGE_ADMISSION_DURABLE_WAIT_TIMEOUT_MS = 30_000;
export const MESSAGE_ADMISSION_DURABLE_WAIT_INTERVAL_MS = 100;
export const MESSAGE_ADMISSION_DURABLE_WAIT_EXTENDED_INTERVAL_MS = 1_000;
export const MESSAGE_ADMISSION_DURABLE_WAIT_MAX_INTERVAL_MS = 15_000;

export const MESSAGE_RESULT_EVIDENCE_BACKGROUND_OBSERVE_TIMEOUT_MS = 5_000;
export const QUEUE_ACCEPTED_RECOVERY_STALE_MS = 30_000;
export const QUEUE_POST_RUN_FINALIZATION_RETRY_MS = 1_000;

/** Back-off before recovery retries a turn whose settlement did not commit. */
export const ORPHANED_DISPATCH_RETRY_MS = 1_000;

export function throwIfAborted(signal: AbortSignal | undefined, path: string): void {
  if (!signal?.aborted) return;
  throw signal.reason ?? new HarnessValidationError(path, 'operation aborted');
}

export function delay(ms: number, signal?: AbortSignal): Promise<void> {
  throwIfAborted(signal, 'delay()');
  return new Promise((resolve, reject) => {
    let timer: ReturnType<typeof setTimeout>;
    const onAbort = () => {
      clearTimeout(timer);
      reject(signal?.reason ?? new HarnessValidationError('delay()', 'operation aborted'));
    };
    timer = setTimeout(() => {
      signal?.removeEventListener('abort', onAbort);
      resolve();
    }, ms);
    signal?.addEventListener('abort', onAbort, { once: true });
  });
}

export function createDeferred<T>(): Deferred<T> {
  let resolve!: (value: T) => void;
  let reject!: (reason: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}
