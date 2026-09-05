import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import {
  computeDelay,
  isTransientLLMError,
  ObservationalMemoryOperationTimeoutError,
  RETRY_CONFIG,
  withRetry,
} from '../retry';

describe('isTransientLLMError', () => {
  it('matches undici "terminated" error messages', () => {
    expect(isTransientLLMError(new TypeError('terminated'))).toBe(true);
    expect(isTransientLLMError(new Error('TypeError: terminated'))).toBe(true);
  });

  it('matches undici UND_ERR_* error codes', () => {
    const err = Object.assign(new Error('something bad'), { code: 'UND_ERR_SOCKET' });
    expect(isTransientLLMError(err)).toBe(true);
  });

  it('matches common transport substrings', () => {
    expect(isTransientLLMError(new Error('fetch failed'))).toBe(true);
    expect(isTransientLLMError(new Error('ECONNRESET'))).toBe(true);
    expect(isTransientLLMError(new Error('socket hang up'))).toBe(true);
    expect(isTransientLLMError(new Error('connection closed'))).toBe(true);
    expect(isTransientLLMError(new Error('Request timeout'))).toBe(true);
    expect(isTransientLLMError(new Error('Connection terminated due to connection timeout'))).toBe(true);
  });

  it('matches retryable HTTP statuses', () => {
    expect(isTransientLLMError({ statusCode: 500 })).toBe(true);
    expect(isTransientLLMError({ statusCode: 502 })).toBe(true);
    expect(isTransientLLMError({ statusCode: 429 })).toBe(true);
    expect(isTransientLLMError({ statusCode: 408 })).toBe(true);
  });

  it('matches AI SDK-style isRetryable: true', () => {
    expect(isTransientLLMError({ isRetryable: true })).toBe(true);
  });

  it('matches OpenRouter-style mid-stream errors with a numeric code property', () => {
    // OpenRouter injects provider errors into the SSE stream as
    // { code: 502, message, metadata: { error_type: 'provider_unavailable' } }
    // and Mastra's getErrorFromUnknown copies those props onto the Error.
    const err = Object.assign(new Error('JSON error injected into SSE stream'), {
      code: 502,
      metadata: { error_type: 'provider_unavailable' },
    });
    expect(isTransientLLMError(err)).toBe(true);
    expect(isTransientLLMError({ code: 429, message: 'rate limited' })).toBe(true);
  });

  it('does NOT retry on non-retryable numeric code properties', () => {
    expect(isTransientLLMError({ code: 400, message: 'bad request' })).toBe(false);
    expect(isTransientLLMError({ code: 401, message: 'unauthorized' })).toBe(false);
  });

  it('walks the error.cause chain', () => {
    const cause = new TypeError('terminated');
    const wrapper = new Error('agent stream failed');
    (wrapper as any).cause = cause;
    expect(isTransientLLMError(wrapper)).toBe(true);
  });

  it('walks the error.error chain (some AI SDK wrappers)', () => {
    const inner = Object.assign(new Error('boom'), { code: 'UND_ERR_CONNECT_TIMEOUT' });
    const wrapper = new Error('wrapped');
    (wrapper as any).error = inner;
    expect(isTransientLLMError(wrapper)).toBe(true);
  });

  it('does NOT retry on AbortError', () => {
    const err = new Error('cancelled');
    (err as any).name = 'AbortError';
    expect(isTransientLLMError(err)).toBe(false);
  });

  it('does NOT retry on DOMException-style abort code', () => {
    expect(isTransientLLMError({ name: 'Error', code: 'ABORT_ERR' })).toBe(false);
  });

  it('does NOT retry on auth / validation / 4xx errors', () => {
    expect(isTransientLLMError({ statusCode: 401 })).toBe(false);
    expect(isTransientLLMError({ statusCode: 403 })).toBe(false);
    expect(isTransientLLMError({ statusCode: 400 })).toBe(false);
    expect(isTransientLLMError({ statusCode: 404 })).toBe(false);
    expect(isTransientLLMError({ statusCode: 422 })).toBe(false);
  });

  it('does NOT retry on plain errors with non-transport messages', () => {
    expect(isTransientLLMError(new Error('invalid api key'))).toBe(false);
    expect(isTransientLLMError(new Error('schema validation failed'))).toBe(false);
  });

  it('handles non-Error / non-object values without throwing', () => {
    expect(isTransientLLMError(undefined)).toBe(false);
    expect(isTransientLLMError(null)).toBe(false);
    expect(isTransientLLMError('terminated')).toBe(false);
    expect(isTransientLLMError(42)).toBe(false);
  });

  it('handles cycles in cause chains', () => {
    const a: any = new Error('a');
    const b: any = new Error('b');
    a.cause = b;
    b.cause = a;
    // Should not stack-overflow; both messages are non-transient.
    expect(isTransientLLMError(a)).toBe(false);
  });
});

describe('default retry schedule', () => {
  // Lock the single-owner retry budget so provider attempts and tail latency do
  // not silently grow again.

  const defaults = {
    maxRetries: 2,
    initialDelayMs: 1_000,
    backoffFactor: 2,
    maxDelayMs: 2_000,
    timeoutMs: 60_000,
  };

  it('has the expected default config', () => {
    expect(RETRY_CONFIG.maxRetries).toBe(defaults.maxRetries);
    expect(RETRY_CONFIG.initialDelayMs).toBe(defaults.initialDelayMs);
    expect(RETRY_CONFIG.backoffFactor).toBe(defaults.backoffFactor);
    expect(RETRY_CONFIG.maxDelayMs).toBe(defaults.maxDelayMs);
    expect(RETRY_CONFIG.timeoutMs).toBe(defaults.timeoutMs);
  });

  it('produces the expected pre-jitter schedule and total budget', () => {
    const originalJitter = RETRY_CONFIG.jitter;
    RETRY_CONFIG.jitter = 0;
    try {
      const expectedSchedule = [1_000, 2_000];
      const actualSchedule: number[] = [];
      for (let attempt = 0; attempt < RETRY_CONFIG.maxRetries; attempt++) {
        actualSchedule.push(computeDelay(attempt));
      }
      expect(actualSchedule).toEqual(expectedSchedule);

      // Final retry delay reaches the configured cap.
      expect(actualSchedule[actualSchedule.length - 1]).toBe(RETRY_CONFIG.maxDelayMs);

      // Three provider attempts have only three seconds of pre-jitter backoff.
      const totalBudgetMs = actualSchedule.reduce((a, b) => a + b, 0);
      expect(totalBudgetMs).toBe(3_000);
      expect(totalBudgetMs).toBeLessThan(RETRY_CONFIG.timeoutMs);
    } finally {
      RETRY_CONFIG.jitter = originalJitter;
    }
  });
});

describe('withRetry', () => {
  const originalConfig = { ...RETRY_CONFIG };

  beforeEach(() => {
    // Shrink the schedule so tests are fast — but keep relative shape.
    RETRY_CONFIG.initialDelayMs = 1;
    RETRY_CONFIG.maxDelayMs = 4;
    RETRY_CONFIG.backoffFactor = 2;
    RETRY_CONFIG.jitter = 0;
    RETRY_CONFIG.maxRetries = 3;
    RETRY_CONFIG.timeoutMs = 10_000;
  });

  afterEach(() => {
    vi.useRealTimers();
    Object.assign(RETRY_CONFIG, originalConfig);
  });

  it('returns the value on first success without retrying', async () => {
    const fn = vi.fn().mockResolvedValue('ok');
    await expect(withRetry(fn, { label: 'test' })).resolves.toBe('ok');
    expect(fn).toHaveBeenCalledTimes(1);
  });

  it('retries on transient errors and eventually succeeds', async () => {
    vi.useFakeTimers();
    RETRY_CONFIG.initialDelayMs = 1_000;
    RETRY_CONFIG.maxDelayMs = 2_000;
    const fn = vi
      .fn()
      .mockRejectedValueOnce(new TypeError('terminated'))
      .mockRejectedValueOnce(new TypeError('fetch failed'))
      .mockResolvedValue('ok');

    const result = withRetry(fn, { label: 'test' });
    await vi.advanceTimersByTimeAsync(0);
    expect(fn).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(999);
    expect(fn).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(1);
    expect(fn).toHaveBeenCalledTimes(2);
    await vi.advanceTimersByTimeAsync(1_999);
    expect(fn).toHaveBeenCalledTimes(2);
    await vi.advanceTimersByTimeAsync(1);

    await expect(result).resolves.toBe('ok');
    expect(fn).toHaveBeenCalledTimes(3);
  });

  it('gives up after maxRetries on persistent transient errors', async () => {
    const fn = vi.fn().mockRejectedValue(new TypeError('terminated'));

    await expect(withRetry(fn, { label: 'test' })).rejects.toThrow('terminated');
    // initial + maxRetries
    expect(fn).toHaveBeenCalledTimes(RETRY_CONFIG.maxRetries + 1);
  });

  it('rethrows non-transient errors immediately without retrying', async () => {
    const fn = vi.fn().mockRejectedValue(new Error('invalid api key'));

    await expect(withRetry(fn, { label: 'test' })).rejects.toThrow('invalid api key');
    expect(fn).toHaveBeenCalledTimes(1);
  });

  it('rethrows AbortError immediately without retrying', async () => {
    const abortErr = new Error('cancelled');
    (abortErr as any).name = 'AbortError';
    const fn = vi.fn().mockRejectedValue(abortErr);

    await expect(withRetry(fn, { label: 'test' })).rejects.toBe(abortErr);
    expect(fn).toHaveBeenCalledTimes(1);
  });

  it('throws when abortSignal is already aborted before the first attempt', async () => {
    const controller = new AbortController();
    controller.abort();
    const fn = vi.fn().mockResolvedValue('ok');

    await expect(withRetry(fn, { label: 'test', abortSignal: controller.signal })).rejects.toThrow(/aborted/);
    expect(fn).not.toHaveBeenCalled();
  });

  it('stops retrying once abortSignal fires mid-backoff', async () => {
    vi.useFakeTimers();
    const controller = new AbortController();
    RETRY_CONFIG.initialDelayMs = 1_000;
    RETRY_CONFIG.maxDelayMs = 2_000;

    const fn = vi.fn().mockRejectedValue(new TypeError('terminated'));

    const promise = withRetry(fn, { label: 'test', abortSignal: controller.signal });
    const rejection = expect(promise).rejects.toThrow(/aborted/i);
    await vi.advanceTimersByTimeAsync(0);
    controller.abort();

    await rejection;
    // Exactly one attempt happened before we aborted the backoff wait.
    expect(fn).toHaveBeenCalledTimes(1);
  });

  it('cancels an in-flight attempt at the total operation deadline', async () => {
    vi.useFakeTimers();
    RETRY_CONFIG.timeoutMs = 50;
    let attemptSignal: AbortSignal | undefined;
    const fn = vi.fn((signal: AbortSignal) => {
      attemptSignal = signal;
      // Deliberately ignore cancellation: the deadline race must still release
      // the caller while propagating the aborted signal to the provider.
      return new Promise<never>(() => {});
    });

    const promise = withRetry(fn, { label: 'observer' });
    const rejection = expect(promise).rejects.toBeInstanceOf(ObservationalMemoryOperationTimeoutError);
    await vi.advanceTimersByTimeAsync(49);
    expect(fn).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(1);

    await rejection;
    expect(fn).toHaveBeenCalledTimes(1);
    expect(attemptSignal?.aborted).toBe(true);
    expect(attemptSignal?.reason).toBeInstanceOf(ObservationalMemoryOperationTimeoutError);
  });

  it('uses one total deadline across attempts and backoff', async () => {
    vi.useFakeTimers();
    RETRY_CONFIG.maxRetries = 5;
    RETRY_CONFIG.initialDelayMs = 1_000;
    RETRY_CONFIG.maxDelayMs = 2_000;
    RETRY_CONFIG.timeoutMs = 1_500;
    const fn = vi.fn().mockRejectedValue(new TypeError('terminated'));

    const promise = withRetry(fn, { label: 'reflector' });
    const rejection = expect(promise).rejects.toThrow('timed out after 1500ms');
    await vi.advanceTimersByTimeAsync(1_000);
    expect(fn).toHaveBeenCalledTimes(2);
    await vi.advanceTimersByTimeAsync(500);

    await rejection;
    expect(fn).toHaveBeenCalledTimes(2);
  });
});
