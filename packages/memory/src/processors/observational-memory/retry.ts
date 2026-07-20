import { omDebug } from './debug';

/**
 * Retry knobs for the internal OM transport-error retry wrapper.
 * Exported as a mutable object so tests can shrink the backoff schedule
 * without changing public API.
 *
 * OM owns retries for Observer and Reflector calls. Those calls disable the
 * model layer's retries so a logical operation has one retry budget instead of
 * multiplying this schedule by the provider retry count.
 *
 * With the defaults the per-retry pre-jitter backoff schedule is 1s, 2s,
 * giving 2 retries / 3 total provider attempts and 3s of pre-jitter backoff.
 * The total operation deadline also bounds slow in-flight attempts.
 *
 * @internal
 */
export const RETRY_CONFIG = {
  /** Maximum number of retry *attempts* (total tries = maxRetries + 1). */
  maxRetries: 2,
  /** Initial backoff delay in milliseconds. */
  initialDelayMs: 1_000,
  /** Multiplier applied to the delay after each failed attempt. */
  backoffFactor: 2,
  /** Cap on per-attempt delay (ms). */
  maxDelayMs: 2_000,
  /** Random jitter as a fraction of the computed delay (e.g. 0.2 = ±20%). */
  jitter: 0.2,
  /** Total deadline for attempts plus backoff. */
  timeoutMs: 60_000,
};

const TRANSIENT_MESSAGE_SUBSTRINGS = [
  'terminated',
  'fetch failed',
  'econnreset',
  'econnrefused',
  'enotfound',
  'eai_again',
  'socket hang up',
  'network error',
  'request timed out',
  'request timeout',
  'connection reset',
  'connection closed',
];

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null;
}

function isAbortError(error: unknown): boolean {
  if (!isRecord(error)) return false;
  if (error.name === 'AbortError') return true;
  // DOMException-style abort
  if (typeof error.code === 'string' && error.code === 'ABORT_ERR') return true;
  return false;
}

function hasTransientMessage(value: unknown): boolean {
  if (!isRecord(value)) return false;
  const message = typeof value.message === 'string' ? value.message.toLowerCase() : '';
  if (message && TRANSIENT_MESSAGE_SUBSTRINGS.some(sub => message.includes(sub))) return true;
  if (typeof value.code === 'string' && value.code.toUpperCase().startsWith('UND_ERR_')) return true;
  return false;
}

function isRetryableHttpStatus(status: number): boolean {
  if (status === 408 || status === 425 || status === 429) return true;
  return status >= 500 && status <= 599;
}

function hasRetryableHttpStatus(value: unknown): boolean {
  if (!isRecord(value)) return false;
  if (typeof value.statusCode === 'number' && isRetryableHttpStatus(value.statusCode)) return true;
  // OpenRouter-style mid-stream SSE errors carry the HTTP status on a numeric
  // `code` property (e.g. { code: 502, message: 'JSON error injected into SSE
  // stream', metadata: { error_type: 'provider_unavailable' } }).
  if (typeof value.code === 'number' && isRetryableHttpStatus(value.code)) return true;
  return false;
}

function hasIsRetryableFlag(value: unknown): boolean {
  if (!isRecord(value)) return false;
  return value.isRetryable === true;
}

/**
 * Returns true when the given error looks like a transient transport-class
 * failure that's worth retrying — undici `terminated`, `fetch failed`,
 * `UND_ERR_*` codes, AI SDK `APICallError` with `isRetryable: true`, and
 * common HTTP 408/425/429/5xx statuses. Walks the `error.cause` chain so
 * wrapper errors don't hide the real failure.
 *
 * Never retries on user-initiated aborts.
 *
 * @internal
 */
export function isTransientLLMError(error: unknown): boolean {
  if (isAbortError(error)) return false;

  const visited = new WeakSet<object>();

  function visit(candidate: unknown): boolean {
    if (isRecord(candidate)) {
      if (visited.has(candidate)) return false;
      visited.add(candidate);
    }

    if (hasTransientMessage(candidate)) return true;
    if (hasRetryableHttpStatus(candidate)) return true;
    if (hasIsRetryableFlag(candidate)) return true;

    if (isRecord(candidate)) {
      if (visit(candidate.cause)) return true;
      // Some libraries wrap the original error under `.error` instead of `.cause`.
      if (visit(candidate.error)) return true;
    }

    return false;
  }

  return visit(error);
}

/**
 * Compute the backoff delay (ms) for the Nth retry (0-indexed).
 *
 * Exponential growth (`initialDelayMs * backoffFactor^attempt`) capped at
 * `maxDelayMs`, then nudged by ±`jitter` (fractional). Exported for unit
 * tests that lock the schedule against drift.
 *
 * @internal
 */
export function computeDelay(attempt: number): number {
  const base = RETRY_CONFIG.initialDelayMs * Math.pow(RETRY_CONFIG.backoffFactor, attempt);
  const capped = Math.min(base, RETRY_CONFIG.maxDelayMs);
  if (RETRY_CONFIG.jitter <= 0) return capped;
  const jitterRange = capped * RETRY_CONFIG.jitter;
  // Symmetric jitter in [-jitterRange, +jitterRange].
  const offset = (Math.random() * 2 - 1) * jitterRange;
  return Math.max(0, Math.round(capped + offset));
}

function abortReason(abortSignal: AbortSignal): Error {
  return abortSignal.reason instanceof Error ? abortSignal.reason : new Error('The operation was aborted.');
}

function sleep(ms: number, abortSignal?: AbortSignal): Promise<void> {
  if (ms <= 0) return Promise.resolve();
  return new Promise<void>((resolve, reject) => {
    if (abortSignal?.aborted) {
      reject(abortReason(abortSignal));
      return;
    }
    const timer = setTimeout(() => {
      abortSignal?.removeEventListener('abort', onAbort);
      resolve();
    }, ms);
    const onAbort = () => {
      clearTimeout(timer);
      abortSignal?.removeEventListener('abort', onAbort);
      reject(abortSignal ? abortReason(abortSignal) : new Error('The operation was aborted.'));
    };
    abortSignal?.addEventListener('abort', onAbort, { once: true });
  });
}

export interface WithRetryOptions {
  /** Short label used in debug logs (e.g. 'observer', 'reflector'). */
  label: string;
  /** Optional abort signal — cancels both in-flight attempts and backoff waits. */
  abortSignal?: AbortSignal;
  /** Total deadline for attempts plus backoff. Defaults to RETRY_CONFIG.timeoutMs. */
  timeoutMs?: number;
}

export class ObservationalMemoryOperationTimeoutError extends Error {
  readonly code = 'OM_OPERATION_TIMEOUT';

  constructor(label: string, timeoutMs: number) {
    super(`Observational Memory ${label} timed out after ${timeoutMs}ms`);
    this.name = 'ObservationalMemoryOperationTimeoutError';
  }
}

function raceWithAbort<T>(operation: Promise<T>, abortSignal: AbortSignal): Promise<T> {
  if (abortSignal.aborted) return Promise.reject(abortReason(abortSignal));

  return new Promise<T>((resolve, reject) => {
    const onAbort = () => reject(abortReason(abortSignal));
    abortSignal.addEventListener('abort', onAbort, { once: true });
    operation.then(resolve, reject).finally(() => {
      abortSignal.removeEventListener('abort', onAbort);
    });
  });
}

/**
 * Run `fn` with retries on transient transport-class errors.
 *
 * Non-transient errors (auth, validation, schema, etc.) are rethrown
 * immediately. User-initiated aborts are rethrown without delay.
 *
 * @internal
 */
export async function withRetry<T>(fn: (abortSignal: AbortSignal) => Promise<T>, opts: WithRetryOptions): Promise<T> {
  const { label, abortSignal } = opts;
  const timeoutMs = opts.timeoutMs ?? RETRY_CONFIG.timeoutMs;
  if (!Number.isFinite(timeoutMs) || timeoutMs < 0) {
    throw new Error(`Observational Memory ${label} timeoutMs must be a finite non-negative number`);
  }

  const operationController = new AbortController();
  const onCallerAbort = () => operationController.abort(abortSignal?.reason);
  if (abortSignal?.aborted) {
    operationController.abort(abortSignal.reason);
  } else {
    abortSignal?.addEventListener('abort', onCallerAbort, { once: true });
  }
  const timeoutId = setTimeout(() => {
    operationController.abort(new ObservationalMemoryOperationTimeoutError(label, timeoutMs));
  }, timeoutMs);
  timeoutId.unref?.();
  const operationSignal = operationController.signal;
  let attempt = 0;

  try {
    // total tries = maxRetries + 1 (the initial attempt isn't a "retry")
    while (true) {
      if (operationSignal.aborted) {
        throw abortReason(operationSignal);
      }
      try {
        return await raceWithAbort(
          Promise.resolve().then(() => fn(operationSignal)),
          operationSignal,
        );
      } catch (error) {
        if (operationSignal.aborted) throw abortReason(operationSignal);
        if (isAbortError(error)) throw error;
        if (attempt >= RETRY_CONFIG.maxRetries || !isTransientLLMError(error)) {
          if (attempt > 0) {
            omDebug(
              `[OM:retry:${label}] giving up after ${attempt} retry/retries: ${
                error instanceof Error ? error.message : String(error)
              }`,
            );
          }
          throw error;
        }
        const delay = computeDelay(attempt);
        attempt++;
        omDebug(
          `[OM:retry:${label}] transient error on attempt ${attempt}, retrying in ${delay}ms: ${
            error instanceof Error ? error.message : String(error)
          }`,
        );
        await sleep(delay, operationSignal);
      }
    }
  } finally {
    clearTimeout(timeoutId);
    abortSignal?.removeEventListener('abort', onCallerAbort);
  }
}
