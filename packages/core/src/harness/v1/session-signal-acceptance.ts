/**
 * Native signal acceptance bound, shared by `Session` signal dispatch and the
 * settlement-notice delivery that recognises its timeout (PF-8326).
 */

import { HarnessConfigError, HarnessValidationError } from './errors';

export const SIGNAL_NATIVE_ACCEPT_TIMEOUT_MS = 30_000;

export class NativeSignalAcceptanceTimeoutError extends HarnessValidationError {
  constructor(validationPath: string) {
    super(validationPath, `native signal acceptance timed out after ${SIGNAL_NATIVE_ACCEPT_TIMEOUT_MS}ms`);
  }
}

export const SIGNAL_DISPATCH_CLAIM_TTL_MS = 30_000;

export function lineagedSignalAcceptanceError(path: string, action: string): HarnessConfigError {
  if (action === 'discard' || action === 'deliver') {
    return new HarnessConfigError(path, 'a full logical message identity cannot be delivered into an active run');
  }
  if (action === 'blocked') {
    return new HarnessConfigError(path, 'a full logical message identity was blocked by a suspended thread');
  }
  return new HarnessConfigError(path, `a full logical message identity was not accepted (${action})`);
}

export function lineagedSignalRunMismatchError(
  path: string,
  expectedRunId: string,
  actualRunId: string,
): HarnessConfigError {
  return new HarnessConfigError(
    path,
    `a full logical message identity was accepted by run "${actualRunId}" instead of its reserved run "${expectedRunId}"`,
  );
}
