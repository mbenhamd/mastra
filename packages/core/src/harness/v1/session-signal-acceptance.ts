/**
 * Native signal acceptance bound, shared by `Session` signal dispatch and the
 * settlement-notice delivery that recognises its timeout (PF-8326).
 */

import { HarnessValidationError } from './errors';

export const SIGNAL_NATIVE_ACCEPT_TIMEOUT_MS = 30_000;

export class NativeSignalAcceptanceTimeoutError extends HarnessValidationError {
  constructor(validationPath: string) {
    super(validationPath, `native signal acceptance timed out after ${SIGNAL_NATIVE_ACCEPT_TIMEOUT_MS}ms`);
  }
}
