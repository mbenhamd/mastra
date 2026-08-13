import { ErrorCategory, ErrorDomain, MastraError } from '../../error';
import type { IMastraLogger } from '../../logger';

export function validateMaxSteps(maxSteps: number | undefined, logger?: IMastraLogger): void {
  if (maxSteps === undefined || (Number.isSafeInteger(maxSteps) && maxSteps >= 1)) {
    return;
  }

  const mastraError = new MastraError({
    id: 'LLM_INVALID_MAX_STEPS',
    domain: ErrorDomain.LLM,
    category: ErrorCategory.USER,
    text: 'maxSteps must be a positive safe integer',
    details: { maxSteps },
  });
  logger?.trackException(mastraError);
  throw mastraError;
}

export function validateRecoveryMaxSteps(recoveryMaxSteps: number | undefined, logger?: IMastraLogger): void {
  if (recoveryMaxSteps === undefined || (Number.isSafeInteger(recoveryMaxSteps) && recoveryMaxSteps >= 0)) {
    return;
  }

  const mastraError = new MastraError({
    id: 'LLM_INVALID_RECOVERY_MAX_STEPS',
    domain: ErrorDomain.LLM,
    category: ErrorCategory.USER,
    text: 'recoveryMaxSteps must be a non-negative safe integer',
    details: { recoveryMaxSteps },
  });
  logger?.trackException(mastraError);
  throw mastraError;
}
