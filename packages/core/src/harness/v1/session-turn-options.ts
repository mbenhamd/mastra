import type { AgentExecutionOptionsBase } from '../../agent/agent.types';
import { normalizeLogicalMessageInputIdentity } from '../../agent/message-list';
import type { LogicalMessageSignalIdentity } from '../../agent/message-list';
import { HarnessTerminalHandoffValidationError, isHarnessWakeReservedKey } from '../../storage/domains/harness';
import type { PendingResume } from '../../storage/domains/harness';
import { HarnessValidationError } from './errors';

export type HarnessIterationCompleteHandler = NonNullable<AgentExecutionOptionsBase<unknown>['onIterationComplete']>;

export type HarnessPrepareStep = NonNullable<AgentExecutionOptionsBase<unknown>['prepareStep']>;

export type HarnessExecutionHookErrorReporter = (hook: 'onIterationComplete' | 'prepareStep', error: unknown) => void;

export class HarnessResponseRecoveryAlreadyPreparedError extends Error {
  constructor() {
    super('Harness response recovery has already admitted its one provider attempt');
    this.name = 'HarnessResponseRecoveryAlreadyPreparedError';
  }
}

const MAX_INBOX_RECEIPT_RESULT_BYTES = 64_000;

export function boundInboxReceiptResult(full: unknown): unknown {
  try {
    const serialized = JSON.stringify(full);
    if (serialized === undefined || Buffer.byteLength(serialized, 'utf8') <= MAX_INBOX_RECEIPT_RESULT_BYTES) {
      return full;
    }
  } catch {
    // Unserializable results cannot be persisted faithfully either way.
  }
  const source = (full ?? {}) as { finishReason?: unknown; text?: unknown; usage?: unknown };
  return {
    truncated: true,
    ...(source.finishReason === undefined ? {} : { finishReason: source.finishReason }),
    ...(typeof source.text === 'string' ? { text: source.text.slice(0, 8_000) } : {}),
    ...(source.usage === undefined ? {} : { usage: source.usage }),
  };
}

export function assertCallerGrantKeyNotReserved(field: string, grant: { key: string } | undefined): void {
  if (grant !== undefined && isHarnessWakeReservedKey(grant.key)) {
    throw new HarnessTerminalHandoffValidationError(field, 'uses the Harness-reserved wake namespace');
  }
}

export function normalizeSessionLogicalMessageInputIdentity(
  value: LogicalMessageSignalIdentity | undefined,
  methodName: string,
): LogicalMessageSignalIdentity | undefined {
  try {
    return normalizeLogicalMessageInputIdentity(value);
  } catch (error) {
    throw new HarnessValidationError(
      `${methodName}.logicalMessageIdentity`,
      error instanceof Error ? error.message : 'must contain a valid input id',
    );
  }
}

/** Shared between `_resume` and `_resumeAnswer` for discard reclassification. */
export interface ResumeAnswerProgress {
  /** Input validation passed; later rejections may be reclassified. */
  validated: boolean;
  /** The pending interaction observed when the answer arrived. */
  pending: PendingResume | undefined;
}
