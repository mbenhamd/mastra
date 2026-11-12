import type { AgentExecutionOptionsBase } from '../../agent/agent.types';
import { normalizeLogicalMessageIdentity, normalizeLogicalMessageInputIdentity } from '../../agent/message-list';
import type { LogicalMessageIdentity, LogicalMessageSignalIdentity } from '../../agent/message-list';
import { HarnessTerminalHandoffValidationError, isHarnessWakeReservedKey } from '../../storage/domains/harness';
import type { PendingResume, PersistedRequestContextInput } from '../../storage/domains/harness';
import type { ObservationalMemorySourceWriteGuard } from '../../storage/types';
import { HarnessValidationError } from './errors';
import type { JsonValue } from './session';

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

/**
 * Validate and clone the native OM source-write fence at a Harness boundary.
 * The fence is an execution identity, so accepting a partial or malformed
 * value would turn a retry into an unbound write.
 */
export function normalizeObservationalMemorySourceWriteGuard(
  value: ObservationalMemorySourceWriteGuard | undefined,
  field: string,
): ObservationalMemorySourceWriteGuard | undefined {
  if (value === undefined) return undefined;
  if (value === null || typeof value !== 'object' || Array.isArray(value)) {
    throw new HarnessValidationError(field, 'must be an object with recordId, threadId, and resourceId');
  }
  if (typeof value.recordId !== 'string' || value.recordId.length === 0) {
    throw new HarnessValidationError(`${field}.recordId`, 'must be a non-empty string');
  }
  if (value.threadId !== null && typeof value.threadId !== 'string') {
    throw new HarnessValidationError(`${field}.threadId`, 'must be a string or null');
  }
  if (typeof value.resourceId !== 'string' || value.resourceId.length === 0) {
    throw new HarnessValidationError(`${field}.resourceId`, 'must be a non-empty string');
  }
  return {
    recordId: value.recordId,
    threadId: value.threadId,
    resourceId: value.resourceId,
  };
}

export function sameObservationalMemorySourceWriteGuard(
  left: ObservationalMemorySourceWriteGuard | undefined,
  right: ObservationalMemorySourceWriteGuard | undefined,
): boolean {
  return (
    left?.recordId === right?.recordId && left?.threadId === right?.threadId && left?.resourceId === right?.resourceId
  );
}

// Step ceiling for every harness-driven agent turn. Without an explicit
// `maxSteps`, `agent.stream()`/`agent.generate()` fall back to the agent's
// small default (5), and a tool-heavy turn terminates CLEANLY at the step
// boundary with no synthesis text — live-diagnosed as a silent empty chat
// turn: 5 tool-only steps, `finish-step`×5, `finish`, zero text deltas
// (models that batch small parallel tool calls burn the budget fastest).
// agent-controller.ts centralizes the same guarantee for CLI-harness runs
// (CONTROLLER_MAX_STEPS); the harness governs runaway turns through its own
// budgets and abort lanes, so this is an escape-proof ceiling, not pacing.
export const HARNESS_SESSION_MAX_STEPS = 1000;

/**
 * PF-7631: the `harness-wake:` namespace is minted only by the Harness, one
 * grant per settlement wake turn. Every public admission boundary shares the
 * session's admission-id space (message, queue and signal evidence), so each
 * rejects a caller id in it before any evidence is written.
 */
export function assertCallerAdmissionIdNotReserved(field: string, admissionId: string | undefined): void {
  if (admissionId !== undefined && isHarnessWakeReservedKey(admissionId)) {
    throw new HarnessValidationError(field, 'admissionId uses the Harness-reserved wake namespace');
  }
}

export function normalizeSessionLogicalMessageIdentity(
  value: LogicalMessageIdentity | undefined,
  methodName: string,
): LogicalMessageIdentity | undefined {
  try {
    return normalizeLogicalMessageIdentity(value);
  } catch (error) {
    throw new HarnessValidationError(
      `${methodName}.logicalMessageIdentity`,
      error instanceof Error ? error.message : 'must contain valid input and response ids',
    );
  }
}

export function cloneAttachmentMetadata(metadata: Record<string, JsonValue>): Record<string, JsonValue> {
  return JSON.parse(JSON.stringify(metadata)) as Record<string, JsonValue>;
}

export function clonePersistedRequestContext(input: PersistedRequestContextInput): PersistedRequestContextInput {
  return JSON.parse(JSON.stringify(input)) as PersistedRequestContextInput;
}
