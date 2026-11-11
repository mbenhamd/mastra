import { HARNESS_RUN_INTERRUPTED_ERROR_CODE } from '../../storage/domains/harness';
import type { AgentSignalResultEvidence, SessionRecord } from '../../storage/domains/harness';
import { sha256CanonicalJsonChecked as sha256CanonicalJson } from './canonical-json';
import { PLAN_TASK_MAX_NODES } from './plan-task-session';
import { summarizeHarnessSubagentEventResult, summarizeHarnessSubagentResult } from './terminal-subagent-result';
import type { HarnessSubagentResultSummary } from './terminal-subagent-result';

/**
 * TM-6 `includeSubtree` transfers the complete valid plan. The plan writer
 * already enforces this bound, so a normal delegated child never receives a
 * partial execution unit that the parent would later settle as complete.
 */
export const DELEGATED_SUBTREE_MAX_NODES = PLAN_TASK_MAX_NODES;

export const DELEGATION_DEADLINE_REACHED = Symbol('delegation-deadline-reached');

/** Race one durable delegation result against its persisted absolute deadline. */
export async function waitForDelegationResult<T>(
  promise: Promise<T>,
  deadlineAt: number,
): Promise<T | typeof DELEGATION_DEADLINE_REACHED> {
  const remainingMs = Math.max(0, deadlineAt - Date.now());
  if (remainingMs === 0) return DELEGATION_DEADLINE_REACHED;
  let timer: ReturnType<typeof setTimeout> | undefined;
  const deadline = new Promise<typeof DELEGATION_DEADLINE_REACHED>(resolve => {
    timer = setTimeout(() => resolve(DELEGATION_DEADLINE_REACHED), remainingMs);
    if (typeof timer.unref === 'function') timer.unref();
  });
  try {
    return await Promise.race([promise, deadline]);
  } finally {
    if (timer !== undefined) clearTimeout(timer);
  }
}

/** The public error of a delegated task its parent cancelled (`cancelled` outcome). */
export const DELEGATED_SUBAGENT_CANCELLED_ERROR = {
  code: 'harness.subagent_cancelled',
  message: 'The delegated subagent was cancelled before it completed',
} as const;

export function summarizeDelegationDeadline(deadlineAt: number): HarnessSubagentResultSummary {
  const summary = summarizeHarnessSubagentEventResult(
    summarizeHarnessSubagentResult(undefined, {
      isError: true,
      error: {
        code: 'harness.delegation_timeout',
        message: `The delegated subagent did not complete before its durable deadline (${deadlineAt})`,
      },
    }),
  );
  return { ...summary, outcome: 'failed' };
}

/** The deterministic admission id of a parent's `task_delegate` signal to its child. */
export function delegationAdmissionId(parentSessionId: string, taskId: string): string {
  return `harness-delegate:${parentSessionId}:${taskId}`;
}

/** The child's signal evidence id for one admitted delegation signal. */
export function delegationSignalId(child: SessionRecord, admissionId: string): string {
  const digest = sha256CanonicalJson({
    kind: 'channel-signal-admission',
    harnessName: child.harnessName,
    sessionId: child.id,
    resourceId: child.resourceId,
    threadId: child.threadId,
    admissionId,
  });
  return `harness-channel-signal-${digest.slice(0, 32)}`;
}

/**
 * Settled evidence recovery publishes (as message recovery does): a completed
 * result that was not erased, or an interruption. Other failures were
 * reported by the run that failed.
 */
export function owesRecoveredCompletion(
  evidence: AgentSignalResultEvidence,
): evidence is AgentSignalResultEvidence & { runId: string } {
  return (
    evidence.runId !== undefined &&
    ((evidence.status === 'completed' && evidence.erased !== true) ||
      (evidence.status === 'failed' && evidence.error.code === HARNESS_RUN_INTERRUPTED_ERROR_CODE))
  );
}

/** PF-7473: true when the caller's admission check now fails. */
export function admissionLost(assertAdmitted: () => void): boolean {
  try {
    assertAdmitted();
    return false;
  } catch {
    return true;
  }
}

/**
 * True when `parentSessionId` itself asked to stop exactly this child: the
 * `subagent` tool's cancel or `cancelSubagent`, which alone record the
 * harness-owned `targeted` marker. The reason is not consulted. A delegation
 * timeout, a cancel cascading down from the parent's own cancellation, and a
 * cancel from anyone else carry no marker, so those settle the delegated task
 * `failed`.
 */
export function isParentRequestedSubagentCancel(
  cancelRequest: SessionRecord['cancelRequest'],
  parentSessionId: string,
): boolean {
  return cancelRequest?.targeted === true && cancelRequest.requestedBy === parentSessionId;
}

/** TM-6 reconcile-on-rehydrate retry (transient/CAS/locked failures). */
export const DELEGATION_RECONCILE_RETRY_BASE_MS = 500;
export const DELEGATION_RECONCILE_RETRY_MAX_MS = 30_000;
