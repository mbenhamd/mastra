import type { HarnessAssistantDraft, SessionRecord } from '../../storage/domains/harness';
import type { PlanTaskSummary } from './plan-task-session';
import type {
  ActiveSubagentState,
  ActiveToolState,
  HarnessRunOperationRefKind,
  SessionLifecycleState,
  TokenUsage,
} from './session';

/**
 * Point-in-time snapshot returned by `getDisplayState()` (§4.2). Reads off
 * the in-memory `SessionRecord` plus a few transient run-only fields.
 *
 * Persistent thread-level aggregates (task lists, modified-file ledgers, OM
 * progress) deliberately live in `session.state`, not here — see the
 * `getDisplayState()` doc-comment for the split rationale. All `Record<>`
 * collections returned here are fresh on every call; do not mutate them.
 */
// ---------------------------------------------------------------------------
// Run projection (§5.1b SessionRunProjection). A bounded read-model view of the
// active run for the session list/detail UI — NOT the durable
// HarnessRunOperationalState recovery record. Populated from live session state;
// `getDisplayState().currentRun` is present only while a run is in flight.
// ---------------------------------------------------------------------------
export type HarnessRunStatus = 'starting' | 'running' | 'waiting' | 'resuming' | 'completed' | 'failed' | 'interrupted';

export interface SessionRunProjection {
  runId: string;
  traceId?: string;
  status: HarnessRunStatus;
  operation: {
    kind: HarnessRunOperationRefKind;
    signalId?: string;
    queuedItemId?: string;
    itemId?: string;
    responseId?: string;
    skillName?: string;
  };
  modeId: string;
  modelId: string;
  agentId: string;
  startedAt: number;
  updatedAt: number;
}

export const MAX_DELIVERED_TERMINAL_SUBAGENT_ANSWERS = 4096;

export interface SessionDisplayState {
  // Identity
  sessionId: string;
  threadId: string;
  resourceId: string;
  parentSessionId?: string;
  lifecycleState: SessionLifecycleState;
  modeId: string;
  modelId: string;
  createdAt: number;
  lastActivityAt: number;

  // Run
  isRunning: boolean;
  currentRunId?: string;
  currentMessageId?: string;
  currentTraceId?: string;
  // §5.1b structured projection of the in-flight run (present only while a run
  // is active). Consolidates the loose currentRun* fields above into the spec
  // SessionRunProjection shape for the session list/detail UI.
  currentRun?: SessionRunProjection;

  // Activity
  activeTools: Record<string, ActiveToolState>;
  toolInputBuffers: Record<string, { toolName: string; text: string }>;
  activeSubagents: Record<string, ActiveSubagentState>;
  assistantDrafts?: Record<string, HarnessAssistantDraft>;

  // Tokens
  tokenUsage: TokenUsage;

  // Pending interrupt (full UX payload, not recovery-only metadata)
  pending: SessionDisplayPending | null;

  // Queue
  queueDepth: number;
  currentQueuedItemId?: string;

  // Goal
  goal?: SessionRecord['goal'];

  // Plan tasks (§5.1k / TM-5): a BOUNDED summary — counts + active in_progress
  // ids + root count — NOT the full tree. UIs drive detail off the
  // `papersflow.plan_task.updated` event deltas + the bounded `plan_task_check`
  // read. Absent until the session has observed its plan tree.
  planTasks?: PlanTaskSummary;
}

export type SessionDisplayPending = Omit<
  NonNullable<SessionRecord['pendingResume']>,
  | 'runtimeDependencies'
  | 'requestContext'
  | 'toolSurfaceFence'
  | 'toolReceipts'
  | 'toolReceiptsOverflow'
  | 'subagentBudgetSpent'
  | 'expiresAt'
> & { expiresAt: number };

export function pendingResumeForDisplay(pending: SessionRecord['pendingResume']): SessionDisplayPending | null {
  if (!pending) return null;
  // Strip storage-internal recovery fields and the caller app bag. Receipt
  // identities authenticate a terminal report after restart; they are not the
  // UI's activity feed (tool events already provide that public projection).
  const {
    runtimeDependencies: _runtimeDependencies,
    requestContext: _requestContext,
    toolSurfaceFence: _toolSurfaceFence,
    toolReceipts: _toolReceipts,
    toolReceiptsOverflow: _toolReceiptsOverflow,
    subagentBudgetSpent: _subagentBudgetSpent,
    ...displayPending
  } = pending;
  return {
    ...displayPending,
    expiresAt: pending.expiresAt,
  };
}
