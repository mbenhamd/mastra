import type { HarnessAssistantDraft, JsonValue } from '../../storage/domains/harness';
import type { PlanTaskSummary } from './plan-task-session';
import type {
  ActiveSubagentState,
  ActiveToolState,
  SessionDisplayPending,
  SessionDisplayState,
  SessionRunProjection,
  TokenUsage,
} from './session';

export type HarnessDisplayJsonValue = JsonValue;

export interface HarnessDisplayActiveToolSnapshotV1 {
  toolCallId: string;
  toolName: string;
  args: HarnessDisplayJsonValue;
  startedAt: number;
  subagentSessionId?: string;
}

export interface HarnessDisplayToolInputBufferSnapshotV1 {
  toolName: string;
  text: string;
}

export interface HarnessDisplayActiveSubagentSnapshotV1 {
  subagentSessionId: string;
  agentType: string;
  task: string;
  parentToolCallId: string;
  startedAt: number;
  status?: ActiveSubagentState['status'];
  currentToolName?: string;
  toolCalls?: number;
  usage?: TokenUsage;
  updatedAt?: number;
}

export interface HarnessDisplayPendingSnapshotV1 extends Omit<SessionDisplayPending, 'payload'> {
  payload?: HarnessDisplayJsonValue;
}

export interface HarnessDisplayStateSnapshotV1 {
  version: 1;
  sessionId: string;
  threadId: string;
  resourceId: string;
  parentSessionId?: string;
  lifecycleState: SessionDisplayState['lifecycleState'];
  modeId: string;
  modelId: string;
  createdAt: number;
  lastActivityAt: number;
  isRunning: boolean;
  currentRunId?: string;
  currentMessageId?: string;
  currentTraceId?: string;
  // §5.1b structured in-flight run projection (JSON-safe; primitive fields only).
  currentRun?: SessionRunProjection;
  activeTools: Record<string, HarnessDisplayActiveToolSnapshotV1>;
  toolInputBuffers: Record<string, HarnessDisplayToolInputBufferSnapshotV1>;
  activeSubagents: Record<string, HarnessDisplayActiveSubagentSnapshotV1>;
  assistantDrafts: Record<string, HarnessAssistantDraft>;
  tokenUsage: TokenUsage;
  pending: HarnessDisplayPendingSnapshotV1 | null;
  queueDepth: number;
  currentQueuedItemId?: string;
  goal?: HarnessDisplayJsonValue;
  /**
   * §5.1k / TM-5 bounded plan-task summary — counts + active `in_progress` ids +
   * root count. NOT the full tree: UIs use the `papersflow.plan_task.updated`
   * event deltas + the bounded `plan_task_check` read for detail. Already
   * JSON-safe (numbers + a string-id array), so it is carried verbatim.
   */
  planTasks?: PlanTaskSummary;
}

/**
 * Converts unknown display-state payloads into JSON-safe values. Dates become
 * ISO strings, bigints become strings, non-finite numbers/cycles/errors become
 * null, and undefined/function/symbol object fields are omitted. Own
 * `__proto__` keys are preserved as data properties to avoid prototype writes.
 */
function toHarnessDisplayJsonValue(value: unknown, seen = new WeakSet<object>()): HarnessDisplayJsonValue {
  if (value === null) return null;

  switch (typeof value) {
    case 'string':
    case 'boolean':
      return value;
    case 'number':
      return Number.isFinite(value) ? value : null;
    case 'bigint':
      return value.toString();
    case 'undefined':
    case 'function':
    case 'symbol':
      return null;
    case 'object':
      break;
  }

  if (value instanceof Date) {
    const time = value.getTime();
    return Number.isFinite(time) ? value.toISOString() : null;
  }

  if (seen.has(value)) return null;
  seen.add(value);

  try {
    const toJSON = (value as { toJSON?: unknown }).toJSON;
    if (typeof toJSON === 'function' && !Array.isArray(value)) {
      return toHarnessDisplayJsonValue(toJSON.call(value), seen);
    }

    if (Array.isArray(value)) {
      return value.map(item => toHarnessDisplayJsonValue(item, seen));
    }

    const output: Record<string, HarnessDisplayJsonValue> = {};
    for (const [key, child] of Object.entries(value as Record<string, unknown>)) {
      if (child === undefined) continue;
      Object.defineProperty(output, key, {
        value: toHarnessDisplayJsonValue(child, seen),
        enumerable: true,
        configurable: true,
        writable: true,
      });
    }
    return output;
  } catch {
    return null;
  } finally {
    seen.delete(value);
  }
}

function encodeActiveTool(tool: ActiveToolState): HarnessDisplayActiveToolSnapshotV1 {
  const encoded: HarnessDisplayActiveToolSnapshotV1 = {
    toolCallId: tool.toolCallId,
    toolName: tool.toolName,
    args: toHarnessDisplayJsonValue(tool.args),
    startedAt: tool.startedAt,
  };
  if (tool.subagentSessionId !== undefined) encoded.subagentSessionId = tool.subagentSessionId;
  return encoded;
}

function encodeActiveSubagent(subagent: ActiveSubagentState): HarnessDisplayActiveSubagentSnapshotV1 {
  const encoded: HarnessDisplayActiveSubagentSnapshotV1 = {
    subagentSessionId: subagent.subagentSessionId,
    agentType: subagent.agentType,
    task: subagent.task,
    parentToolCallId: subagent.parentToolCallId,
    startedAt: subagent.startedAt,
  };
  if (subagent.status !== undefined) encoded.status = subagent.status;
  if (subagent.currentToolName !== undefined) encoded.currentToolName = subagent.currentToolName;
  if (subagent.toolCalls !== undefined) encoded.toolCalls = subagent.toolCalls;
  if (subagent.usage !== undefined) encoded.usage = { ...subagent.usage };
  if (subagent.updatedAt !== undefined) encoded.updatedAt = subagent.updatedAt;
  return encoded;
}

function encodePending(pending: SessionDisplayPending | null): HarnessDisplayPendingSnapshotV1 | null {
  if (!pending) return null;
  const { payload: _payload, ...pendingWithoutPayload } = pending;
  const encoded: HarnessDisplayPendingSnapshotV1 = pendingWithoutPayload;
  if (pending.payload !== undefined) encoded.payload = toHarnessDisplayJsonValue(pending.payload);
  return encoded;
}

function encodeAssistantDraft(
  draft: NonNullable<SessionDisplayState['assistantDrafts']>[string],
): NonNullable<SessionDisplayState['assistantDrafts']>[string] {
  return {
    runId: draft.runId,
    sessionId: draft.sessionId,
    resourceId: draft.resourceId,
    threadId: draft.threadId,
    ...(draft.signalId === undefined ? {} : { signalId: draft.signalId }),
    ...(draft.queuedItemId === undefined ? {} : { queuedItemId: draft.queuedItemId }),
    ...(draft.messageId === undefined ? {} : { messageId: draft.messageId }),
    text: draft.text,
    status: draft.status,
    startedAt: draft.startedAt,
    updatedAt: draft.updatedAt,
    ...(draft.terminalAt === undefined ? {} : { terminalAt: draft.terminalAt }),
    ...(draft.finishReason === undefined ? {} : { finishReason: draft.finishReason }),
    ...(draft.truncated === undefined ? {} : { truncated: draft.truncated }),
  };
}

export function toHarnessDisplayStateSnapshotV1(state: SessionDisplayState): HarnessDisplayStateSnapshotV1 {
  const snapshot: HarnessDisplayStateSnapshotV1 = {
    version: 1,
    sessionId: state.sessionId,
    threadId: state.threadId,
    resourceId: state.resourceId,
    lifecycleState: state.lifecycleState,
    modeId: state.modeId,
    modelId: state.modelId,
    createdAt: state.createdAt,
    lastActivityAt: state.lastActivityAt,
    isRunning: state.isRunning,
    activeTools: Object.fromEntries(
      Object.entries(state.activeTools).map(([id, tool]) => [id, encodeActiveTool(tool)]),
    ),
    toolInputBuffers: Object.fromEntries(
      Object.entries(state.toolInputBuffers).map(([id, buffer]) => [
        id,
        { toolName: buffer.toolName, text: buffer.text },
      ]),
    ),
    activeSubagents: Object.fromEntries(
      Object.entries(state.activeSubagents).map(([id, subagent]) => [id, encodeActiveSubagent(subagent)]),
    ),
    assistantDrafts: Object.fromEntries(
      Object.entries(state.assistantDrafts ?? {}).map(([runId, draft]) => [runId, encodeAssistantDraft(draft)]),
    ),
    tokenUsage: { ...state.tokenUsage },
    pending: encodePending(state.pending),
    queueDepth: state.queueDepth,
  };

  if (state.parentSessionId !== undefined) snapshot.parentSessionId = state.parentSessionId;
  if (state.currentRunId !== undefined) snapshot.currentRunId = state.currentRunId;
  if (state.currentMessageId !== undefined) snapshot.currentMessageId = state.currentMessageId;
  if (state.currentTraceId !== undefined) snapshot.currentTraceId = state.currentTraceId;
  if (state.currentQueuedItemId !== undefined) snapshot.currentQueuedItemId = state.currentQueuedItemId;
  // SessionRunProjection is already JSON-safe (primitive fields only).
  if (state.currentRun !== undefined) snapshot.currentRun = state.currentRun;
  if (state.goal !== undefined) snapshot.goal = toHarnessDisplayJsonValue(state.goal);
  // §5.1k / TM-5 — already JSON-safe; copy so callers can't mutate internals.
  if (state.planTasks !== undefined) {
    snapshot.planTasks = {
      total: state.planTasks.total,
      byStatus: { ...state.planTasks.byStatus },
      inProgressTaskIds: [...state.planTasks.inProgressTaskIds],
      rootCount: state.planTasks.rootCount,
    };
  }

  return snapshot;
}
