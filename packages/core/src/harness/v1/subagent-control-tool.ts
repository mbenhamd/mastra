/**
 * Built-in `subagent` control tool (PF-7473).
 *
 * `spawn_subagent` and `task_delegate` let the parent model START children;
 * this tool lets it operate on them afterwards:
 *
 *   - `status` — list this session's children, or inspect one;
 *   - `wait`   — block up to a bounded timeout for one or more children to settle;
 *   - `send`   — interleave a bounded instruction into a running child's run;
 *   - `cancel` — cancel one owned child, durably across processes.
 *
 * Ownership is the durable `parentSessionId` edge: a session acts only on its
 * own children (descendants deeper than that are reached through their own
 * parent, exactly like the cancellation cascade). Every rejection is an
 * `isError` payload, never a throw, so the model can recover inside the turn.
 *
 * Registered by `Session._buildToolSurface()` next to the other Harness
 * built-ins when subagent types are configured and the session may still have
 * children (`subagentDepth < subagents.maxDepth`). Like every other built-in it
 * passes through the per-turn permission gate, so a host can deny `subagent`
 * by tool name or by the category its `toolCategoryResolver` assigns.
 */

import { z } from 'zod';

import type { HarnessPlanTask, HarnessStorage, SessionRecord } from '../../storage/domains/harness';
import { HarnessStorageVersionConflictError } from '../../storage/domains/harness';
import { createTool } from '../../tools/tool';
import { projectHarnessPublicError } from './events';
import { readDelegationAttemptMetadata } from './plan-task-session';
import type { ActiveSubagentState, Session } from './session';
import { harnessSubagentResultSummarySchema } from './terminal-subagent-result';
import type { HarnessSubagentResultSummary } from './terminal-subagent-result';

export const SUBAGENT_CONTROL_TOOL_ID = 'subagent';

/** Durable `cancelRequest.reason` written by the `cancel` action. */
export const SUBAGENT_CONTROL_CANCEL_REASON = 'subagent_cancelled';

export const SUBAGENT_CONTROL_DEFAULT_WAIT_MS = 30_000;
export const SUBAGENT_CONTROL_MAX_WAIT_MS = 300_000;
export const SUBAGENT_CONTROL_MAX_WAIT_TARGETS = 16;
export const SUBAGENT_CONTROL_MAX_MESSAGE_CHARS = 4_000;
export const SUBAGENT_CONTROL_STATUS_LIMIT = 25;
const SUBAGENT_CONTROL_TASK_PREVIEW_CHARS = 500;
const WAIT_POLL_INTERVAL_MS = 100;
const CANCEL_INTENT_CAS_ATTEMPTS = 3;

const subagentStatusEnum = z.enum(['running', 'awaiting_input', 'idle', 'completed', 'blocked', 'failed', 'closed']);
type SubagentStatus = z.infer<typeof subagentStatusEnum>;

/** A child whose status is one of these has nothing left to wait for. */
const SETTLED_STATUSES: ReadonlySet<SubagentStatus> = new Set(['idle', 'completed', 'blocked', 'failed', 'closed']);

const subagentViewSchema = z.object({
  subagentSessionId: z.string(),
  /** How the child was started: an inline `spawn_subagent`, or a durable `task_delegate`. */
  source: z.enum(['inline', 'delegated', 'session']),
  status: subagentStatusEnum,
  settled: z.boolean(),
  /** A durable cancel request is recorded on the child (it may still be unwinding). */
  cancelRequested: z.boolean(),
  /** The child is live in this process (only then can `send` reach it). */
  live: z.boolean(),
  agentType: z.string().optional(),
  task: z.string().optional(),
  taskId: z.string().optional(),
  startedAt: z.number().optional(),
  settledAt: z.number().optional(),
  result: harnessSubagentResultSummarySchema.optional(),
});
type SubagentView = z.infer<typeof subagentViewSchema>;

const inputSchema = z
  .object({
    action: z
      .enum(['status', 'wait', 'send', 'cancel'])
      .describe(
        'status: list your subagents or inspect one. wait: block until subagents settle or the timeout passes. ' +
          'send: add an instruction to a running subagent. cancel: stop one subagent.',
      ),
    subagentSessionId: z
      .string()
      .min(1)
      .optional()
      .describe('Target subagent session id. Required for send and cancel; optional for status and wait.'),
    subagentSessionIds: z
      .array(z.string().min(1))
      .min(1)
      .max(SUBAGENT_CONTROL_MAX_WAIT_TARGETS)
      .optional()
      .describe('wait only: several subagent ids. Omit both ids to wait for every running subagent.'),
    timeoutMs: z
      .number()
      .int()
      .min(0)
      .max(SUBAGENT_CONTROL_MAX_WAIT_MS)
      .optional()
      .describe(
        `wait only: maximum wait in ms (default ${SUBAGENT_CONTROL_DEFAULT_WAIT_MS}, max ${SUBAGENT_CONTROL_MAX_WAIT_MS}).`,
      ),
    message: z
      .string()
      .min(1)
      .max(SUBAGENT_CONTROL_MAX_MESSAGE_CHARS)
      .optional()
      .describe('send only: the instruction to deliver to the running subagent.'),
  })
  .superRefine((input, ctx) => {
    if ((input.action === 'send' || input.action === 'cancel') && input.subagentSessionId === undefined) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        path: ['subagentSessionId'],
        message: `${input.action} requires subagentSessionId`,
      });
    }
    if (input.action === 'send' && input.message === undefined) {
      ctx.addIssue({ code: z.ZodIssueCode.custom, path: ['message'], message: 'send requires message' });
    }
    if (input.action !== 'send' && input.message !== undefined) {
      ctx.addIssue({ code: z.ZodIssueCode.custom, path: ['message'], message: 'message is only valid for send' });
    }
    if (input.action !== 'wait' && (input.subagentSessionIds !== undefined || input.timeoutMs !== undefined)) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        path: ['action'],
        message: 'subagentSessionIds and timeoutMs are only valid for wait',
      });
    }
  });

const outputSchema = z.object({
  action: z.enum(['status', 'wait', 'send', 'cancel']).optional(),
  subagents: z.array(subagentViewSchema).optional(),
  truncated: z.boolean().optional(),
  /** wait: children that settled before the timeout. */
  settledSubagents: z.array(subagentViewSchema).optional(),
  /** wait: children still running when the wait ended. */
  runningSubagents: z.array(subagentViewSchema).optional(),
  timedOut: z.boolean().optional(),
  subagentSessionId: z.string().optional(),
  /** send: the instruction reached the child's active run. */
  delivered: z.boolean().optional(),
  /**
   * cancel: how the request landed.
   * - `acknowledged`: the cancel committed on the child record and its abort
   *   ran in this process.
   * - `intent_recorded`: another process holds the child; the durable cancel
   *   intent is on the child record and is honoured by that owner on its next
   *   record write, or by restart reconciliation.
   * - `already_requested`: a cancel request was already recorded.
   * - `already_settled`: the child had already finished; nothing changed.
   */
  cancel: z.enum(['acknowledged', 'intent_recorded', 'already_requested', 'already_settled']).optional(),
  /** cancel: the child has settled (true only when nothing is left to unwind). */
  settled: z.boolean().optional(),
  status: subagentStatusEnum.optional(),
  isError: z.boolean().optional(),
  errorName: z.string().optional(),
  code: z.string().optional(),
  field: z.string().optional(),
  reason: z.string().optional(),
  message: z.string().optional(),
});
type SubagentControlOutput = z.infer<typeof outputSchema>;

type ControlHarness = {
  _listSubagentTypeIds(options: { invocation: 'inline' | 'delegated' }): string[];
  _getSubagentMaxDepth(): number;
  _internalGetLiveSession(sessionId: string): Session | undefined;
};

function validationError(field: string, reason: string): SubagentControlOutput {
  return { isError: true, errorName: 'HarnessValidationError', field, reason, message: `${field}: ${reason}` };
}

function preview(text: string): string {
  return text.length <= SUBAGENT_CONTROL_TASK_PREVIEW_CHARS
    ? text
    : `${text.slice(0, SUBAGENT_CONTROL_TASK_PREVIEW_CHARS - 1)}…`;
}

function abortable(ms: number, signal: AbortSignal | undefined): Promise<void> {
  return new Promise(resolve => {
    if (signal?.aborted) return resolve();
    const onAbort = () => {
      clearTimeout(timer);
      resolve();
    };
    const timer = setTimeout(() => {
      signal?.removeEventListener('abort', onAbort);
      resolve();
    }, ms);
    signal?.addEventListener('abort', onAbort, { once: true });
  });
}

/**
 * Durable cancel intent for a child whose lease another process holds. The
 * write is a version-CAS on the freshly loaded row that adds ONLY
 * `cancelRequest` (first writer wins, never cleared) and preserves the lease
 * metadata, so the holder's next `saveSession` hits a version conflict, reloads
 * the row, and from then on fails closed on new turns/resumes; a delegation
 * driver or restart reconcile maps the recorded cancel to a failed delegation.
 */
async function recordDurableCancelIntent(
  storage: HarnessStorage,
  harnessName: string,
  childId: string,
  requestedBy: string,
  fallbackOwnerId: string,
): Promise<'recorded' | 'already_requested' | 'settled' | 'unavailable'> {
  for (let attempt = 0; attempt < CANCEL_INTENT_CAS_ATTEMPTS; attempt++) {
    const current = await storage.loadSession({ harnessName, sessionId: childId });
    if (current === null || current.closedAt !== undefined || current.closingAt !== undefined) return 'settled';
    if (current.cancelRequest !== undefined) return 'already_requested';
    try {
      await storage.saveSession(
        {
          ...current,
          cancelRequest: { requestedAt: Date.now(), reason: SUBAGENT_CONTROL_CANCEL_REASON, requestedBy },
        },
        { harnessName, ownerId: current.ownerId ?? fallbackOwnerId, ifVersion: current.version },
      );
      return 'recorded';
    } catch (err) {
      if (err instanceof HarnessStorageVersionConflictError) continue;
      return 'unavailable';
    }
  }
  return 'unavailable';
}

/**
 * Build the `subagent` control tool for one session. Returns `undefined` when
 * the harness has no subagent types or this session is already at
 * `subagents.maxDepth` (it can have no children to control).
 */
export function createSubagentControlTool(parent: Session) {
  const harness = (parent as unknown as { _harness: ControlHarness })._harness;
  // The delegated listing is every registered type (inline is a subset).
  if (harness._listSubagentTypeIds({ invocation: 'delegated' }).length === 0) return undefined;
  if (parent.subagentDepth >= harness._getSubagentMaxDepth()) return undefined;

  const storage = parent._internalStorage;
  const harnessName = parent.getRecord().harnessName;
  const activeSubagents = () =>
    Array.from((parent as unknown as { _activeSubagents: Map<string, ActiveSubagentState> })._activeSubagents.values());

  async function loadDelegatedTasks(): Promise<Map<string, HarnessPlanTask>> {
    const byChild = new Map<string, HarnessPlanTask>();
    let cursor: string | undefined;
    for (;;) {
      const page = await storage.listPlanTasks({ harnessName, sessionId: parent.id, limit: 500, cursor });
      for (const task of page.tasks) {
        // Settlement clears the live link but keeps the attempt (and its result)
        // in the task metadata, so index both.
        const childId = task.delegatedSubagentSessionId ?? readDelegationAttemptMetadata(task)?.subagentSessionId;
        if (childId !== undefined) byChild.set(childId, task);
      }
      if (!page.cursor || page.tasks.length === 0) break;
      cursor = page.cursor;
    }
    return byChild;
  }

  /** Live record when the child is in this process, else the durable row. */
  async function loadChildRecord(childId: string): Promise<{ record: SessionRecord | null; live: boolean }> {
    const live = harness._internalGetLiveSession(childId);
    if (live !== undefined) return { record: live.getRecord() as SessionRecord, live: true };
    return { record: await storage.loadSession({ harnessName, sessionId: childId }), live: false };
  }

  function isOwnChild(record: SessionRecord | null): record is SessionRecord {
    return record !== null && record.parentSessionId === parent.id && record.resourceId === parent.resourceId;
  }

  function project(
    childId: string,
    record: SessionRecord,
    live: boolean,
    active: ActiveSubagentState | undefined,
    delegated: HarnessPlanTask | undefined,
  ): SubagentView {
    const attempt = delegated === undefined ? undefined : readDelegationAttemptMetadata(delegated, childId);
    let status: SubagentStatus;
    let result: HarnessSubagentResultSummary | undefined;
    let settledAt: number | undefined;
    if (attempt?.settlement !== undefined) {
      status = attempt.settlement.outcome;
      result = attempt.settlement.result;
      settledAt = attempt.settlement.settledAt;
    } else if (delegated !== undefined && delegated.status === 'completed') {
      status = 'completed';
    } else if (delegated !== undefined && (delegated.status === 'failed' || delegated.status === 'cancelled')) {
      status = 'failed';
    } else if (record.pendingResume !== undefined || active?.status === 'awaiting_input') {
      status = 'awaiting_input';
    } else if (delegated?.delegatedSubagentSessionId === childId) {
      // The durable link is live: the delegation driver (or reconcile) still
      // owes the settlement, even if the child row already closed.
      status = 'running';
    } else if (record.closedAt !== undefined || record.closingAt !== undefined) {
      status = 'closed';
    } else if (active !== undefined) {
      status = 'running';
    } else {
      status = 'idle';
    }
    const task = attempt?.taskBody ?? active?.task;
    const startedAt = attempt?.startedAt ?? active?.startedAt;
    return {
      subagentSessionId: childId,
      source: delegated !== undefined ? 'delegated' : active !== undefined ? 'inline' : 'session',
      status,
      settled: SETTLED_STATUSES.has(status),
      cancelRequested: record.cancelRequest !== undefined,
      live,
      ...(record.subagentTypeId !== undefined ? { agentType: record.subagentTypeId } : {}),
      ...(active?.agentType !== undefined && record.subagentTypeId === undefined
        ? { agentType: active.agentType }
        : {}),
      ...(task !== undefined ? { task: preview(task) } : {}),
      ...(delegated !== undefined ? { taskId: delegated.taskId } : {}),
      ...(startedAt !== undefined ? { startedAt } : {}),
      ...(settledAt !== undefined ? { settledAt } : {}),
      ...(result !== undefined ? { result } : {}),
    };
  }

  /**
   * Resolve the views for `childIds`, or for every known child when omitted.
   * A named id that is not this session's own child is returned in `foreign`.
   */
  async function resolveChildren(
    childIds: readonly string[] | undefined,
  ): Promise<{ views: SubagentView[]; foreign: string[]; truncated: boolean }> {
    const delegated = await loadDelegatedTasks();
    const activeById = new Map(activeSubagents().map(state => [state.subagentSessionId, state]));
    let ids: string[];
    let truncated = false;
    if (childIds !== undefined) {
      ids = Array.from(new Set(childIds));
    } else {
      const known = new Map<string, number>();
      for (const [id, state] of activeById) known.set(id, state.startedAt);
      for (const [id, task] of delegated) {
        if (!known.has(id)) known.set(id, readDelegationAttemptMetadata(task, id)?.startedAt ?? task.createdAt);
      }
      ids = Array.from(known.entries())
        .sort((left, right) => right[1] - left[1])
        .map(([id]) => id);
      truncated = ids.length > SUBAGENT_CONTROL_STATUS_LIMIT;
      ids = ids.slice(0, SUBAGENT_CONTROL_STATUS_LIMIT);
    }
    const views: SubagentView[] = [];
    const foreign: string[] = [];
    for (const id of ids) {
      const { record, live } = await loadChildRecord(id);
      if (!isOwnChild(record)) {
        foreign.push(id);
        continue;
      }
      views.push(project(id, record, live, activeById.get(id), delegated.get(id)));
    }
    return { views, foreign, truncated };
  }

  function foreignError(ids: readonly string[]): SubagentControlOutput {
    return validationError('subagentSessionId', `not a subagent of this session: ${ids.join(', ')}`);
  }

  async function status(childId: string | undefined): Promise<SubagentControlOutput> {
    const { views, foreign, truncated } = await resolveChildren(childId === undefined ? undefined : [childId]);
    if (foreign.length > 0) return foreignError(foreign);
    return { action: 'status', subagents: views, ...(truncated ? { truncated: true } : {}) };
  }

  async function wait(
    childIds: readonly string[] | undefined,
    timeoutMs: number,
    signal: AbortSignal | undefined,
  ): Promise<SubagentControlOutput> {
    const deadline = Date.now() + timeoutMs;
    let targets = childIds;
    for (;;) {
      const { views, foreign } = await resolveChildren(targets);
      if (foreign.length > 0) return foreignError(foreign);
      // An implicit wait pins the children that were running when it started.
      if (targets === undefined) {
        targets = views.filter(view => !view.settled).map(view => view.subagentSessionId);
        if (targets.length === 0) {
          return { action: 'wait', settledSubagents: [], runningSubagents: [], timedOut: false };
        }
        continue;
      }
      const running = views.filter(view => !view.settled);
      const timedOut = running.length > 0 && Date.now() >= deadline;
      if (running.length === 0 || timedOut || signal?.aborted === true) {
        return {
          action: 'wait',
          settledSubagents: views.filter(view => view.settled),
          runningSubagents: running,
          timedOut,
        };
      }
      await abortable(Math.min(WAIT_POLL_INTERVAL_MS, Math.max(0, deadline - Date.now())), signal);
    }
  }

  async function send(childId: string, message: string): Promise<SubagentControlOutput> {
    const { views, foreign } = await resolveChildren([childId]);
    if (foreign.length > 0) return foreignError(foreign);
    const view = views[0]!;
    const child = harness._internalGetLiveSession(childId);
    if (child === undefined || view.settled || view.cancelRequested) {
      return validationError(
        'subagentSessionId',
        view.settled || view.cancelRequested
          ? `subagent is ${view.cancelRequested ? 'cancelled' : view.status}; it can no longer receive messages`
          : 'subagent is not live in this process; inspect it with status',
      );
    }
    if (!child.isRunning() || view.status === 'awaiting_input') {
      return validationError(
        'subagentSessionId',
        view.status === 'awaiting_input'
          ? 'subagent is awaiting human input; it cannot take a message until that interaction is resolved'
          : 'subagent has no active run to deliver the message to',
      );
    }
    const delivered = await child.injectSystemReminder(message, {
      attributes: { source: 'parent_session', parentSessionId: parent.id },
    });
    return {
      action: 'send',
      subagentSessionId: childId,
      delivered: delivered.willInterleave,
      status: view.status,
    };
  }

  async function cancel(childId: string): Promise<SubagentControlOutput> {
    const before = await resolveChildren([childId]);
    if (before.foreign.length > 0) return foreignError(before.foreign);
    const view = before.views[0]!;
    if (view.settled && view.status !== 'idle') {
      return {
        action: 'cancel',
        subagentSessionId: childId,
        cancel: 'already_settled',
        settled: true,
        status: view.status,
      };
    }
    if (view.cancelRequested) {
      return {
        action: 'cancel',
        subagentSessionId: childId,
        cancel: 'already_requested',
        settled: view.settled,
        status: view.status,
      };
    }
    const delivery = await parent._internalCancelOwnedSubagent(childId, SUBAGENT_CONTROL_CANCEL_REASON);
    let landed: 'acknowledged' | 'intent_recorded' | 'already_requested' | 'already_settled' = 'acknowledged';
    if (delivery === 'unavailable') {
      const intent = await recordDurableCancelIntent(storage, harnessName, childId, parent.id, parent._internalOwnerId);
      if (intent === 'unavailable') {
        return {
          isError: true,
          errorName: 'HarnessSubagentCancelUnavailableError',
          code: 'harness.subagent_cancel_unavailable',
          message: 'The subagent could not be reached and its cancel request could not be recorded; retry later.',
          subagentSessionId: childId,
        };
      }
      landed =
        intent === 'recorded' ? 'intent_recorded' : intent === 'settled' ? 'already_settled' : 'already_requested';
    }
    const after = (await resolveChildren([childId])).views[0];
    return {
      action: 'cancel',
      subagentSessionId: childId,
      cancel: landed,
      // A recorded intent has not reached the holder yet: an idle child there is
      // not settled until it observes the intent or closes.
      settled:
        after === undefined
          ? landed === 'already_settled'
          : after.settled && !(landed === 'intent_recorded' && after.status === 'idle'),
      ...(after !== undefined ? { status: after.status } : {}),
    };
  }

  return createTool({
    id: SUBAGENT_CONTROL_TOOL_ID,
    description:
      'Operate on subagents this session started with spawn_subagent or task_delegate. ' +
      '`status` lists them (or inspects one) with their status and, once settled, their result. ' +
      '`wait` blocks until the named subagents (or every running one) settle, or the timeout passes; it ' +
      'returns settled results and those still running. `send` adds an instruction to a running ' +
      'subagent. `cancel` stops one subagent; `cancel: "acknowledged"` or `"intent_recorded"` means the ' +
      'request is accepted, and `settled` says whether the subagent has finished unwinding. You can only ' +
      'act on your own subagents.',
    inputSchema,
    outputSchema,
    execute: async (input, ctx): Promise<SubagentControlOutput> => {
      const ids =
        input.subagentSessionIds !== undefined
          ? input.subagentSessionIds
          : input.subagentSessionId !== undefined
            ? [input.subagentSessionId]
            : undefined;
      try {
        switch (input.action) {
          case 'status':
            return await status(input.subagentSessionId);
          case 'wait':
            return await wait(ids, input.timeoutMs ?? SUBAGENT_CONTROL_DEFAULT_WAIT_MS, ctx?.abortSignal);
          case 'send':
            return await send(input.subagentSessionId!, input.message!);
          case 'cancel':
            return await cancel(input.subagentSessionId!);
        }
      } catch (err) {
        const publicError = projectHarnessPublicError(err);
        return {
          isError: true,
          errorName: err instanceof Error ? err.name : 'Error',
          code: publicError.code,
          message: publicError.message,
        };
      }
    },
  });
}
