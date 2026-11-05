/**
 * Built-in `subagent` control tool (PF-7473).
 *
 * `spawn_subagent` and `task_delegate` let the parent model START children;
 * this tool lets it operate on them afterwards:
 *
 *   - `status` — list this session's children, or inspect one;
 *   - `wait`   — block up to a bounded timeout for one or more children to settle;
 *   - `send`   — interleave a bounded instruction into a running child's run;
 *   - `cancel` — cancel one owned child. The cancel commits on the child's own
 *     record, so it survives restarts. A child whose lease another process
 *     still holds is reported unavailable; nothing is written under the other
 *     owner's lease.
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

import type { HarnessPlanTask, SessionRecord } from '../../storage/domains/harness';
import { createTool } from '../../tools/tool';
import { HarnessValidationError } from './errors';
import { projectHarnessPublicError } from './events';
import { readDelegationAttemptMetadata } from './plan-task-session';
import type { ActiveSubagentState, ReturnedInlineSubagentState, Session } from './session';
import {
  harnessSubagentResultSummarySchema,
  MAX_HARNESS_SUBAGENT_EVENT_TEXT_BYTES,
  summarizeHarnessSubagentEventResult,
} from './terminal-subagent-result';
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
/**
 * UTF-8 byte budget for a whole `status` listing or multi-child `wait` output
 * (the same budget as `plan_task_check`). A child named in the call, or one
 * that settled during it, gets its full result while the budget allows; every
 * other child gets a bounded preview. When the children's own fields still do
 * not fit, task texts are shortened, and then the last children are left out;
 * `resultsHint` says how to read them. A call that names exactly one child
 * returns that child's full result whatever its size: the terminal result caps
 * in `terminal-subagent-result.ts` already bound it.
 */
export const SUBAGENT_CONTROL_MAX_OUTPUT_BYTES = 64 * 1024;
/** A full result no larger than this is never replaced by a preview. */
const SUBAGENT_CONTROL_SMALL_RESULT_BYTES = MAX_HARNESS_SUBAGENT_EVENT_TEXT_BYTES;
/** UTF-8 bytes of task text a child keeps once task texts are shortened. */
const SUBAGENT_CONTROL_SHORT_TASK_BYTES = 160;
export const SUBAGENT_CONTROL_RESULTS_SHORTENED_HINT =
  'Some results or task texts are previews or were left out to keep this output small. To read one in full, ' +
  'call subagent with action "status" and only that subagentSessionId.';
const WAIT_POLL_INTERVAL_MS = 100;
const WAIT_POLL_MAX_INTERVAL_MS = 1_000;
/**
 * How long a `wait` poll's storage read may run past the wait deadline. The
 * deadline decides when to stop polling, never how long a started read may
 * take; only a read still pending this long after it started (and after the
 * deadline) counts as stalled.
 */
export const SUBAGENT_CONTROL_WAIT_READ_STALL_MS = 1_000;

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
  /** `result` is a bounded preview of the full result. */
  resultPreview: z.boolean().optional(),
  /** The child has a result, but it did not fit in this output. */
  resultOmitted: z.boolean().optional(),
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
        `wait only: maximum wait in ms (default ${SUBAGENT_CONTROL_DEFAULT_WAIT_MS}, max ${SUBAGENT_CONTROL_MAX_WAIT_MS}). ` +
          'If the first status read stalls, wait fails with harness.storage and can be retried; if a later ' +
          'read stalls past the timeout, wait returns timedOut with the statuses from its last completed read.',
      ),
    offset: z
      .number()
      .int()
      .min(0)
      .optional()
      .describe(
        'status and wait without ids only: the position to start from in the list of all your subagents, ' +
          'running and settled, ordered oldest first (by start time, then id). A subagent keeps its position ' +
          'when it settles. A wait with an offset returns the settled subagents from that position on and ' +
          'waits for the running ones. When an output leaves subagents out, resultsHint gives the next ' +
          'offset. Positions can shift only when a subagent starts between calls with the same or an earlier ' +
          'start time, or when a new run begins: a spawn_subagent subagent whose call returned in an earlier ' +
          'run is no longer listed. A message that joins the current run does not start a new one.',
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
    if (input.subagentSessionIds !== undefined && input.subagentSessionId !== undefined) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        path: ['subagentSessionIds'],
        message: 'pass subagentSessionId or subagentSessionIds, not both',
      });
    }
    if (input.action !== 'wait' && (input.subagentSessionIds !== undefined || input.timeoutMs !== undefined)) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        path: ['action'],
        message: 'subagentSessionIds and timeoutMs are only valid for wait',
      });
    }
    if (
      input.offset !== undefined &&
      ((input.action !== 'status' && input.action !== 'wait') ||
        input.subagentSessionId !== undefined ||
        input.subagentSessionIds !== undefined)
    ) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        path: ['offset'],
        message: 'offset is only valid for status or wait without subagent ids',
      });
    }
  });

const outputSchema = z.object({
  action: z.enum(['status', 'wait', 'send', 'cancel']).optional(),
  subagents: z.array(subagentViewSchema).optional(),
  truncated: z.boolean().optional(),
  /**
   * status/wait: at least one result or task text is a preview or was left
   * out, or some children were left out; `resultsHint` says how to read them.
   */
  resultsShortened: z.boolean().optional(),
  /** status/wait: how many children were left out of this output; `resultsHint` names them. */
  omittedSubagentCount: z.number().int().optional(),
  resultsHint: z.string().optional(),
  /** wait: children that settled before the timeout. */
  settledSubagents: z.array(subagentViewSchema).optional(),
  /** wait: children still running when the wait ended. */
  runningSubagents: z.array(subagentViewSchema).optional(),
  timedOut: z.boolean().optional(),
  /** wait: the tool call was aborted before the children settled. */
  aborted: z.boolean().optional(),
  subagentSessionId: z.string().optional(),
  /** send: the instruction reached the child's active run. */
  delivered: z.boolean().optional(),
  /**
   * cancel: how the request landed.
   * - `acknowledged`: the cancel committed durably on the child record (it
   *   survives restarts) and the child's work was aborted in this process.
   * - `already_requested`: a cancel was already recorded; the cascade to the
   *   child's own descendants was resumed.
   * - `already_settled`: the child had already finished; nothing changed.
   */
  cancel: z.enum(['acknowledged', 'already_requested', 'already_settled']).optional(),
  /** cancel: the child has settled (true only when nothing is left to unwind). */
  settled: z.boolean().optional(),
  /** cancel: a descendant below the child could not be reached yet; cancel again to resume. */
  descendantCancelIncomplete: z.boolean().optional(),
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

const utf8 = new TextEncoder();

function jsonBytes(value: unknown): number {
  return utf8.encode(JSON.stringify(value)).byteLength;
}

function withoutResult(view: SubagentView): SubagentView {
  const { result: _result, resultPreview: _preview, resultOmitted: _omitted, ...rest } = view;
  return rest;
}

/** `text` cut to at most `maxBytes` UTF-8 bytes on a character boundary, with an ellipsis when cut. */
function trimUtf8(text: string, maxBytes: number): string {
  if (utf8.encode(text).byteLength <= maxBytes) return text;
  const ellipsis = '…';
  let budget = maxBytes - utf8.encode(ellipsis).byteLength;
  let kept = '';
  for (const char of text) {
    const bytes = utf8.encode(char).byteLength;
    if (bytes > budget) break;
    budget -= bytes;
    kept += char;
  }
  return `${kept}${ellipsis}`;
}

function shortTask(view: SubagentView): SubagentView {
  if (view.task === undefined) return view;
  const task = trimUtf8(view.task, SUBAGENT_CONTROL_SHORT_TASK_BYTES);
  return task === view.task ? view : { ...view, task };
}

/**
 * How an implicit-list call reads the children its output left out. `position`
 * maps each child to its place in the call's stable list (every child in
 * scope, ordered by start time and then id), so the hint's offset is the
 * place of the first child left out, whatever settled in between. `tail`
 * counts the children after the call's page that it never read (a `status`
 * listing past SUBAGENT_CONTROL_STATUS_LIMIT). Undefined for a call that names
 * its children; that hint lists the left-out ids instead.
 */
type Continuation =
  | {
      action: 'status' | 'wait';
      position: ReadonlyMap<string, number>;
      tail?: { count: number; nextOffset: number };
    }
  | undefined;

/** The hint for the children left out of an output, so a narrower call can read them. */
function omittedHint(ids: readonly string[], continuation: Continuation): string {
  let how: string;
  let count = ids.length;
  if (continuation === undefined) {
    how =
      `call subagent with action "wait", timeoutMs 0 and subagentSessionIds set to at most ` +
      `${SUBAGENT_CONTROL_MAX_WAIT_TARGETS} of these ids per call: ${ids.join(', ')}`;
  } else {
    count += continuation.tail?.count ?? 0;
    const offset = ids.length > 0 ? continuation.position.get(ids[0]!)! : continuation.tail!.nextOffset;
    how =
      continuation.action === 'wait'
        ? `call subagent with action "wait", timeoutMs 0 and offset ${offset}`
        : `call subagent with action "status" and offset ${offset}`;
  }
  return `${count} subagents were left out of this output. To read them, ${how}. ${SUBAGENT_CONTROL_RESULTS_SHORTENED_HINT}`;
}

/** Children ordered by start time, then id, so every process pages them alike. */
function byStart(left: [string, number], right: [string, number]): number {
  return left[1] - right[1] || (left[0] < right[0] ? -1 : left[0] > right[0] ? 1 : 0);
}

type PlaceViews = (list: readonly SubagentView[]) => SubagentView[];

/**
 * Keep a `status`/`wait` output under SUBAGENT_CONTROL_MAX_OUTPUT_BYTES.
 * `build` places views into the output envelope; `views` lists them in output
 * order.
 *
 * An output that holds only the one child the call names is returned whole.
 * Otherwise, in order, until the children fit:
 *
 *   1. their results are fitted (see `fitResults`);
 *   2. their task texts are cut to SUBAGENT_CONTROL_SHORT_TASK_BYTES and the
 *      results fitted again;
 *   3. only the longest prefix of the children that fits is kept; the rest
 *      are counted in `omittedSubagentCount`. For an implicit list,
 *      `resultsHint` gives the stable-list place (`offset`) of the first one
 *      left out, plus any `status` children past its limit; each page
 *      keeps at least one child, so paging always moves on. A call that names
 *      its children (at most SUBAGENT_CONTROL_MAX_WAIT_TARGETS, which always
 *      fit) never leaves one out; if it did, the hint would list their ids.
 */
function boundResults(
  build: (place: PlaceViews) => SubagentControlOutput,
  views: readonly SubagentView[],
  fullResultIds: ReadonlySet<string>,
  namedOne: string | undefined,
  continuation?: Continuation,
): SubagentControlOutput {
  if (namedOne !== undefined && views.every(view => view.subagentSessionId === namedOne)) {
    return build(list => [...list]);
  }
  // Room for the shortening notice is reserved only once something must be
  // shortened: an output whose full results fit as they are carries no notice.
  const unshortened = fitResults(build, views, views, fullResultIds, continuation, false);
  if (unshortened !== undefined) return unshortened;
  const whole =
    fitResults(build, views, views, fullResultIds, continuation) ??
    fitResults(build, views, views.map(shortTask), fullResultIds, continuation);
  if (whole !== undefined) return whole;
  const short = views.map(shortTask);
  // The envelope grows with each kept child (a child's own fields are larger
  // than its id in the hint), so the largest prefix that fits is found by
  // bisection.
  let low = 0;
  let high = views.length - 1;
  let best: SubagentControlOutput | undefined;
  while (low <= high) {
    const keep = Math.floor((low + high) / 2);
    const fitted = fitResults(build, views, short.slice(0, keep), fullResultIds, continuation);
    if (fitted === undefined) {
      high = keep - 1;
    } else {
      best = fitted;
      low = keep + 1;
    }
  }
  return (
    best ?? {
      ...build(() => []),
      resultsShortened: true,
      omittedSubagentCount: views.length,
      resultsHint: `${views.length} subagents were left out of this output. Call subagent with action "wait" and at most ${SUBAGENT_CONTROL_MAX_WAIT_TARGETS} subagentSessionIds at a time.`,
    }
  );
}

/**
 * Fit the results of `kept` (a prefix of `views`, possibly with shortened
 * task texts) into the output, or return undefined when even the children
 * with every result left out exceed the budget. With `reserveNotice` false
 * no room is kept for the shortening notice, and any output that would need
 * it is refused (undefined) instead.
 *
 * The envelope is measured once with every result left out (and the notice
 * present when reserved), and each choice adds only its own byte delta to a running count.
 * The first pass gives every child its preview (a small result is its own
 * preview), or no result when even that does not fit. The second pass
 * upgrades children in `fullResultIds` to their full result, in order, while
 * the budget allows, so one large result never starves the previews of the
 * others.
 */
function fitResults(
  build: (place: PlaceViews) => SubagentControlOutput,
  views: readonly SubagentView[],
  kept: readonly SubagentView[],
  fullResultIds: ReadonlySet<string>,
  continuation: Continuation,
  reserveNotice = true,
): SubagentControlOutput | undefined {
  const keptIds = new Set(kept.map(view => view.subagentSessionId));
  const leftOut = views.filter(view => !keptIds.has(view.subagentSessionId)).map(view => view.subagentSessionId);
  const omittedCount = leftOut.length + (continuation?.tail?.count ?? 0);
  const notice = {
    resultsShortened: true,
    ...(omittedCount > 0 ? { omittedSubagentCount: omittedCount } : {}),
    resultsHint: omittedCount > 0 ? omittedHint(leftOut, continuation) : SUBAGENT_CONTROL_RESULTS_SHORTENED_HINT,
  };
  const omitted = (view: SubagentView): SubagentView =>
    view.result === undefined ? view : { ...withoutResult(view), resultOmitted: true };
  const byId = new Map(kept.map(view => [view.subagentSessionId, view]));
  const placeWith =
    (choose: (view: SubagentView) => SubagentView): PlaceViews =>
    list =>
      list.flatMap(view => {
        const keptView = byId.get(view.subagentSessionId);
        return keptView === undefined ? [] : [choose(keptView)];
      });
  let remaining =
    SUBAGENT_CONTROL_MAX_OUTPUT_BYTES - jsonBytes({ ...build(placeWith(omitted)), ...(reserveNotice ? notice : {}) });
  if (remaining < 0) return undefined;
  const fitted = new Map<string, SubagentView>();
  const upgrades: Array<{ full: SubagentView; previewBytes: number }> = [];
  for (const view of kept) {
    if (view.result === undefined) continue;
    const fullBytes = jsonBytes(view);
    let preview = view;
    let previewBytes = fullBytes;
    if (jsonBytes(view.result) > SUBAGENT_CONTROL_SMALL_RESULT_BYTES) {
      const candidate: SubagentView = {
        ...withoutResult(view),
        result: summarizeHarnessSubagentEventResult(view.result),
        resultPreview: true,
      };
      const candidateBytes = jsonBytes(candidate);
      if (candidateBytes < fullBytes) {
        preview = candidate;
        previewBytes = candidateBytes;
      }
    }
    const omittedView = omitted(view);
    const delta = previewBytes - jsonBytes(omittedView);
    if (delta > remaining) {
      fitted.set(view.subagentSessionId, omittedView);
      continue;
    }
    remaining -= delta;
    fitted.set(view.subagentSessionId, preview);
    if (preview !== view && fullResultIds.has(view.subagentSessionId)) upgrades.push({ full: view, previewBytes });
  }
  for (const { full, previewBytes } of upgrades) {
    const delta = jsonBytes(full) - previewBytes;
    if (delta > remaining) continue;
    remaining -= delta;
    fitted.set(full.subagentSessionId, full);
  }
  const original = new Map(views.map(view => [view.subagentSessionId, view]));
  const shortened =
    omittedCount > 0 ||
    kept.some(view => {
      const source = original.get(view.subagentSessionId)!;
      return (fitted.get(view.subagentSessionId) ?? view) !== source;
    });
  const output = build(placeWith(view => fitted.get(view.subagentSessionId) ?? view));
  if (!shortened) return output;
  return reserveNotice ? { ...output, ...notice } : undefined;
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
 * Settle with `work`, unless `signal` aborts or `stallAt` passes first; then
 * resolve `{ done: false }` and stop waiting. The
 * storage call behind `work` keeps running (the harness storage tracker still
 * owns it); only this caller stops waiting for it.
 */
function settleWithin<T>(
  work: Promise<T>,
  stallAt: number,
  signal: AbortSignal | undefined,
): Promise<{ done: true; value: T } | { done: false }> {
  if (signal?.aborted) {
    work.catch(() => {});
    return Promise.resolve({ done: false });
  }
  return new Promise((resolve, reject) => {
    const onStop = () => {
      clearTimeout(timer);
      signal?.removeEventListener('abort', onStop);
      resolve({ done: false });
    };
    const timer = setTimeout(onStop, Math.max(0, stallAt - Date.now()));
    signal?.addEventListener('abort', onStop, { once: true });
    work.then(
      value => {
        clearTimeout(timer);
        signal?.removeEventListener('abort', onStop);
        resolve({ done: true, value });
      },
      (err: unknown) => {
        clearTimeout(timer);
        signal?.removeEventListener('abort', onStop);
        reject(err);
      },
    );
  });
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
    returned?: ReturnedInlineSubagentState,
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
    } else if (record.closedAt !== undefined && delegated?.delegatedSubagentSessionId !== childId) {
      // Terminal lifecycle wins over suspension metadata a cancel preserves.
      status = 'closed';
    } else if (
      record.cancelRequest === undefined &&
      record.closedAt === undefined &&
      (record.pendingResume !== undefined || active?.status === 'awaiting_input')
    ) {
      // A cancel (or close) keeps `pendingResume` only as audit evidence: the
      // interaction can no longer be resumed, so it is never awaiting input.
      status = 'awaiting_input';
    } else if (delegated?.delegatedSubagentSessionId === childId) {
      // The durable link is live: the delegation driver (or reconcile) still
      // owes the settlement, even if the child row already closed.
      status = 'running';
    } else if (record.closedAt !== undefined) {
      status = 'closed';
    } else if (active !== undefined || record.closingAt !== undefined) {
      // A closing child is still draining admitted work until closedAt lands.
      status = 'running';
    } else {
      status = 'idle';
    }
    const task = attempt?.taskBody ?? active?.task;
    const startedAt = attempt?.startedAt ?? active?.startedAt ?? returned?.startedAt;
    const inlineAgentType = active?.agentType ?? returned?.agentType;
    return {
      subagentSessionId: childId,
      source:
        delegated !== undefined ? 'delegated' : active !== undefined || returned !== undefined ? 'inline' : 'session',
      status,
      settled: SETTLED_STATUSES.has(status),
      cancelRequested: record.cancelRequest !== undefined,
      live,
      ...(record.subagentTypeId !== undefined ? { agentType: record.subagentTypeId } : {}),
      ...(inlineAgentType !== undefined && record.subagentTypeId === undefined ? { agentType: inlineAgentType } : {}),
      ...(task !== undefined ? { task: preview(task) } : {}),
      ...(delegated !== undefined ? { taskId: delegated.taskId } : {}),
      ...(startedAt !== undefined ? { startedAt } : {}),
      ...(settledAt !== undefined ? { settledAt } : {}),
      ...(result !== undefined ? { result } : {}),
    };
  }

  /**
   * Resolve the views for `childIds`, or for every known child when omitted.
   * A named id that is not this session's own child is returned in `foreign`;
   * a discovered id (implicit listing) without an own row is skipped, and an
   * id in `knownOwned` whose row has since been deleted reads as closed.
   */
  async function resolveChildren(
    childIds: readonly string[] | undefined,
    opts: { limit?: number; knownOwned?: ReadonlySet<string>; offset?: number } = {},
  ): Promise<{
    views: SubagentView[];
    foreign: string[];
    truncated: boolean;
    position: Map<string, number>;
    tail?: { count: number; nextOffset: number };
  }> {
    const delegated = await loadDelegatedTasks();
    const activeById = new Map(activeSubagents().map(state => [state.subagentSessionId, state]));
    const returnedById = parent._internalReturnedInlineSubagents();
    let ids: string[];
    // An implicit list is every known child, oldest first, so a child keeps its
    // place when it settles and a new child joins at the end.
    const position = new Map<string, number>();
    if (childIds !== undefined) {
      ids = Array.from(new Set(childIds));
    } else {
      const known = new Map<string, number>();
      for (const [id, state] of activeById) known.set(id, state.startedAt);
      // An inline child whose spawn_subagent call returned in this run keeps
      // its place, so paging within a run never skips a child.
      for (const [id, state] of returnedById) if (!known.has(id)) known.set(id, state.startedAt);
      for (const [id, task] of delegated) {
        if (!known.has(id)) known.set(id, readDelegationAttemptMetadata(task, id)?.startedAt ?? task.createdAt);
      }
      const ordered = Array.from(known.entries()).sort(byStart);
      ordered.forEach(([id], index) => position.set(id, index));
      ids = ordered.map(([id]) => id).slice(opts.offset ?? 0);
    }
    const views: SubagentView[] = [];
    const foreign: string[] = [];
    for (const id of ids) {
      // One view past the limit is enough to flag truncation.
      if (opts.limit !== undefined && views.length > opts.limit) break;
      const { record, live } = await loadChildRecord(id);
      if (isOwnChild(record)) {
        views.push(
          project(
            id,
            record,
            live,
            activeById.get(id),
            delegated.get(id),
            activeById.has(id) ? undefined : returnedById.get(id),
          ),
        );
      } else if (record === null && opts.knownOwned?.has(id) === true) {
        views.push({
          subagentSessionId: id,
          source: 'session',
          status: 'closed',
          settled: true,
          cancelRequested: false,
          live: false,
        });
      } else if (childIds !== undefined) {
        foreign.push(id);
      }
    }
    const truncated = opts.limit !== undefined && views.length > opts.limit;
    if (!truncated) return { views, foreign, truncated, position };
    const nextOffset = position.get(views[opts.limit!]!.subagentSessionId)!;
    return {
      views: views.slice(0, opts.limit),
      foreign,
      truncated,
      position,
      tail: { count: position.size - nextOffset, nextOffset },
    };
  }

  function foreignError(ids: readonly string[]): SubagentControlOutput {
    return validationError('subagentSessionId', `not a subagent of this session: ${ids.join(', ')}`);
  }

  async function status(childId: string | undefined, offset = 0): Promise<SubagentControlOutput> {
    const { views, foreign, truncated, position, tail } = await resolveChildren(
      childId === undefined ? undefined : [childId],
      { limit: SUBAGENT_CONTROL_STATUS_LIMIT, offset },
    );
    if (foreign.length > 0) return foreignError(foreign);
    return boundResults(
      place => ({ action: 'status', subagents: place(views), ...(truncated ? { truncated: true } : {}) }),
      views,
      new Set(childId === undefined ? [] : [childId]),
      childId,
      childId === undefined ? { action: 'status', position, ...(tail !== undefined ? { tail } : {}) } : undefined,
    );
  }

  async function wait(
    childIds: readonly string[] | undefined,
    timeoutMs: number,
    signal: AbortSignal | undefined,
    offset: number | undefined,
  ): Promise<SubagentControlOutput> {
    const deadline = Date.now() + timeoutMs;
    let targets = childIds;
    let knownOwned: ReadonlySet<string> | undefined;
    let interval = WAIT_POLL_INTERVAL_MS;
    // The last resolved views of the pinned children, reported when a later
    // storage read stalls past the deadline or the call is aborted.
    let lastSettled: SubagentView[] = [];
    let lastRunning: SubagentView[] = [];
    // The pinned children in pinned order: an implicit wait leaves out a tail
    // of this list, and the hint gives the stable-list place of its first child.
    let lastCurrent: SubagentView[] = [];
    let position: ReadonlyMap<string, number> = new Map();
    // Full results go to named children and to children that settle during
    // this call; a child already settled when the wait began gets a preview.
    const fullResultIds = new Set(childIds ?? []);
    const namedOne = fullResultIds.size === 1 ? childIds![0] : undefined;
    let runningAtStart: ReadonlySet<string> | undefined;
    const output = (extra: { timedOut: boolean; aborted?: true }): SubagentControlOutput => {
      for (const view of lastSettled) {
        if (runningAtStart?.has(view.subagentSessionId) === true) fullResultIds.add(view.subagentSessionId);
      }
      return boundResults(
        place => ({
          action: 'wait',
          settledSubagents: place(lastSettled),
          runningSubagents: place(lastRunning),
          ...extra,
        }),
        childIds === undefined ? lastCurrent : [...lastSettled, ...lastRunning],
        fullResultIds,
        namedOne,
        childIds === undefined ? { action: 'wait', position } : undefined,
      );
    };
    for (;;) {
      // The deadline decides when to stop polling, not how long a started read
      // may take: every read may run at least SUBAGENT_CONTROL_WAIT_READ_STALL_MS,
      // even when it starts at the deadline.
      const resolution = await settleWithin(
        resolveChildren(
          targets,
          targets === undefined ? { offset: offset ?? 0 } : knownOwned === undefined ? {} : { knownOwned },
        ),
        Math.max(deadline, Date.now() + SUBAGENT_CONTROL_WAIT_READ_STALL_MS),
        signal,
      );
      if (!resolution.done) {
        const aborted = signal?.aborted === true;
        if (!aborted && knownOwned === undefined) {
          // The first read proves ownership and reads the initial views; with it
          // stalled both are unknown, so this is a storage failure, not a timeout.
          return {
            isError: true,
            errorName: 'HarnessStorageError',
            code: 'harness.storage',
            message:
              `The subagent state read did not answer within ${SUBAGENT_CONTROL_WAIT_READ_STALL_MS} ms; ` +
              'nothing is known about the subagents yet. Retry the wait.',
          };
        }
        return output({ timedOut: !aborted, ...(aborted ? { aborted: true as const } : {}) });
      }
      const { views, foreign } = resolution.value;
      if (foreign.length > 0) return foreignError(foreign);
      // Ownership is proven once; a child row deleted mid-wait then reads as closed.
      knownOwned ??= new Set(views.map(view => view.subagentSessionId));
      runningAtStart ??= new Set(views.filter(view => !view.settled).map(view => view.subagentSessionId));
      // An implicit wait pins every child that was running when it started;
      // with an offset it pins every child from that place of the stable list,
      // so the settled ones on its page are returned too.
      if (targets === undefined) {
        position = resolution.value.position;
        targets = views.filter(view => offset !== undefined || !view.settled).map(view => view.subagentSessionId);
      }
      const pinned = new Set(targets);
      const current = views.filter(view => pinned.has(view.subagentSessionId));
      const running = current.filter(view => !view.settled);
      lastSettled = current.filter(view => view.settled);
      lastRunning = running;
      lastCurrent = current;
      const aborted = signal?.aborted === true;
      const timedOut = running.length > 0 && !aborted && Date.now() >= deadline;
      if (running.length === 0 || timedOut || aborted) {
        return output({ timedOut, ...(aborted && running.length > 0 ? { aborted: true as const } : {}) });
      }
      await abortable(Math.min(interval, Math.max(0, deadline - Date.now())), signal);
      interval = Math.min(interval * 2, WAIT_POLL_MAX_INTERVAL_MS);
    }
  }

  async function send(
    childId: string,
    message: string,
    signal: AbortSignal | undefined,
  ): Promise<SubagentControlOutput> {
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
    if (view.status === 'awaiting_input') {
      return validationError(
        'subagentSessionId',
        'subagent is awaiting human input; it cannot take a message until that interaction is resolved',
      );
    }
    // Re-prove the parent turn after the async resolution above.
    parent._internalAssertSubagentControlAdmitted(signal);
    try {
      // Interleave-only: a child without an active run is never woken into a
      // new, unsupervised turn.
      await child.injectSystemReminder(
        message,
        { attributes: { source: 'parent_session', parentSessionId: parent.id } },
        { requireActiveRun: true, assertAdmitted: () => parent._internalAssertSubagentControlAdmitted(signal) },
      );
    } catch (err) {
      if (err instanceof HarnessValidationError) {
        return validationError('subagentSessionId', 'subagent has no active run to deliver the message to');
      }
      throw err;
    }
    return { action: 'send', subagentSessionId: childId, delivered: true, status: view.status };
  }

  async function cancel(childId: string, signal: AbortSignal | undefined): Promise<SubagentControlOutput> {
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
    // A repeated cancel still runs: it resumes an incomplete descendant cascade.
    const wasRequested = view.cancelRequested;
    let delivery = await parent._internalCancelOwnedSubagent(childId, SUBAGENT_CONTROL_CANCEL_REASON, signal);
    const stored = await storage.loadSession({ harnessName, sessionId: childId });
    if (stored !== null && stored.closedAt === undefined && stored.closingAt !== undefined && !stored.cancelRequest) {
      // The cascade skips a closing child; its close is already draining it.
      return {
        isError: true,
        errorName: 'HarnessSubagentClosingError',
        code: 'harness.subagent_closing',
        message: 'The subagent is already closing; wait for it to settle.',
        subagentSessionId: childId,
      };
    }
    if (stored === null || (stored.closedAt !== undefined && !stored.cancelRequest)) {
      // The cascade skips a gone/closed child without writing anything.
      return {
        action: 'cancel',
        subagentSessionId: childId,
        cancel: 'already_settled',
        settled: true,
        status: 'closed',
      };
    }
    if (delivery === 'unavailable' && stored.cancelRequest !== undefined && !wasRequested) {
      // The child committed the cancel; only its own descendant discovery failed.
      delivery = 'delivered_incomplete';
    }
    if (delivery === 'unavailable') {
      // Never write under another process's lease: its holder would then fail
      // its own CAS writes, including close. A foreign owner below a live parent
      // is a split subtree that the parent's lease renewal fences, so there is
      // nothing useful to wait for here.
      const heldElsewhere =
        stored.ownerId !== undefined &&
        stored.ownerId !== parent._internalOwnerId &&
        (stored.leaseExpiresAt ?? 0) > Date.now();
      return {
        isError: true,
        errorName: 'HarnessSubagentCancelUnavailableError',
        code: 'harness.subagent_cancel_unavailable',
        message: heldElsewhere
          ? 'The subagent is held by another process and could not be cancelled from here; this call recorded no cancel. Retry later.'
          : 'The subagent could not be reached to cancel it; this call recorded no cancel. Retry later.',
        subagentSessionId: childId,
      };
    }
    const incomplete = delivery === 'delivered_incomplete';
    const after = (await resolveChildren([childId])).views[0];
    return {
      action: 'cancel',
      subagentSessionId: childId,
      cancel: wasRequested ? 'already_requested' : 'acknowledged',
      settled: (after?.settled ?? false) && !incomplete,
      ...(incomplete ? { descendantCancelIncomplete: true } : {}),
      ...(after !== undefined ? { status: after.status } : {}),
    };
  }

  return createTool({
    id: SUBAGENT_CONTROL_TOOL_ID,
    description:
      'Operate on subagents this session started with spawn_subagent or task_delegate. You can only act ' +
      'on your own subagents. ' +
      '`status` lists them (or inspects one) with their status. Only a settled task_delegate subagent ' +
      'carries its result; a spawn_subagent result is returned by that spawn_subagent call only. ' +
      `Listings and multi-subagent waits stay under ${SUBAGENT_CONTROL_MAX_OUTPUT_BYTES} bytes: a subagent ` +
      'you name, or one that settles during a wait, gets its full result while it fits; others get a preview ' +
      '(`resultPreview`), and a result that does not fit is left out (`resultOmitted`). If the subagents still ' +
      'do not fit, task texts are shortened, then the last subagents are left out (`omittedSubagentCount`) and ' +
      '`resultsHint` gives the `offset` that reads on from them; `resultsShortened` marks such an output. ' +
      `\`status\` lists at most ${SUBAGENT_CONTROL_STATUS_LIMIT} subagents per call, and \`resultsHint\` ` +
      'gives the offset of the next ones. Listings without ids are ordered oldest first (by start time, then ' +
      'id). `offset` is a place in that list of all your subagents, running and settled; a subagent keeps its ' +
      'place when it settles, and `wait` with an offset also returns the settled subagents from that place ' +
      'on. Places can shift only when a subagent starts between calls with the same or an earlier start ' +
      'time, or when a new run begins: a spawn_subagent subagent whose call returned in an earlier run is ' +
      'no longer listed (a message that joins the current run does not start a new one). A call ' +
      'naming exactly one subagent always returns its full result, so call `status` with only that ' +
      'subagentSessionId to read it. ' +
      '`wait` blocks until the named subagents (or every running one) settle, or the timeout passes, and ' +
      'returns those settled and those still running. If the first status read stalls, wait fails with ' +
      '`harness.storage` (retry it); if a later read stalls past the timeout, the lists are the statuses from ' +
      'its last completed read and may be stale. ' +
      '`send` adds an instruction to a subagent whose run is in progress. It is rejected for a subagent ' +
      'that is idle or waiting for human input, and a message the run ends without reading is dropped. ' +
      '`cancel` stops one subagent: `cancel: "acknowledged"` means the cancel is recorded on the subagent ' +
      'and its work aborted, and `settled` says whether it has finished unwinding. Cancel needs the ' +
      'subagent to be free or held by this process; if another process holds it, cancel returns ' +
      '`harness.subagent_cancel_unavailable` and changes nothing, so retry later.',
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
            return await status(input.subagentSessionId, input.offset ?? 0);
          case 'wait':
            return await wait(ids, input.timeoutMs ?? SUBAGENT_CONTROL_DEFAULT_WAIT_MS, ctx?.abortSignal, input.offset);
          case 'send':
            return await send(input.subagentSessionId!, input.message!, ctx?.abortSignal);
          case 'cancel':
            return await cancel(input.subagentSessionId!, ctx?.abortSignal);
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
