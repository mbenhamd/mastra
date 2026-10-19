import type { WorkflowRunState } from '../workflows';
import type {
  WorkflowSnapshotHandoffCanonicalState,
  WorkflowSnapshotHandoffCursor,
  WorkflowSnapshotHandoffRecord,
} from './types';

const WORKFLOW_HANDOFF_UNSAFE_JSON_UNICODE_ESCAPE_RE = new RegExp(
  String.raw`(?<!\\)((?:\\\\)*)(?:(\\u[Dd][89AaBb][0-9A-Fa-f]{2}\\u[Dd][CcDdEeFf][0-9A-Fa-f]{2})|\\u(?:0000|[Dd][89A-Fa-f][0-9A-Fa-f]{2}))`,
  'g',
);

/** Raised when an ordinary native writer attempts to mutate a fenced run. */
export class WorkflowSnapshotHandoffFenceError extends TypeError {
  readonly code = 'WORKFLOW_SNAPSHOT_HANDOFF_FENCED';
  readonly workflowName: string;
  readonly runId: string;
  readonly handoffStatus: 'pending' | 'completed';

  constructor({
    workflowName,
    runId,
    handoffStatus,
  }: {
    workflowName: string;
    runId: string;
    handoffStatus: 'pending' | 'completed';
  }) {
    super(`Workflow snapshot handoff fence is held for ${workflowName}/${runId}`);
    this.name = 'WorkflowSnapshotHandoffFenceError';
    this.workflowName = workflowName;
    this.runId = runId;
    this.handoffStatus = handoffStatus;
  }
}

/**
 * Raised when a storage adapter finds its own handoff bookkeeping
 * inconsistent (for example, a row locked for an update was not updated).
 * It signals a storage defect, never invalid input; the transaction that
 * found it is rolled back.
 */
export class WorkflowSnapshotHandoffInvariantError extends Error {
  readonly code = 'WORKFLOW_SNAPSHOT_HANDOFF_INVARIANT';
  readonly workflowName: string;
  readonly runId: string;

  constructor({ workflowName, runId, message }: { workflowName: string; runId: string; message: string }) {
    super(`${message} for ${workflowName}/${runId}`);
    this.name = 'WorkflowSnapshotHandoffInvariantError';
    this.workflowName = workflowName;
    this.runId = runId;
  }
}

/**
 * Raised when a generation-guarded `persistWorkflowSnapshot` finds the stored
 * row missing or owned by a different execution generation — the caller's
 * execution lifetime ended (deletion-tombstone reopen), so writing its
 * snapshot would resurrect or overwrite the reopened run's row.
 */
export class WorkflowStaleSnapshotPersistError extends TypeError {
  readonly code = 'WORKFLOW_SNAPSHOT_PERSIST_STALE_GENERATION';
  readonly workflowName: string;
  readonly runId: string;

  constructor({ workflowName, runId }: { workflowName: string; runId: string }) {
    super(`Workflow snapshot persist rejected a stale execution generation for ${workflowName}/${runId}`);
    this.name = 'WorkflowStaleSnapshotPersistError';
    this.workflowName = workflowName;
    this.runId = runId;
  }
}

/**
 * Matches the stale-persist rejection across `@mastra/core` module instances:
 * a CJS-loaded store adapter and an ESM consumer can resolve separate copies
 * of the class, defeating `instanceof`. The stable `code` field is the
 * cross-instance discriminator.
 */
export function isWorkflowStaleSnapshotPersistError(error: unknown): error is WorkflowStaleSnapshotPersistError {
  return (
    error instanceof WorkflowStaleSnapshotPersistError ||
    (typeof error === 'object' &&
      error !== null &&
      (error as { code?: unknown }).code === 'WORKFLOW_SNAPSHOT_PERSIST_STALE_GENERATION')
  );
}

function sortCanonicalJson(value: unknown): unknown {
  if (value === null || typeof value !== 'object') return value;
  if (Array.isArray(value)) return value.map(sortCanonicalJson);
  const record = value as Record<string, unknown>;
  return Object.fromEntries(
    Object.keys(record)
      .sort()
      .map(key => [key, sortCanonicalJson(record[key])]),
  );
}

function canonicalize(value: unknown): unknown {
  // Compare on the exact JSON projection durable adapters persist. Plain
  // JSON.stringify keeps enumerable properties assigned onto an Error (name,
  // cause, custom fields) and honors custom toJSON, so a live value and its
  // stored JSONB round-trip canonicalize identically. In-memory snapshot
  // clones preserve each Error property's original enumerability for the same
  // reason, so no Error-specific handling is needed here.
  const serialized = JSON.stringify(value);
  if (serialized === undefined) return undefined;
  const sanitized = serialized
    .replace(WORKFLOW_HANDOFF_UNSAFE_JSON_UNICODE_ESCAPE_RE, '$1$2')
    .replace(/(^|[^\\])(\\(?!["\\/bfnrtu]))/g, '$1\\\\');
  return sortCanonicalJson(JSON.parse(sanitized));
}

/** Materializes a handoff snapshot using the JSON representation persisted by durable adapters. */
export function materializeWorkflowSnapshotHandoffSnapshot(snapshot: WorkflowRunState): WorkflowRunState {
  const materialized = canonicalize(snapshot);
  if (materialized === undefined) throw new TypeError('Workflow snapshot handoff snapshot must be JSON-serializable');
  if (!materialized || typeof materialized !== 'object' || Array.isArray(materialized)) {
    throw new TypeError('Workflow snapshot handoff snapshot must be a JSON object');
  }
  return materialized as WorkflowRunState;
}

/** Compares the JSON-native snapshot representation independent of key order. */
export function workflowSnapshotHandoffSnapshotsEqual(left: WorkflowRunState, right: WorkflowRunState): boolean {
  try {
    const leftCanonical = canonicalize(left);
    const rightCanonical = canonicalize(right);
    if (leftCanonical === undefined || rightCanonical === undefined) return false;
    return JSON.stringify(leftCanonical) === JSON.stringify(rightCanonical);
  } catch {
    return false;
  }
}

export function workflowSnapshotHandoffCanonicalStatesEqual(
  left: WorkflowSnapshotHandoffCanonicalState,
  right: WorkflowSnapshotHandoffCanonicalState,
): boolean {
  if (left.kind !== right.kind) return false;
  if (left.kind === 'absent' || right.kind === 'absent') return true;
  return left.resourceId === right.resourceId && workflowSnapshotHandoffSnapshotsEqual(left.snapshot, right.snapshot);
}

function compareWorkflowSnapshotHandoffText(left: string, right: string): number {
  // PostgreSQL orders these columns under COLLATE "C" (UTF-8 byte order), which
  // is code-point order. Iterate code points so in-memory ordering matches
  // exactly; UTF-16 code-unit comparison would invert astral characters.
  const leftPoints = Array.from(left);
  const rightPoints = Array.from(right);
  const shared = Math.min(leftPoints.length, rightPoints.length);
  for (let index = 0; index < shared; index++) {
    const leftPoint = leftPoints[index]!.codePointAt(0)!;
    const rightPoint = rightPoints[index]!.codePointAt(0)!;
    if (leftPoint !== rightPoint) return leftPoint - rightPoint;
  }
  return leftPoints.length - rightPoints.length;
}

/** Uses the same code-point tuple ordering for in-memory sort and cursor filtering. */
export function compareWorkflowSnapshotHandoffCursors(
  left: WorkflowSnapshotHandoffCursor,
  right: WorkflowSnapshotHandoffCursor,
): number {
  return (
    left.updatedAt - right.updatedAt ||
    compareWorkflowSnapshotHandoffText(left.workflowName, right.workflowName) ||
    compareWorkflowSnapshotHandoffText(left.runId, right.runId)
  );
}

export function validateWorkflowSnapshotHandoffFence(mutationFence: string): void {
  if (typeof mutationFence !== 'string' || mutationFence.length < 1 || mutationFence.length > 4096) {
    throw new TypeError('Workflow snapshot handoff mutationFence must be between 1 and 4096 characters');
  }
  if (hasUnsafeIdentityChar(mutationFence)) {
    throw new TypeError('Workflow snapshot handoff mutationFence must be well-formed UTF-16 with no NUL characters');
  }
}

function hasUnsafeIdentityChar(value: string): boolean {
  // Durable adapters transmit strings as UTF-8 into text columns: unpaired
  // surrogates encode as U+FFFD on the PostgreSQL wire and NUL is rejected
  // outright — either would rewrite fence tokens and row identities or fail
  // only on durable adapters, collapsing in-memory/PostgreSQL parity.
  for (let i = 0; i < value.length; i++) {
    const unit = value.charCodeAt(i);
    if (unit === 0) return true;
    if (unit >= 0xd800 && unit <= 0xdbff) {
      const next = value.charCodeAt(i + 1);
      if (Number.isNaN(next) || next < 0xdc00 || next > 0xdfff) return true;
      i++;
    } else if (unit >= 0xdc00 && unit <= 0xdfff) {
      return true;
    }
  }
  return false;
}

/**
 * The release rule shared by every adapter, applied to the locked row:
 * - another claim generation than the caller saw is a conflict;
 * - a supplied fence must match the stored fence exactly;
 * - without a fence only a completed handoff may be released.
 */
export function isWorkflowSnapshotHandoffReleasable(
  record: Pick<WorkflowSnapshotHandoffRecord, 'status' | 'mutationFence' | 'claimGeneration'>,
  expectedClaimGeneration: number,
  mutationFence: string | undefined,
): boolean {
  if (record.claimGeneration !== expectedClaimGeneration) return false;
  if (mutationFence !== undefined) return record.mutationFence === mutationFence;
  return record.status === 'completed';
}

/** Validates the release compare-and-set token. */
export function validateWorkflowSnapshotHandoffClaimGeneration(expectedClaimGeneration: number): void {
  if (!Number.isSafeInteger(expectedClaimGeneration) || expectedClaimGeneration < 1) {
    throw new RangeError('Workflow snapshot handoff expectedClaimGeneration must be a positive safe integer');
  }
}

/**
 * Whether a claim of a released row keeps it as the retained outcome of the
 * same claim rather than claiming the row again: a completed row claimed
 * under its own fence answers `completed`.
 */
export function isWorkflowSnapshotHandoffRetainedOutcome(
  record: Pick<WorkflowSnapshotHandoffRecord, 'status' | 'mutationFence'>,
  mutationFence: string,
): boolean {
  return record.status === 'completed' && record.mutationFence === mutationFence;
}

/** Rejects handoff identity fields that cannot round-trip through UTF-8 encoding. */
export function validateWorkflowSnapshotHandoffIdentity(
  workflowName: string,
  runId: string,
  resourceId?: string,
  expectedResourceId?: string,
): void {
  if (hasUnsafeIdentityChar(workflowName) || hasUnsafeIdentityChar(runId)) {
    throw new TypeError(
      'Workflow snapshot handoff workflowName/runId must be well-formed UTF-16 with no NUL characters',
    );
  }
  if (
    (resourceId !== undefined && hasUnsafeIdentityChar(resourceId)) ||
    (expectedResourceId !== undefined && hasUnsafeIdentityChar(expectedResourceId))
  ) {
    throw new TypeError('Workflow snapshot handoff resourceId must be well-formed UTF-16 with no NUL characters');
  }
}

/**
 * Freezes a caller-owned CAS guard to the JSON projection both adapters
 * compare, so payload getters fired during capture or mid-transaction
 * mutation through the caller's retained reference cannot retarget the
 * expectation after it is pinned.
 */
export function pinWorkflowCasGuardValue<T>(value: T): T {
  if (value === undefined || value === null) return value;
  const json = JSON.stringify(value);
  if (json === undefined) {
    throw new TypeError('Workflow CAS guard value must be JSON-serializable');
  }
  return JSON.parse(json) as T;
}

export function validateWorkflowSnapshotHandoffLimit(limit: number | undefined): number {
  const resolved = limit ?? 100;
  if (!Number.isSafeInteger(resolved) || resolved < 1 || resolved > 100) {
    throw new RangeError('Workflow snapshot handoff recovery limit must be between 1 and 100');
  }
  return resolved;
}

/** Snapshot statuses after which a run takes no further native steps. */
export const SETTLED_WORKFLOW_RUN_STATUSES = [
  'success',
  'failed',
  'canceled',
  'tripwire',
  'bailed',
  'skipped',
] as const;

export function isSettledWorkflowRunStatus(status: unknown): boolean {
  return (SETTLED_WORKFLOW_RUN_STATUSES as readonly unknown[]).includes(status);
}

/** Validates a closure erasure's identity and resource before any read. */
export function validateWorkflowRunClosureIdentity(workflowName: string, runId: string, resourceId: string): void {
  // Empty names are allowed: persistWorkflowSnapshot stores them, so their
  // closures must stay erasable.
  if (typeof workflowName !== 'string') {
    throw new TypeError('Workflow run closure workflowName must be a string');
  }
  if (typeof runId !== 'string') {
    throw new TypeError('Workflow run closure runId must be a string');
  }
  if (typeof resourceId !== 'string' || resourceId.length === 0) {
    throw new TypeError('Workflow run closure resourceId must be a non-empty string');
  }
  validateWorkflowSnapshotHandoffIdentity(workflowName, runId, resourceId);
}

/**
 * Reads a closure listing's input exactly once and validates that copy, so
 * accessor-backed inputs cannot hand the query a different value than the
 * one validated. Adapters use only the returned copy.
 */
export function captureWorkflowRunClosureListInput(input: {
  resourceId: string;
  after?: { workflowName: string; runId: string };
  limit: number;
}): { resourceId: string; after?: { workflowName: string; runId: string }; limit: number } {
  const resourceId: unknown = input.resourceId;
  const limit: unknown = input.limit;
  const rawAfter: unknown = input.after;
  let after: { workflowName: string; runId: string } | undefined;
  if (rawAfter !== undefined) {
    const cursor = rawAfter as { workflowName: unknown; runId: unknown };
    const workflowName = cursor.workflowName;
    const runId = cursor.runId;
    if (typeof workflowName !== 'string' || typeof runId !== 'string') {
      throw new TypeError('Workflow run closure cursor must carry workflowName and runId strings');
    }
    after = { workflowName, runId };
  }
  if (typeof resourceId !== 'string' || resourceId.length === 0) {
    throw new TypeError('Workflow run closure resourceId must be a non-empty string');
  }
  if (typeof limit !== 'number' || !Number.isSafeInteger(limit) || limit < 1 || limit > 100) {
    throw new RangeError('Workflow run closure limit must be between 1 and 100');
  }
  validateWorkflowSnapshotHandoffIdentity(after?.workflowName ?? '', after?.runId ?? '', resourceId);
  return Object.freeze({ resourceId, limit, ...(after ? { after: Object.freeze(after) } : {}) });
}

/**
 * Reads a thread erasure listing's input exactly once and validates that
 * copy, as {@link captureWorkflowRunClosureListInput} does for the resource,
 * cursor and limit. Adapters use only the returned copy.
 */
export function captureWorkflowRunThreadErasureListInput(input: {
  resourceId: string;
  threadId: string;
  after?: { workflowName: string; runId: string };
  limit: number;
  includeUnattributed?: boolean;
}): {
  resourceId: string;
  threadId: string;
  after?: { workflowName: string; runId: string };
  limit: number;
  includeUnattributed: boolean;
} {
  const threadId: unknown = input.threadId;
  const includeUnattributed: unknown = input.includeUnattributed;
  const closureInput = captureWorkflowRunClosureListInput(input);
  if (typeof threadId !== 'string' || threadId.length === 0) {
    throw new TypeError('Workflow run thread erasure threadId must be a non-empty string');
  }
  if (hasUnsafeIdentityChar(threadId)) {
    throw new TypeError('Workflow run thread erasure threadId must be well-formed UTF-16 with no NUL characters');
  }
  if (includeUnattributed !== undefined && typeof includeUnattributed !== 'boolean') {
    throw new TypeError('Workflow run thread erasure includeUnattributed must be a boolean');
  }
  return Object.freeze({ ...closureInput, threadId, includeUnattributed: includeUnattributed === true });
}

/**
 * The locations where the framework itself writes a thread id, each anchored
 * on a known structure so application data shaped like thread metadata never
 * attributes a run. A group's `guard` must hold for its paths to count.
 */
type ThreadIdPathGroup = { guard?: string; paths: readonly string[] };

/** The durable agent's workflow (`DurableStepIds.AGENTIC_LOOP`). */
const DURABLE_AGENTIC_LOOP_WORKFLOW = 'durable-agentic-loop';

const SNAPSHOT_THREAD_ID_PATH_GROUPS: readonly ThreadIdPathGroup[] = [
  // The agentic loop's suspension: tool-call-step stores streamState.serialize().
  { paths: ['@.context.*.suspendPayload.__streamState.messageList.memoryInfo.threadId'] },
  // A durable-agent run: its input, step outputs and result each carry the
  // serialized MessageList and the SerializableDurableState.
  {
    guard: '@.context.input.__workflowKind == "durable-agent"',
    paths: [
      '@.context.input.messageListState.memoryInfo.threadId',
      '@.context.input.state.threadId',
      '@.context.*.output.messageListState.memoryInfo.threadId',
      '@.context.*.output.state.threadId',
      '@.result.messageListState.memoryInfo.threadId',
      '@.result.state.threadId',
      // persistWorkflowTerminalState stores the whole step result as `result`.
      '@.result.output.messageListState.memoryInfo.threadId',
      '@.result.output.state.threadId',
    ],
  },
  // The request context persisted with every started run.
  { paths: ['@.requestContext.MastraMemory.thread.id', '@.requestContext."mastra__threadId"'] },
];

const TERMINAL_ENVELOPE_THREAD_ID_PATH_GROUPS: readonly ThreadIdPathGroup[] = [
  {
    guard: `@.workflowName == "${DURABLE_AGENTIC_LOOP_WORKFLOW}"`,
    // terminalResult is the child's step result; the loop output is its `output`.
    paths: ['@.terminalResult.output.messageListState.memoryInfo.threadId', '@.terminalResult.output.state.threadId'],
  },
  { paths: ['@.requestContextPatch.MastraMemory.thread.id', '@.requestContextPatch."mastra__threadId"'] },
];

const threadIdPredicate = (groups: readonly ThreadIdPathGroup[], test: (path: string) => string) =>
  `$ ? (${groups
    .map(group => {
      const paths = group.paths.map(test).join(' || ');
      return group.guard ? `(${group.guard} && (${paths}))` : paths;
    })
    .join(' || ')})`;
const threadIdMatchPath = (groups: readonly ThreadIdPathGroup[]) => threadIdPredicate(groups, path => `${path} == $t`);
const anyThreadIdPath = (groups: readonly ThreadIdPathGroup[]) =>
  threadIdPredicate(groups, path => `exists(${path} ? (@.type() == "string"))`);

/**
 * SQL/JSON path (PostgreSQL `jsonb_path_exists`, lax mode) that is true when
 * a snapshot (canonical or handoff) records the thread id bound to the `t`
 * variable at a framework-owned location: an agentic-loop suspension's
 * `__streamState`, a durable-agent run's MessageList or durable state (input,
 * step outputs, result), or the request context persisted with every started
 * run (`requestContext.MastraMemory.thread.id`, `requestContext.mastra__threadId`).
 * `== $t` only holds for a string equal to the bound string.
 */
export const WORKFLOW_SNAPSHOT_THREAD_ERASURE_MATCH_PATH = threadIdMatchPath(SNAPSHOT_THREAD_ID_PATH_GROUPS);

/**
 * SQL/JSON path that is true when a snapshot records any string thread id at
 * a location {@link WORKFLOW_SNAPSHOT_THREAD_ERASURE_MATCH_PATH} reads. A
 * snapshot it is false for is unattributed.
 */
export const WORKFLOW_SNAPSHOT_THREAD_ERASURE_ANY_THREAD_PATH = anyThreadIdPath(SNAPSHOT_THREAD_ID_PATH_GROUPS);

/**
 * SQL/JSON path that is true when a terminal recovery envelope records the
 * thread id bound to `t`: its request-context patch (`MastraMemory.thread.id`,
 * `mastra__threadId`) or, for a durable-agent run, the MessageList or durable
 * state of its step result's `output`.
 */
export const WORKFLOW_TERMINAL_ENVELOPE_THREAD_ERASURE_MATCH_PATH = threadIdMatchPath(
  TERMINAL_ENVELOPE_THREAD_ID_PATH_GROUPS,
);

/** SQL/JSON path that is true when a terminal envelope records any string thread id where the match path reads. */
export const WORKFLOW_TERMINAL_ENVELOPE_THREAD_ERASURE_ANY_THREAD_PATH = anyThreadIdPath(
  TERMINAL_ENVELOPE_THREAD_ID_PATH_GROUPS,
);

/** Own enumerable data fields of a JSON-like object, as a JSON copy would keep them. Accessors are not run. */
function threadErasureMembers(value: unknown, key: string): unknown[] {
  // Lax mode: a member accessor applied to an array applies to its elements.
  const holders = Array.isArray(value) ? value : [value];
  const members: unknown[] = [];
  for (const holder of holders) {
    if (holder === null || typeof holder !== 'object' || Array.isArray(holder)) continue;
    // `.*` reads every member; `.key` reads one.
    for (const name of key === '*' ? Object.keys(holder) : [key]) {
      const descriptor = Object.getOwnPropertyDescriptor(holder, name);
      if (descriptor && descriptor.enumerable && 'value' in descriptor && descriptor.value !== undefined) {
        members.push(descriptor.value);
      }
    }
  }
  return members;
}

function threadErasureMemberPath(roots: unknown[], keys: readonly string[]): unknown[] {
  let items = roots;
  for (const key of keys) items = items.flatMap(item => threadErasureMembers(item, key));
  // Lax mode: comparison operands that are arrays are unwrapped.
  return items.flatMap(item => (Array.isArray(item) ? item : [item]));
}

/** Values at a dotted member path (`*` for every member), mirroring a lax SQL/JSON accessor chain. */
const at = (root: unknown, path: string): unknown[] => threadErasureMemberPath([root], path.split('.'));

/** Lax `==` against a string: true when any item is that string. */
const holds = (root: unknown, path: string, value: string): boolean => at(root, path).includes(value);

/**
 * Classifies a plain-data snapshot for a thread erasure listing at the same
 * locations as {@link WORKFLOW_SNAPSHOT_THREAD_ERASURE_MATCH_PATH}: `thread`
 * when it records the thread id, `other` when it records only other thread
 * ids, `unattributed` when it records none. Storage adapters without SQL/JSON
 * path support (the in-memory store) use this on a detached copy.
 */
export function classifyWorkflowSnapshotThreadErasure(
  snapshot: unknown,
  threadId: string,
): 'thread' | 'other' | 'unattributed' {
  const durable = holds(snapshot, 'context.input.__workflowKind', 'durable-agent');
  return classifyThreadErasureValues(
    [
      ...at(snapshot, 'context.*.suspendPayload.__streamState.messageList.memoryInfo.threadId'),
      ...(durable
        ? [
            'context.input.messageListState.memoryInfo.threadId',
            'context.input.state.threadId',
            'context.*.output.messageListState.memoryInfo.threadId',
            'context.*.output.state.threadId',
            'result.messageListState.memoryInfo.threadId',
            'result.state.threadId',
            'result.output.messageListState.memoryInfo.threadId',
            'result.output.state.threadId',
          ].flatMap(path => at(snapshot, path))
        : []),
      ...at(snapshot, 'requestContext.MastraMemory.thread.id'),
      ...at(snapshot, 'requestContext.mastra__threadId'),
    ],
    threadId,
  );
}

/**
 * Classifies a plain-data terminal recovery envelope at the same locations as
 * {@link WORKFLOW_TERMINAL_ENVELOPE_THREAD_ERASURE_MATCH_PATH}.
 */
export function classifyWorkflowTerminalEnvelopeThreadErasure(
  envelope: unknown,
  threadId: string,
): 'thread' | 'other' | 'unattributed' {
  const durable = holds(envelope, 'workflowName', DURABLE_AGENTIC_LOOP_WORKFLOW);
  return classifyThreadErasureValues(
    [
      ...(durable
        ? [
            ...at(envelope, 'terminalResult.output.messageListState.memoryInfo.threadId'),
            ...at(envelope, 'terminalResult.output.state.threadId'),
          ]
        : []),
      ...at(envelope, 'requestContextPatch.MastraMemory.thread.id'),
      ...at(envelope, 'requestContextPatch.mastra__threadId'),
    ],
    threadId,
  );
}

function classifyThreadErasureValues(found: unknown[], threadId: string): 'thread' | 'other' | 'unattributed' {
  const values = found.filter((value): value is string => typeof value === 'string');
  if (values.includes(threadId)) return 'thread';
  return values.length > 0 ? 'other' : 'unattributed';
}

/**
 * The `contentFree` rule of an unattributed thread erasure entry, over a
 * plain-data snapshot: `context` absent, `null` or `{}`, or only an `input`
 * that is `null` or `{}`; and `requestContext` absent, `null` or `{}`.
 * `{}` means a plain object with no fields; a class instance with no own
 * fields (a `Date`) counts as content, as its JSON form does. Accessors are
 * not run (an accessor field counts as content).
 */
export function isWorkflowSnapshotThreadErasureContentFree(snapshot: unknown): boolean {
  if (snapshot === null || typeof snapshot !== 'object' || Array.isArray(snapshot)) return false;
  const field = (holder: object, key: string): { present: boolean; value?: unknown; accessor?: boolean } => {
    const descriptor = Object.getOwnPropertyDescriptor(holder, key);
    if (!descriptor || !descriptor.enumerable) return { present: false };
    if (!('value' in descriptor)) return { present: true, accessor: true };
    return descriptor.value === undefined ? { present: false } : { present: true, value: descriptor.value };
  };
  // Only a plain object can be empty: a class instance (a Date, a URL, a
  // Buffer) has no own keys but serializes to content, so it counts as content.
  const isPlainObject = (value: unknown): value is object => {
    if (value === null || typeof value !== 'object' || Array.isArray(value)) return false;
    const prototype: unknown = Object.getPrototypeOf(value);
    return prototype === Object.prototype || prototype === null;
  };
  const isEmptyObject = (value: unknown): boolean =>
    isPlainObject(value) && Object.keys(value).every(key => field(value, key).present === false);
  const nullOrEmpty = (entry: { present: boolean; value?: unknown; accessor?: boolean }): boolean =>
    !entry.present || (!entry.accessor && (entry.value === null || isEmptyObject(entry.value)));
  if (!nullOrEmpty(field(snapshot, 'requestContext'))) return false;
  const context = field(snapshot, 'context');
  if (nullOrEmpty(context)) return true;
  if (context.accessor || !isPlainObject(context.value)) return false;
  const contextValue = context.value;
  const keys = Object.keys(contextValue).filter(key => field(contextValue, key).present);
  return keys.length === 1 && keys[0] === 'input' && nullOrEmpty(field(contextValue, 'input'));
}

/** Orders closure cursors by code point, matching PostgreSQL's `COLLATE "C"` keyset. */
export function compareWorkflowRunClosureCursors(
  left: { workflowName: string; runId: string },
  right: { workflowName: string; runId: string },
): number {
  return (
    compareWorkflowSnapshotHandoffText(left.workflowName, right.workflowName) ||
    compareWorkflowSnapshotHandoffText(left.runId, right.runId)
  );
}
