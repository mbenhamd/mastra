/**
 * Harness v1 — runtime Session class.
 *
 * This is the in-memory authority for a single SessionRecord (§5.4). The
 * Harness creates one instance per live session and routes all writes to
 * the underlying record through it. The full surface is described in §4.2.
 *
 * The current local surface includes message/signal/queue turns, mode/model
 * and state mutation, display snapshots, message listing, pending inbox
 * responses, permissions, code/workspace skills, subagents, goals, event
 * forwarding, abort, idle waiting, wakeup queue admission, and the core
 * admission/mutation primitives used by remote routes, plus request-context
 * `registerQuestion` / `registerPlanApproval` / `registerSandboxAccess`
 * pending registration. Remote SDKs and full channel routing remain
 * follow-up lanes.
 *
 * Lifecycle states tracked here:
 *   - 'live'    — session is in the harness's live map and holds the lease.
 *   - 'closing' — the durable close marker has committed; new work is rejected
 *                 while previously admitted turns drain until the close deadline.
 *   - 'closed'  — `close()` has run; record has `closedAt` set in storage.
 *   - 'deleted' — the session row has been hard-deleted from storage.
 *   - 'evicted' — flushed to storage and dropped from live map; the record
 *                 remains active and the session can be re-hydrated after
 *                 fail-closed lease loss.
 *
 * Once a Session leaves 'live', every method except identity reads throws.
 * Callers must re-resolve via `harness.session(...)` to get a fresh instance.
 */

import { randomUUID } from 'node:crypto';
import { z } from 'zod';

import { Agent } from '../../agent';
import type { AgentExecutionOptionsBase } from '../../agent/agent.types';
import type { AgentSignalContents } from '../../agent/signals';
import type { AgentThreadSubscription, ToolsInput } from '../../agent/types';
import { ModelRouterLanguageModel } from '../../llm/model/router';
import { PrefillErrorHandler, ProviderHistoryCompat, StreamErrorRetryProcessor } from '../../processors';
import { RequestContext } from '../../request-context';
import {
  HarnessStorageAdmissionConflictError,
  HarnessStorageLeaseConflictError,
  HarnessStorageSessionEventReplayUnsupportedError,
  HarnessStorageSessionNotFoundError,
  HarnessStorageVersionConflictError,
} from '../../storage/domains/harness';
import type {
  GoalJudgeDecision,
  GoalState,
  AgentSignalResultEvidence,
  AgentSignalResultStatus,
  HarnessStorage,
  HarnessStorageAttachmentUnavailableError,
  HarnessRuntimeDependencyRefs,
  InboxResponseReceipt,
  QueueAdmissionReceipt,
  PendingResume,
  PermissionRules,
  PersistedAttachment,
  OperationAdmissionTombstone,
  PersistedRequestContextInput,
  QueuedItem,
  SaveAttachmentReferenceInput,
  SessionGrants,
  SessionRecord,
} from '../../storage/domains/harness';
import type { MastraModelOutput, FullOutput } from '../../stream/base/output';

import { ASK_USER_TOOL_ID, SUBMIT_PLAN_TOOL_ID } from '../../tools/builtin';
import type { Workspace } from '../../workspace';
import { convertStoredMessageToHarnessMessage } from '../_shared/message-conversion';
import type { StoredMessageRow } from '../_shared/message-conversion';
import type { HarnessMessage } from '../types';

import {
  HarnessAbortedError,
  type HarnessAbortReason,
  HarnessAdmissionConflictError,
  HarnessAttachmentUnavailableError,
  HarnessBusyError,
  HarnessConfigError,
  HarnessError,
  HarnessInboxItemNotFoundError,
  HarnessInboxResponseConflictError,
  HarnessOutputGenerationError,
  type HarnessOutputGenerationReason,
  HarnessOverrideConflictError,
  HarnessQueueFullDroppedError,
  HarnessQueueFullError,
  HarnessQueueItemExpiredError,
  HarnessSessionCancelledError,
  HarnessSessionClosedError,
  harnessSessionClosingError,
  HarnessSessionDeletedError,
  HarnessSessionLockedError,
  HarnessSessionNotFoundError,
  HarnessStateConflictError,
  HarnessStateSerializationError,
  HarnessSkillArgsValidationError,
  HarnessSkillNotFoundError,
  HarnessValidationError,
  HarnessWorkspaceLostError,
  redactPublicBoundaryRejection,
} from './errors';
// §5.1 stable-hash canonicalization (centralized in ./canonical-json). Admission hashing here
// always validates caller-reachable input, so the checked variant is bound to the local name.
import {
  assertJsonValue,
  jsonValuesEqual,
  sha256CanonicalJsonChecked as sha256CanonicalJson,
} from './canonical-json';
// §4.4c caller request-context validation + the durable-DTO mapping used by admission hashing
// and the tool-visible context.
import { callerRequestContextToPersisted, validateCallerRequestContext } from './request-context-input';
import { toHarnessDisplayStateSnapshotV1 } from './display-state';
import type { HarnessDisplayStateSnapshotV1 } from './display-state';
import {
  assertCustomEventType,
  assertJsonSerializable,
  EventEmitter,
  parseHarnessEventId,
  projectHarnessPublicError,
  projectToolEventPayloadForJson,
  snapshotHarnessEventForJson,
} from './events';
import type {
  EmitInput,
  HarnessEvent,
  HarnessEventListener,
  HarnessEventUnsubscribe,
  SubagentEndEvent,
  SubagentStartEvent,
  SubagentTextDeltaEvent,
  SubagentToolEndEvent,
  SubagentToolStartEvent,
} from './events';
import type { Harness } from './harness';
import { createSpawnSubagentTool, SPAWN_SUBAGENT_TOOL_ID } from './spawn-subagent-tool';
import type {
  AgentResult,
  AgentStream,
  AttachmentRef,
  GoalOptions,
  HarnessActionCatalogEntry,
  HarnessActionCatalogListOptions,
  HarnessActionCatalogSourceKind,
  HarnessActionCatalogUnavailableReason,
  HarnessMcpServerDescriptor,
  HarnessMcpToolDescriptor,
  HarnessMode,
  InboxResponseOptions,
  InboxResponseResult,
  HarnessCustomEventInput,
  HarnessRequestContext,
  HarnessSkill,
  HarnessSkillActionMetadata,
  UseSkillOptions,
  ListMessagesOptions,
  MessageAdmissionResult,
  MessageOptions,
  MessageOptionsDefault,
  MessageOptionsStream,
  MessageOptionsStructured,
  ModelAuthStatus,
  PermissionPolicy,
  QueueAdmissionResult,
  QueueOptions,
  RegisterPlanApprovalParams,
  RegisterQuestionParams,
  RegisterSandboxAccessParams,
  SessionInjectSystemReminderOptions,
  SessionInjectSystemReminderResult,
  SessionSignalOptions,
  SessionSignalResult,
  SetStateOptions,
  ToolCategory,
} from './types';

type MessageAdmissionIdentity = {
  signalId: string;
  runId: string;
};

type Deferred<T> = {
  promise: Promise<T>;
  resolve: (value: T) => void;
  reject: (reason: unknown) => void;
};

type MessageAdmissionStart = {
  admissionHash: string;
  modeId: string;
  modelId: string;
  promise: Promise<AgentSignalResultEvidence | OperationAdmissionTombstone>;
};

type MessageAdmissionHashes = {
  primary: string;
  legacyCompatible: readonly string[];
};

type QueueResumeRecoveryResult =
  | { status: 'none' }
  | { status: 'completed'; result: AgentResult }
  | { status: 'stale' };

type ResumeResponseMode = 'agent-result' | 'inbox-receipt';
type InboxReceiptResponseOptions = InboxResponseOptions & { responseId: string };
type LegacyInboxResponseOptions = Omit<InboxResponseOptions, 'responseId'> & { responseId?: undefined };
type ActionCatalogSkillDescriptor = Pick<
  HarnessSkill,
  'name' | 'description' | 'filePath' | 'category' | 'action' | 'metadata'
>;
type ActionCatalogMcpCacheEntry = {
  entries: HarnessActionCatalogEntry[];
  expiresAt?: number;
  successful?: boolean;
};
type ActionCatalogMcpTimedOutWork = {
  work: Promise<HarnessActionCatalogEntry[]>;
  retryAfter: number;
  retryCount: number;
};

/**
 * Tool IDs the harness translates from `tool-call-approval` /
 * `tool-call-suspended` events into `question` / `plan-approval` `kind`s.
 * Shared with the built-in `askUser` / `submitPlan` tools so the contract
 * lives in a single place (`packages/core/src/tools/builtin`).
 */
const ASK_USER_TOOL_NAME = ASK_USER_TOOL_ID;
const SUBMIT_PLAN_TOOL_NAME = SUBMIT_PLAN_TOOL_ID;
const MESSAGE_ADMISSION_DURABLE_WAIT_TIMEOUT_MS = 30_000;
const MESSAGE_ADMISSION_DURABLE_WAIT_INTERVAL_MS = 100;
const MESSAGE_RESULT_EVIDENCE_BACKGROUND_OBSERVE_TIMEOUT_MS = 5_000;
const QUEUE_ACCEPTED_RECOVERY_STALE_MS = 30_000;
const QUEUE_POST_RUN_FINALIZATION_RETRY_MS = 1_000;
const ACTION_CATALOG_DEFAULT_LIMIT = 100;
const ACTION_CATALOG_MAX_LIMIT = 500;
const ACTION_CATALOG_MCP_LIST_TIMEOUT_MS = 2_000;
// Cap explicit extensions to one day so a lost worker cannot pin a session indefinitely.
const MAX_LEASE_EXTENSION_MS = 24 * 60 * 60 * 1_000;
const ACTION_CATALOG_MCP_SUCCESS_CACHE_MS = 5_000;
const ACTION_CATALOG_MCP_FAILURE_CACHE_MS = 30_000;
const ACTION_CATALOG_MCP_MAX_TIMEOUT_RETRIES = 1;
const ACTION_CATALOG_SOURCE_KINDS: readonly HarnessActionCatalogSourceKind[] = ['skill', 'mcp-server', 'mcp-tool'];
const ACTION_CATALOG_SOURCE_ORDER: Record<HarnessActionCatalogSourceKind, number> = {
  skill: 0,
  'mcp-server': 1,
  'mcp-tool': 2,
};
const SUPPORTED_SKILL_ARG_SCHEMA_KEYS = new Set([
  'required',
  'properties',
  'type',
  'enum',
  'items',
  'additionalProperties',
]);
const RESERVED_MCP_SERVER_KEYS = new Set([...Object.getOwnPropertyNames(Object.prototype), '__proto__']);

function isPlainStateObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

/**
 * §10.2 state_changed sentinel for a whole-root change. `session.state` is
 * `unknown` — it may legitimately be a scalar, array, or plain object root (see
 * `firstNonJsonStatePath`, which explicitly allows scalar/array roots). When
 * the root itself is not a plain object on both sides, there are no top-level
 * keys to diff, so a genuine root change is reported under this sentinel key,
 * matching the `$`-for-root path convention used elsewhere in this module.
 */
const STATE_ROOT_SENTINEL = '$';

/**
 * §10.2 state_changed: the keys whose value changed between two durable
 * `session.state` snapshots.
 *
 * When BOTH snapshots are plain objects, the result is the set of top-level
 * keys that were added, removed, or mutated. Reference-equal values
 * short-circuit (the common `setState` spread keeps unchanged value refs
 * intact), so only genuinely-changed keys are reported; a canonical
 * (key-order-independent) JSON compare confirms changed references.
 *
 * When the root is NOT a plain object on both sides (e.g. a scalar->scalar,
 * array->array, or scalar<->object transition), there are no top-level keys to
 * diff. A genuine change is then reported under the `'$'` root sentinel so the
 * state_changed signal still fires; consumers read the full new root from the
 * event's `state` field. Equal roots (canonical JSON) report no change.
 */
function diffStateKeys(prev: unknown, next: unknown): string[] {
  if (!isPlainStateObject(prev) || !isPlainStateObject(next)) {
    if (prev === next) return [];
    if (jsonValuesEqual(prev as JsonValue | undefined, next as JsonValue | undefined)) return [];
    return [STATE_ROOT_SENTINEL];
  }
  const a = prev;
  const b = next;
  const changed: string[] = [];
  for (const key of new Set<string>([...Object.keys(a), ...Object.keys(b)])) {
    const av = a[key];
    const bv = b[key];
    if (av === bv) continue;
    if (jsonValuesEqual(av as JsonValue | undefined, bv as JsonValue | undefined)) continue;
    changed.push(key);
  }
  return changed;
}

/**
 * §5.1 state-shape validation. Returns the dotted path (`$` for the root) of the
 * FIRST value in a candidate `session.state` that cannot round-trip through
 * `JSON.stringify`/`JSON.parse` as plain JSON, or `undefined` when the whole
 * value is valid. Valid = `null`, string, boolean, FINITE number, plain array,
 * or PLAIN object whose values are all valid.
 *
 * The check is descriptor-STRICT — it rejects everything `JSON.stringify` would
 * silently DROP or TRANSFORM, not just obviously-bad scalar values:
 *   - scalar: `undefined`, `function`, `symbol`, `bigint`, `NaN`/`Infinity`
 *   - circular references
 *   - non-plain objects (`Date`/`Map`/`Set`/`RegExp`/class instances) — these
 *     either throw, serialize as `{}`, or transform via `toJSON`
 *   - symbol-keyed properties (dropped by JSON)
 *   - non-enumerable own properties (dropped by JSON)
 *   - accessor properties (getters/setters) — not data, and a getter would run
 *     arbitrary/throwing code; we reject WITHOUT invoking it
 *   - arrays with non-index own properties or sparse holes (dropped / become null)
 * Traversal is deterministic (`Reflect.ownKeys` order) so the reported path is
 * stable, and a data value is only READ after its descriptor is confirmed safe.
 */
function firstNonJsonStatePath(value: unknown, path: string, seen: Set<object>): string | undefined {
  if (value === null) return undefined;
  const t = typeof value;
  if (t === 'string' || t === 'boolean') return undefined;
  if (t === 'number') return Number.isFinite(value as number) ? undefined : path;
  if (t === 'undefined' || t === 'function' || t === 'symbol' || t === 'bigint') return path;
  if (t !== 'object') return path;
  const obj = value as object;
  if (seen.has(obj)) return path; // circular reference

  if (Array.isArray(obj)) {
    seen.add(obj);
    const len = obj.length;
    // Reject any own key that isn't a dense [0, len) index (extra props +
    // accessors are dropped/transformed by JSON; `length` is intrinsic).
    for (const key of Reflect.ownKeys(obj)) {
      if (key === 'length') continue;
      if (typeof key === 'symbol') return done(seen, obj, path);
      // Only CANONICAL dense indices are real array elements; `"01"`/`"1.0"`/
      // `""`/`" "` etc. coerce into range but are non-index props JSON drops.
      const idx = Number(key);
      if (!Number.isInteger(idx) || idx < 0 || idx >= len || String(idx) !== key) return done(seen, obj, path);
      const desc = Object.getOwnPropertyDescriptor(obj, key);
      if (desc === undefined || desc.get !== undefined || desc.set !== undefined) {
        return done(seen, obj, path === '$' ? String(idx) : `${path}.${idx}`);
      }
    }
    for (let i = 0; i < len; i++) {
      const childPath = path === '$' ? String(i) : `${path}.${i}`;
      if (!(i in obj)) return done(seen, obj, childPath); // sparse hole (JSON → null)
      const r = firstNonJsonStatePath((obj as unknown[])[i], childPath, seen);
      if (r !== undefined) return done(seen, obj, r);
    }
    seen.delete(obj);
    return undefined;
  }

  // Only plain objects (Object.prototype or null prototype) are valid JSON
  // containers; Date/Map/Set/class instances etc. are not round-trippable.
  const proto = Object.getPrototypeOf(obj);
  if (proto !== Object.prototype && proto !== null) return path;
  seen.add(obj);
  for (const key of Reflect.ownKeys(obj)) {
    if (typeof key === 'symbol') return done(seen, obj, path); // symbol-keyed prop (dropped)
    const childPath = path === '$' ? key : `${path}.${key}`;
    const desc = Object.getOwnPropertyDescriptor(obj, key);
    if (desc === undefined) return done(seen, obj, childPath);
    if (!desc.enumerable) return done(seen, obj, childPath); // dropped by JSON
    if (desc.get !== undefined || desc.set !== undefined) return done(seen, obj, childPath); // accessor — never invoked
    const r = firstNonJsonStatePath((obj as Record<string, unknown>)[key], childPath, seen);
    if (r !== undefined) return done(seen, obj, r);
  }
  seen.delete(obj);
  return undefined;
}

/** Pop the circular-tracking frame and return the offending path. */
function done(seen: Set<object>, obj: object, path: string): string {
  seen.delete(obj);
  return path;
}

function cloneMcpCatalogValue(value: unknown): unknown | undefined {
  if (value === undefined) return undefined;
  try {
    return structuredClone(value);
  } catch {
    try {
      return JSON.parse(JSON.stringify(value)) as unknown;
    } catch {
      return undefined;
    }
  }
}

function isPlainMcpCatalogObject(value: unknown): value is Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return false;
  if (Object.prototype.toString.call(value) !== '[object Object]') return false;
  const proto = Object.getPrototypeOf(value);
  return proto === Object.prototype || proto === null;
}

function cloneMcpCatalogRecord(value: unknown): Record<string, unknown> | undefined {
  const cloned = cloneMcpCatalogValue(value);
  return isPlainMcpCatalogObject(cloned) ? cloned : undefined;
}

function cloneMcpCatalogRecordArray(value: unknown): readonly Record<string, unknown>[] | undefined {
  const cloned = cloneMcpCatalogValue(value);
  if (!Array.isArray(cloned) || cloned.some(item => !isPlainMcpCatalogObject(item))) {
    return undefined;
  }
  return cloned as Record<string, unknown>[];
}

function cloneMcpSchemaLike(value: unknown): unknown | undefined {
  if (value && typeof value === 'object' && 'jsonSchema' in value) {
    return cloneMcpCatalogValue((value as { jsonSchema?: unknown }).jsonSchema);
  }
  return cloneMcpCatalogValue(value);
}

function encodeActionCatalogIdPart(value: string): string {
  return encodeURIComponent(value);
}

function compareActionCatalogIds(a: string, b: string): number {
  if (a < b) return -1;
  if (a > b) return 1;
  return 0;
}

function cloneActionCatalogEntry(entry: HarnessActionCatalogEntry): HarnessActionCatalogEntry {
  const cloned = cloneMcpCatalogValue(entry);
  if (cloned === undefined) {
    throw new HarnessValidationError(
      'actions.list()',
      `could not clone action catalog entry ${JSON.stringify(entry.id)}`,
    );
  }
  return cloned as HarnessActionCatalogEntry;
}

function cloneActionCatalogStringArray(value: unknown): readonly string[] | undefined {
  if (!Array.isArray(value) || value.some(item => typeof item !== 'string' || item.length === 0)) {
    return undefined;
  }
  return [...value];
}

function cloneActionCatalogShortcuts(value: unknown): HarnessSkillActionMetadata['shortcuts'] | undefined {
  if (!Array.isArray(value)) return undefined;
  const shortcuts = value.map(item => {
    if (!isPlainMcpCatalogObject(item) || typeof item.id !== 'string' || item.id.length === 0) return undefined;
    const keys = cloneActionCatalogStringArray(item.keys);
    if (item.keys !== undefined && !keys) return undefined;
    if (item.label !== undefined && typeof item.label !== 'string') return undefined;
    return {
      id: item.id,
      ...(item.label ? { label: item.label } : {}),
      ...(keys ? { keys } : {}),
    };
  });
  return shortcuts.some(shortcut => !shortcut) ? undefined : (shortcuts as HarnessSkillActionMetadata['shortcuts']);
}

function cloneActionCatalogPermissions(value: unknown): HarnessSkillActionMetadata['permissions'] | undefined {
  if (!isPlainMcpCatalogObject(value)) return undefined;
  const tools = cloneActionCatalogStringArray(value.tools);
  const fileScopes = cloneActionCatalogStringArray(value.fileScopes);
  const networkScopes = cloneActionCatalogStringArray(value.networkScopes);
  const mcpScopes = cloneActionCatalogStringArray(value.mcpScopes);
  if (
    (value.tools !== undefined && !tools) ||
    (value.fileScopes !== undefined && !fileScopes) ||
    (value.networkScopes !== undefined && !networkScopes) ||
    (value.mcpScopes !== undefined && !mcpScopes)
  ) {
    return undefined;
  }
  return {
    ...(tools ? { tools } : {}),
    ...(fileScopes ? { fileScopes } : {}),
    ...(networkScopes ? { networkScopes } : {}),
    ...(mcpScopes ? { mcpScopes } : {}),
  };
}

function cloneActionMetadataLike(value: unknown): HarnessSkillActionMetadata | undefined {
  if (!isPlainMcpCatalogObject(value)) return undefined;
  if (value.displayName !== undefined && typeof value.displayName !== 'string') return undefined;
  if (value.icon !== undefined && typeof value.icon !== 'string') return undefined;
  const shortcuts = cloneActionCatalogShortcuts(value.shortcuts);
  const inputSchema = cloneMcpCatalogRecord(value.inputSchema);
  const outputSchema = cloneMcpCatalogRecord(value.outputSchema);
  const artifactTypes = cloneActionCatalogStringArray(value.artifactTypes);
  const permissions = cloneActionCatalogPermissions(value.permissions);
  if (
    (value.shortcuts !== undefined && !shortcuts) ||
    (value.inputSchema !== undefined && !inputSchema) ||
    (value.outputSchema !== undefined && !outputSchema) ||
    (value.artifactTypes !== undefined && !artifactTypes) ||
    (value.permissions !== undefined && !permissions)
  ) {
    return undefined;
  }
  return {
    ...(value.displayName ? { displayName: value.displayName } : {}),
    ...(value.icon ? { icon: value.icon } : {}),
    ...(shortcuts ? { shortcuts } : {}),
    ...(inputSchema ? { inputSchema } : {}),
    ...(outputSchema ? { outputSchema } : {}),
    ...(artifactTypes ? { artifactTypes } : {}),
    ...(permissions ? { permissions } : {}),
  };
}

class ActionCatalogMcpListTimeoutError extends Error {
  constructor() {
    super(`MCP tool catalog did not resolve within ${ACTION_CATALOG_MCP_LIST_TIMEOUT_MS}ms`);
    this.name = 'ActionCatalogMcpListTimeoutError';
  }
}

function startActionCatalogMcpListWithTimeout<T>(
  run: (abortSignal: AbortSignal) => Promise<T>,
  onTimeout?: (work: Promise<T>) => void,
): {
  pending: Promise<T>;
  work: Promise<T>;
  didTimeout: () => boolean;
} {
  const controller = new AbortController();
  let timer: ReturnType<typeof setTimeout> | undefined;
  let timedOut = false;
  const work = run(controller.signal);
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => {
      const timeoutError = new ActionCatalogMcpListTimeoutError();
      timedOut = true;
      onTimeout?.(work);
      controller.abort(timeoutError);
      reject(timeoutError);
    }, ACTION_CATALOG_MCP_LIST_TIMEOUT_MS);
  });
  const pending = Promise.race([work, timeout]).finally(() => {
    if (timer !== undefined) clearTimeout(timer);
  });
  return {
    pending,
    work,
    didTimeout: () => timedOut,
  };
}

export type SessionLifecycleState = 'live' | 'closing' | 'closed' | 'deleted' | 'evicted';

interface QueueBackpressureDrop {
  queuedItemId: string;
  admissionId?: string;
  replacementQueuedItemId?: string;
  replacementAdmissionId?: string;
  maxQueueDepth: number;
  source: 'queue' | 'goal';
  goalId?: string;
}

/**
 * System prompt for the goal judge. Lifted verbatim from
 * mastracode/src/tui/goal-manager.ts so the harness-native judge produces
 * the same verdicts as the TUI implementation. The wording matters — the
 * "don't wait for yourself" rule and the asked-question-vs-checkpoint
 * distinction prevent the loop from flip-flopping.
 */
const JUDGE_SYSTEM_PROMPT = `You are the goal judge. Your decision directly controls whether the assistant continues working toward the goal.

Given a goal and the assistant's latest response, reason about whether the goal's requirements have been satisfied. Compare what the goal asks for against what the assistant has actually produced. Focus on substance, not phrasing.

Use "done" when the goal is fully achieved.
Use "waiting" only when the goal explicitly requires a user checkpoint, user feedback, human verification, human confirmation, or another external event outside the goal-judge loop before the assistant should continue, and the assistant has correctly stopped at that checkpoint. Do not use "waiting" merely because the assistant asked a question or could benefit from user input.
Use "continue" when the goal is not done and the assistant should keep working autonomously, including when it asked for input that the goal did not explicitly require.
If your previous decision was "waiting" for an explicit user checkpoint, keep choosing "waiting" when the user's latest response asks a question, requests clarification, or otherwise does not satisfy the checkpoint. Do not continue until the required user feedback/confirmation/verification has actually been provided.
If the goal says to wait for the goal judge, judge, evaluator, or you to respond, approve, verify, validate, tell the assistant to continue, or otherwise provide the next signal, treat your own decision as that judge response. Verification can be performed by you unless the goal explicitly says it needs human/user verification. Choose "continue" when the assistant should proceed to the next step. Do not choose "waiting" for judge-controlled checkpoints, because that would mean waiting for yourself.

Your "reason" field is sent back to the assistant as guidance when the goal is not yet done — be specific about what still needs to be accomplished. When choosing "continue", write the reason as an instruction for what the assistant should do next. When choosing "waiting", explain what specific user checkpoint is still outstanding.`;

/** Structured-output schema used by the goal judge call (§4.7). */
const GoalJudgeSchema = z.object({
  decision: z
    .enum(['done', 'continue', 'waiting'])
    .describe(
      'Whether the goal is done, should continue autonomously, or is at an explicit user checkpoint required by the goal',
    ),
  reason: z.string().describe('Brief explanation of what was accomplished or what remains to be done'),
});

/** Per-message cap on judge-context strings to keep judge latency bounded. */
const JUDGE_TRUNCATE_LIMIT = 4000;

// ---------------------------------------------------------------------------
// Permission helpers (§4.2e). Tiny shape validators kept module-scoped so the
// session methods stay focused on persistence + event emission.
// ---------------------------------------------------------------------------

const TOOL_CATEGORIES: readonly ToolCategory[] = ['read', 'edit', 'execute', 'mcp', 'other'];
const PERMISSION_POLICIES: readonly PermissionPolicy[] = ['allow', 'ask', 'deny'];

function assertToolCategory(method: string, value: unknown): asserts value is ToolCategory {
  if (typeof value !== 'string' || !TOOL_CATEGORIES.includes(value as ToolCategory)) {
    throw new HarnessValidationError(method, `unknown ToolCategory ${JSON.stringify(value)}`);
  }
}

function assertToolName(method: string, value: unknown): asserts value is string {
  if (typeof value !== 'string' || value.length === 0) {
    throw new HarnessValidationError(method, 'toolName must be a non-empty string');
  }
}

function assertAgentType(method: string, value: unknown): asserts value is string {
  if (typeof value !== 'string' || value.length === 0) {
    throw new HarnessValidationError(method, 'agentType must be a non-empty string');
  }
}

function assertModelId(method: string, value: unknown): asserts value is string {
  if (typeof value !== 'string' || value.length === 0) {
    throw new HarnessValidationError(method, 'model must be a non-empty string');
  }
}

function assertPolicy(method: string, value: unknown): asserts value is PermissionPolicy {
  if (typeof value !== 'string' || !PERMISSION_POLICIES.includes(value as PermissionPolicy)) {
    throw new HarnessValidationError(method, `policy must be one of ${PERMISSION_POLICIES.join(' | ')}`);
  }
}

function isStorageAttachmentUnavailableError(err: unknown): err is HarnessStorageAttachmentUnavailableError {
  return (
    typeof err === 'object' &&
    err !== null &&
    (err as { code?: unknown }).code === 'harness.storage.attachment_unavailable' &&
    typeof (err as { sessionId?: unknown }).sessionId === 'string' &&
    typeof (err as { attachmentId?: unknown }).attachmentId === 'string'
  );
}

function truncateForJudge(value: string): string {
  return value.length > JUDGE_TRUNCATE_LIMIT ? value.slice(0, JUDGE_TRUNCATE_LIMIT) + '\n...[truncated]' : value;
}

function escapeGoalXml(value: string): string {
  return value.replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;');
}

/**
 * Continuation prompts. The wording matters — these are lifted verbatim
 * from `mastracode/src/tui/goal-manager.ts` + `commands/goal.ts` so the
 * harness-native goal API produces byte-identical kickoff/resume/judge-
 * continue messages.
 */

/** Kickoff sent by `setGoal` (parity with TUI's `createGoalReminderXml`). */
function buildKickoffContinuation(objective: string): string {
  return `<system-reminder type="goal">${escapeGoalXml(objective)}</system-reminder>`;
}

/** Continuation sent by `resumeGoal` (parity with TUI's `/goal resume`). */
function buildResumeContinuation(objective: string): string {
  return `Continue working toward the goal: ${objective}`;
}

/**
 * Continuation sent after a judge `continue` verdict (parity with TUI's
 * `GoalManager.buildContinuationPrompt`).
 */
function buildJudgeContinuation(opts: { turn: number; max: number; objective: string; judgeReason: string }): string {
  const message = `[Goal attempt ${opts.turn}/${opts.max}] The goal is not yet complete. Judge feedback: ${opts.judgeReason}\n\nContinue working toward the goal: ${opts.objective}`;
  return `<system-reminder type="goal-judge">${escapeGoalXml(message)}</system-reminder>`;
}

/**
 * Active-tool tracking for `SessionDisplayState.activeTools`. One entry per
 * `tool_start` that has not yet been settled by a matching `tool_end`. Drops
 * out on `tool_end` regardless of `isError`.
 */
export interface ActiveToolState {
  toolCallId: string;
  toolName: string;
  args: unknown;
  startedAt: number;
  /** Set when this tool call came from a spawned subagent, not the parent. */
  subagentSessionId?: string;
}

/**
 * Active-subagent tracking for `SessionDisplayState.activeSubagents`. Keyed
 * on the parent's `spawn_subagent` tool call id. Dropped on subagent close.
 */
export interface ActiveSubagentState {
  subagentSessionId: string;
  agentType: string;
  task: string;
  parentToolCallId: string;
  startedAt: number;
}

/** Cumulative token usage for the session's thread. */
export interface TokenUsage {
  promptTokens: number;
  completionTokens: number;
  totalTokens: number;
}

/**
 * Internal: outstanding `waitForIdle()` waiter. `check()` re-evaluates
 * `isBusy()` and resolves the underlying Promise if idle (returns `true`
 * when the waiter is satisfied); `reject()` finalises with an error
 * (close/timeout); `cleanup()` disposes timers + removes the waiter from
 * `_idleWaiters`.
 */
interface IdleWaiter {
  check: () => boolean;
  reject: (err: unknown) => void;
  cleanup: () => void;
}

interface ActiveTurnWaiter {
  promise: Promise<never>;
  controller?: AbortController;
  reject: (err: unknown) => void;
  cleanup: () => void;
}

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
export type HarnessRunStatus =
  | 'starting'
  | 'running'
  | 'waiting'
  | 'resuming'
  | 'completed'
  | 'failed'
  | 'interrupted';

export type HarnessRunOperationRefKind = 'signal' | 'queue' | 'sync-generate' | 'use-skill' | 'inbox-response';

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

  // Tokens
  tokenUsage: TokenUsage;

  // Pending interrupt (full UX payload, not recovery-only metadata)
  pending: SessionDisplayPending | null;

  // Queue
  queueDepth: number;
  currentQueuedItemId?: string;

  // Goal
  goal?: SessionRecord['goal'];
}

export type SessionDisplayPending = Omit<NonNullable<SessionRecord['pendingResume']>, 'runtimeDependencies'>;

function pendingResumeForDisplay(pending: SessionRecord['pendingResume']): SessionDisplayPending | null {
  if (!pending) return null;
  const { runtimeDependencies: _runtimeDependencies, ...displayPending } = pending;
  return displayPending;
}

/**
 * Internal handle the Harness uses to construct + tear down a Session
 * without exposing those operations on the public API. Plain object so
 * tests can construct a Session in isolation if needed.
 */
export interface SessionInternals {
  harness: Harness;
  storage: HarnessStorage;
  ownerId: string;
  /** Initial record loaded under the lease. The Session takes ownership. */
  record: SessionRecord;
  /** Lease TTL the Harness acquired the lease for. */
  leaseExpiresAt: number;
  /** Durable event replay cursor seed from the previous live owner, if any. */
  eventReplaySeed?: { epoch: string; nextSequence: number };
  /**
   * §10.5: when false, transient streaming deltas (`text_delta`,
   * `subagent_text_delta`) are NOT persisted to the durable session-event log
   * (live subscribers still receive them). Defaults to true.
   */
  persistTransientStreamingEvents?: boolean;
}

export class Session {
  /** Stable identity. Frozen at construction. */
  readonly id: string;
  readonly resourceId: string;
  readonly threadId: string;
  readonly parentSessionId?: string;
  readonly subagentDepth: number;
  readonly createdAt: number;

  private _record: SessionRecord;
  private _state: SessionLifecycleState = 'live';
  private readonly _harness: Harness;
  private readonly _storage: HarnessStorage;
  private readonly _ownerId: string;
  private readonly _emitter: EventEmitter;

  /**
   * Queue resolvers indexed by `queuedItem.id`. Set in `queue()` so the
   * caller's promise settles when the head turn completes (or rejects on
   * permanent failure). Cleared after settle. Items recovered from
   * `pendingQueue` on hydration have no resolver — `queue_item_replayed` is
   * emitted instead and the turn runs purely for its side-effects.
   */
  private readonly _queueResolvers = new Map<
    string,
    { promise: Promise<AgentResult>; resolve: (result: AgentResult) => void; reject: (err: unknown) => void }
  >();
  /** `queuedItem.id` of the turn currently running (live or suspended). */
  private _currentQueuedItemId?: string;
  /** `queuedItem.source` of the turn currently running. Used by the goal
   *  judge loop to skip re-judging on goal-driven continuation turns. */
  private _currentQueuedItemSource?: 'user' | 'goal';
  /** Fresh remote queue admissions have no local promise resolver but are not crash replays. */
  private readonly _liveAdmittedQueuedItemIds = new Set<string>();
  /** True while `_maybeDrainQueue` is running so re-entrant kicks are no-ops. */
  private _draining = false;
  private _queueWakeTimer?: ReturnType<typeof setTimeout>;
  private _queueWakeAt?: number;
  private _queuedResumeRecoveryTimer?: ReturnType<typeof setTimeout>;
  /**
   * Tracks the AbortController for the currently-running turn (message or
   * queued). Set when a turn begins, cleared on terminal completion or
   * suspension. `session.abort()` calls `abort()` on this controller. Also
   * powers `session.isRunning()` — non-undefined means a turn is in-flight.
   */
  private _currentTurnAbortController?: AbortController;
  /**
   * Transient per-turn tracking surfaced via `getDisplayState()`. Reset at
   * the start of every turn (in `_beginTurn` via `_resetTurnTracking`) and
   * mutated from `_drainStreamToEvents`, `_maybeCaptureSuspend`, and the
   * `_resume` path. Not persisted — these are run-only fields.
   */
  private _currentRunId?: string;
  /** §5.1b run-start time for the SessionRunProjection; set at agent_start, cleared on turn reset. */
  private _currentRunStartedAt?: number;
  /**
   * §5.1b effective per-run mode/model for the SessionRunProjection. Captured in
   * `message()` (the single turn runner for live signal/stream/sync/queue-drain/
   * useSkill paths) so the projection reports the run's EFFECTIVE identity (a
   * per-turn `mode`/`model` override), not just the session default. Read only
   * while `_currentRunId` is set; cleared on turn reset.
   */
  private _currentRunModeId?: string;
  private _currentRunModelId?: string;
  /**
   * §5.1b: the originating signalId of the live run, when it is signal-backed.
   * Lets the live SessionRunProjection report `operation: { kind: 'signal',
   * signalId }` (symmetric with the pending-resume branch) so a UI can link the
   * running turn to its durable signal row. Undefined for queue-backed runs (the
   * queuedItemId discriminates those) and for paths with no originating signal.
   */
  private _currentRunSignalId?: string;
  private _currentMessageId?: string;
  private _currentTraceId?: string;
  private readonly _activeTurnWaiters = new Set<ActiveTurnWaiter>();
  private readonly _turnAbortSignalCleanups = new WeakMap<AbortController, () => void>();
  private readonly _operationEvidenceSignalIds = new Set<string>();
  private readonly _activeTools = new Map<string, ActiveToolState>();
  private readonly _toolInputBuffers = new Map<string, { toolName: string; text: string }>();
  private readonly _activeSubagents = new Map<string, ActiveSubagentState>();
  /**
   * Cumulative usage for the session's thread. Live counter, single source of
   * truth in-process. Seeded from `internals.record.tokenUsage` in the
   * constructor so reopens carry the persisted aggregate; flushed back into
   * `_record.tokenUsage` on every `_flushUpdate` so the next reopen sees the
   * latest value.
   */
  private _tokenUsage: TokenUsage = { promptTokens: 0, completionTokens: 0, totalTokens: 0 };
  private _leaseExtensionDeadline?: number;
  private _leaseRenewalChain: Promise<void> = Promise.resolve();
  /**
   * Outstanding `waitForIdle()` callers. On close/evict each waiter is
   * rejected so callers don't hang on a dead session.
   */
  private readonly _idleWaiters = new Set<IdleWaiter>();
  /**
   * In-process serialization for `_flushUpdate`. Concurrent setters chain
   * onto this so each CAS write reads the latest in-memory version. Without
   * this, two parallel callers both observe `version=N`, both attempt
   * `ifVersion: N`, and the loser hits a `HarnessStorageVersionConflictError`.
   */
  private _flushChain: Promise<void> = Promise.resolve();

  /** Cached workspace handle. Resolves lazily on first `getWorkspace()` call. */
  private _workspace?: Workspace;
  /** Dedup promise so concurrent `getWorkspace()` calls share one provision attempt. */
  private _workspaceResolving?: Promise<Workspace>;
  /**
   * True when a non-resumable per-session workspace was found in storage on
   * rehydrate. Set by `_publish` via {@link _markWorkspaceLost}. The first
   * `getWorkspace()` call throws {@link HarnessWorkspaceLostError}; callers
   * can drop the marker and reprovision by calling `clearWorkspaceLost()`.
   */
  private _workspaceLost = false;

  // -------------------------------------------------------------------------
  // Skill discovery cache (§4.6).
  //
  // Workspace skill discovery is async and lazy. We cache the merged code +
  // workspace catalog for the lifetime of this in-memory Session instance.
  // Concurrent `skills.list` / `skills.get` calls during a generation build
  // share the same in-flight promise (single-flight), avoiding duplicate
  // discovery work. `skills.refresh()` clears the cache so the next read
  // re-runs discovery through the workspace skill source.
  // -------------------------------------------------------------------------
  private _skillsCache?: HarnessSkill[];
  private _skillsResolving?: Promise<HarnessSkill[]>;
  private _actionsSkillEntriesCache?: HarnessActionCatalogEntry[];
  private _actionsSkillEntriesResolving?: Promise<HarnessActionCatalogEntry[]>;
  private _actionsMcpEntriesCacheByServer = new Map<string, ActionCatalogMcpCacheEntry>();
  private _actionsMcpEntriesResolvingByServer = new Map<string, Promise<HarnessActionCatalogEntry[]>>();
  private _actionsMcpTimedOutWorkByServer = new Map<string, ActionCatalogMcpTimedOutWork>();
  private _actionsMcpCatalogGeneration = 0;

  // -------------------------------------------------------------------------
  // Thread subscription — §4.2 signal routing.
  //
  // One AgentThreadSubscription per Session, lazy-acquired on the first
  // signal-routed `message()` call. The subscription multiplexes every run
  // on the (resource, thread) tuple — idle-start wakes, mid-flight signal
  // deliveries, resume runs, queue drains — so a single drain loop owns
  // chunk → harness event translation for the whole session lifetime.
  //
  // Subscription lifetime ends with `close()` (explicit) or session
  // eviction (the new Session that rehydrates will lazy-open its own).
  // Cross-agent mode switches re-open against the new agent — see
  // `_ensureThreadSubscription` for the teardown contract.
  // -------------------------------------------------------------------------

  /** Cached thread subscription. Lazy. One per Session at a time. */
  private _threadSubscription?: AgentThreadSubscription<unknown>;
  /** Agent the current subscription was opened against. Used to detect
   *  cross-agent mode switches that require re-opening. */
  private _threadSubscriptionAgent?: Agent;
  /** Handle to the running drain loop, awaited by `close()`. */
  private _threadSubscriptionDrain?: Promise<void>;
  /** True once the subscription has been torn down (by close or eviction).
   *  Guards re-opens and re-entrant teardown. */
  private _threadSubscriptionClosed = false;
  /**
   * Per-run completion promises, keyed by `runId`. `_watchRunCompletion()`
   * resolves or rejects the matching entry after the runtime output finishes.
   * Entries left over on `close()` are rejected so callers don't hang.
   */
  private readonly _runCompletionPromises = new Map<
    string,
    {
      promise: Promise<FullOutput<unknown>>;
      resolve: (full: FullOutput<unknown>) => void;
      reject: (err: unknown) => void;
    }
  >();
  /**
   * Cache of run completion results that landed before any caller had a chance
   * to register a waiter. `sendSignal()` returns synchronously and the runtime
   * can drive the entire run to completion in the same microtask tick, so by
   * the time `_awaitRunCompletion(runId)` runs the terminal chunk may already
   * have been processed. Entries are retained so duplicate admission waiters
   * that converge on the same run can all observe the terminal result.
   */
  private readonly _completedRuns = new Map<
    string,
    { ok: true; full: FullOutput<unknown> } | { ok: false; err: unknown }
  >();
  /**
   * Message admission retries can observe `_completedRuns` before the original
   * message continuation has accounted usage. Track run ids accounted through
   * either path so the retry can persist before writing evidence without the
   * original continuation double-counting later.
   */
  private readonly _messageTokenAccountedRunIds = new Set<string>();
  private readonly _messageTokenAccountingRunIds = new Set<string>();
  /** §10.5: when false, transient streaming deltas are not persisted (live-only). */
  private readonly _persistTransientStreamingEvents: boolean;
  private readonly _messageTokenAccountingReservations = new Map<string, Deferred<void>>();
  private readonly _messageAdmissionStarts = new Map<string, MessageAdmissionStart>();
  private _eventPersistenceTail: Promise<void> = Promise.resolve();
  private _eventPersistenceError: unknown;
  private readonly _backgroundTurnCompletions = new Set<Promise<unknown>>();

  /** @internal — constructed by the Harness, not directly. */
  constructor(internals: SessionInternals) {
    this.id = internals.record.id;
    this.resourceId = internals.record.resourceId;
    this.threadId = internals.record.threadId;
    this.parentSessionId = internals.record.parentSessionId;
    this.subagentDepth = internals.record.subagentDepth ?? 0;
    this.createdAt = internals.record.createdAt;

    this._record = internals.record;
    // Seed the live token-usage counter from the persisted aggregate so reopens
    // (after eviction / process restart) continue accumulating instead of
    // restarting at zero. Accept only non-negative integer fields because rows
    // written before tokenUsage durability shipped may carry partially-populated
    // or malformed objects; a bad component would otherwise poison the
    // aggregate.
    {
      const persisted = internals.record.tokenUsage as Partial<TokenUsage> | undefined;
      const promptTokens =
        typeof persisted?.promptTokens === 'number' &&
        Number.isInteger(persisted.promptTokens) &&
        persisted.promptTokens >= 0
          ? persisted.promptTokens
          : 0;
      const completionTokens =
        typeof persisted?.completionTokens === 'number' &&
        Number.isInteger(persisted.completionTokens) &&
        persisted.completionTokens >= 0
          ? persisted.completionTokens
          : 0;
      const derivedTotalTokens = promptTokens + completionTokens;
      const totalTokens =
        typeof persisted?.totalTokens === 'number' &&
        Number.isInteger(persisted.totalTokens) &&
        persisted.totalTokens >= 0
          ? persisted.totalTokens < derivedTotalTokens
            ? derivedTotalTokens
            : persisted.totalTokens
          : derivedTotalTokens;
      this._tokenUsage = {
        promptTokens,
        completionTokens,
        totalTokens,
      };
    }
    if (this._record.closedAt !== undefined) {
      this._state = 'closed';
    } else if (this._record.closingAt !== undefined) {
      this._state = 'closing';
    }
    this._harness = internals.harness;
    this._storage = internals.storage;
    this._ownerId = internals.ownerId;
    this._persistTransientStreamingEvents = internals.persistTransientStreamingEvents ?? true;
    this._emitter = new EventEmitter(
      { sessionId: this.id },
      {
        onEvent: event => this._enqueueSessionEventPersistence(event),
        epoch: internals.eventReplaySeed?.epoch,
        nextSequence: internals.eventReplaySeed?.nextSequence,
      },
    );
  }

  // -------------------------------------------------------------------------
  // Events — §10.
  // -------------------------------------------------------------------------

  /**
   * Subscribe to events emitted on this session. Returns an unsubscribe
   * function. Listeners see only events emitted after `subscribe()` returns;
   * there is no automatic backfill (use `listMessages()` for history).
   *
   * Listener exceptions and rejected promises are isolated — they will not
   * disrupt the producer or other listeners.
   */
  subscribe(listener: HarnessEventListener): HarnessEventUnsubscribe {
    this._assertLive('subscribe()');
    return this._emitter.subscribe(listener);
  }

  async lookupMessageResult(signalId: string): Promise<AgentSignalResultStatus | OperationAdmissionTombstone | null> {
    this._assertLive('lookupMessageResult()');
    if (signalId.length === 0) {
      throw new HarnessValidationError('lookupMessageResult().signalId', 'signalId must be a non-empty string');
    }
    const record = this.getRecord();
    return this._storage.loadMessageResultEvidence({
      harnessName: record.harnessName,
      sessionId: record.id,
      resourceId: record.resourceId,
      threadId: record.threadId,
      signalId,
    });
  }

  async lookupQueueResult(queuedItemId: string): Promise<QueueAdmissionReceipt | OperationAdmissionTombstone | null> {
    this._assertLive('lookupQueueResult()');
    if (queuedItemId.length === 0) {
      throw new HarnessValidationError('lookupQueueResult().queuedItemId', 'queuedItemId must be a non-empty string');
    }
    const record = this.getRecord();
    return this._storage.loadQueueResultEvidence({
      harnessName: record.harnessName,
      sessionId: record.id,
      resourceId: record.resourceId,
      queuedItemId,
    });
  }

  async getEventReplayState() {
    this._assertLive('getEventReplayState()');
    await this._flushEventPersistence();
    const record = this.getRecord();
    return this._storage.getSessionEventReplayState({
      harnessName: record.harnessName,
      sessionId: record.id,
      resourceId: record.resourceId,
      threadId: record.threadId,
    });
  }

  async listEventsAfter(opts: { epoch: string; afterSequence: number; limit: number }) {
    this._assertLive('listEventsAfter()');
    if (opts.epoch.length === 0) {
      throw new HarnessValidationError('listEventsAfter().epoch', 'epoch must be a non-empty string');
    }
    if (!Number.isSafeInteger(opts.afterSequence) || opts.afterSequence < 0) {
      throw new HarnessValidationError(
        'listEventsAfter().afterSequence',
        'afterSequence must be a non-negative safe integer',
      );
    }
    if (!Number.isSafeInteger(opts.limit) || opts.limit < 1) {
      throw new HarnessValidationError('listEventsAfter().limit', 'limit must be a positive safe integer');
    }
    await this._flushEventPersistence();
    const record = this.getRecord();
    return this._storage.listSessionEvents({
      harnessName: record.harnessName,
      sessionId: record.id,
      resourceId: record.resourceId,
      threadId: record.threadId,
      epoch: opts.epoch,
      afterSequence: opts.afterSequence,
      limit: opts.limit,
    });
  }

  /** @internal — used by the Harness to publish events on this session's emitter. */
  _emit(event: EmitInput): HarnessEvent {
    return this._emitter.emit(event);
  }

  /**
   * §6.1/§6.3/§10.2: tool-authored custom event emission (`ctx.emitCustomEvent`).
   * Validates the type (dotted custom prefix, not a reserved built-in family) and
   * the payload (JSON-serializable) at call time — both throw `HarnessValidationError`
   * / `HarnessEventSerializationError` before anything is dispatched. The harness
   * stamps id/timestamp/sessionId via `_emit` and attributes the event to the live
   * run when one is active.
   */
  _emitCustomEvent(input: HarnessCustomEventInput): void {
    this._assertOpenForTurn('ctx.emitCustomEvent');
    if (input === null || typeof input !== 'object') {
      throw new HarnessValidationError('ctx.emitCustomEvent', 'expects an object with `type` and optional `payload`');
    }
    if (typeof input.type !== 'string') {
      throw new HarnessValidationError('ctx.emitCustomEvent.type', 'must be a string');
    }
    // §10.3: the input carries only `type` + optional `payload`; the harness owns
    // every envelope field (id/timestamp/sessionId/runId). Reject extra top-level
    // keys so a tool cannot smuggle reserved envelope fields.
    for (const key of Object.keys(input)) {
      if (key !== 'type' && key !== 'payload') {
        throw new HarnessValidationError('ctx.emitCustomEvent', `unexpected field "${key}" — only "type" and "payload" are allowed`);
      }
    }
    assertCustomEventType(input.type);
    // §6.1: tool-context custom events may only be emitted while a turn is in
    // flight. The gate is the active-turn marker, NOT `_currentRunId`: the
    // structured sync `message({ sync, output })` path runs `agent.generate(...)`
    // atomically and only learns its runId when generate returns, yet tools
    // execute mid-generate — so requiring a known run id would wrongly reject a
    // legitimate in-run emit. A cleared turn controller means a stale/post-turn or
    // evicted context (both clear it via `_endTurn`).
    if (this._currentTurnAbortController === undefined) {
      throw new HarnessValidationError('ctx.emitCustomEvent', 'no active turn — custom events may only be emitted during a run');
    }
    // Payload is optional; only validate JSON-serializability when one is present
    // (the walker treats a bare `undefined` as non-serializable).
    if (input.payload !== undefined) {
      assertJsonSerializable(input.type, this.id, input.payload);
    }
    this._emit({
      type: input.type as `${string}.${string}`,
      // §10.3: the harness fills the session identity fields. `sessionId` is
      // stamped by the emitter; `resourceId` / `threadId` are stamped here so a
      // custom event routes by the same (resourceId, threadId) tuple as built-in
      // session-scoped events.
      resourceId: this.resourceId,
      threadId: this.threadId,
      ...(input.payload !== undefined ? { payload: input.payload } : {}),
      // Attribute to the run when known; the structured-sync window before
      // agent_start has no runId yet, which is acceptable (sessionId still anchors it).
      ...(this._currentRunId !== undefined ? { runId: this._currentRunId } : {}),
    });
  }

  /** @internal — waits for prior event ledger writes before replay decisions. */
  async _flushEventPersistence(): Promise<void> {
    await this._eventPersistenceTail;
    if (this._eventPersistenceError !== undefined) {
      throw this._eventPersistenceError;
    }
  }

  private _enqueueSessionEventPersistence(event: HarnessEvent): void {
    if (this._eventPersistenceError !== undefined) return;
    // §10.5: high-volume transient streaming deltas are live-subscriber-only when
    // persistence is disabled — skip the durable `appendSessionEvent` (live dispatch
    // already delivered them). The emitter seq still advanced, so a reconnect whose
    // `Last-Event-ID` precedes a skipped delta gets a 412 `unreplayable_gap` and
    // recovers via the §10.5 snapshot/message path. Removes per-token write amplification.
    if (
      !this._persistTransientStreamingEvents &&
      (event.type === 'text_delta' || event.type === 'subagent_text_delta')
    ) {
      return;
    }
    let parsed: ReturnType<typeof parseHarnessEventId>;
    let storedEvent: JsonValue;
    try {
      parsed = parseHarnessEventId(event.id);
      storedEvent = snapshotHarnessEventForJson(event);
    } catch (err) {
      this._eventPersistenceError = err;
      console.error('[harness/v1] session event serialization failed:', err);
      return;
    }
    const record = this._record;
    const task = this._eventPersistenceTail
      .catch(() => undefined)
      .then(async () => {
        if (this._eventPersistenceError !== undefined) return;
        await this._storage.appendSessionEvent({
          harnessName: record.harnessName,
          sessionId: record.id,
          resourceId: record.resourceId,
          threadId: record.threadId,
          eventId: event.id,
          epoch: parsed.epoch,
          sequence: parsed.sequence,
          event: storedEvent,
          emittedAt: event.timestamp,
          storedAt: Date.now(),
        });
      });
    this._eventPersistenceTail = task.catch(err => {
      if (err instanceof HarnessStorageSessionEventReplayUnsupportedError) return;
      this._eventPersistenceError = err;
      console.error('[harness/v1] session event persistence failed:', err);
    });
  }

  /**
   * Emit an event that belongs to a turn (agent_*, message_*, tool_*,
   * suspension_*). Auto-stamps `queuedItemId` from `_currentQueuedItemId`
   * when a queued turn is running so subscribers can correlate every event
   * back to its `queue()` item.
   */
  private _emitTurnEvent(event: EmitInput): HarnessEvent {
    if (this._currentQueuedItemId !== undefined && (event as { queuedItemId?: string }).queuedItemId === undefined) {
      return this._emitter.emit({ ...event, queuedItemId: this._currentQueuedItemId } as EmitInput);
    }
    return this._emitter.emit(event);
  }

  /**
   * @internal — publish a `subagent_*` event on this session's emitter.
   * Called by the spawn-subagent bridge when forwarding a child session's
   * own `agent_start` / `text_delta` / `tool_start` / `tool_end` /
   * `agent_end` into the parent's subscriber stream as the corresponding
   * `subagent_*` event (§10.6).
   *
   * Auto-stamps `parentId` (this session's id) and `queuedItemId` from
   * `_currentQueuedItemId` so a subscriber can correlate every nested
   * event back to the parent's `queue()` item that spawned it. Callers
   * supply `depth` (child's depth in the subagent tree) and the rest of
   * the event payload.
   */
  _emitSubagentEvent(
    event:
      | Omit<SubagentStartEvent, 'id' | 'timestamp' | 'sessionId' | 'parentId'>
      | Omit<SubagentTextDeltaEvent, 'id' | 'timestamp' | 'sessionId' | 'parentId'>
      | Omit<SubagentToolStartEvent, 'id' | 'timestamp' | 'sessionId' | 'parentId'>
      | Omit<SubagentToolEndEvent, 'id' | 'timestamp' | 'sessionId' | 'parentId'>
      | Omit<SubagentEndEvent, 'id' | 'timestamp' | 'sessionId' | 'parentId'>,
  ): HarnessEvent {
    const stamped = { ...event, parentId: this.id } as EmitInput;
    if (this._currentQueuedItemId !== undefined && (stamped as { queuedItemId?: string }).queuedItemId === undefined) {
      return this._emitter.emit({ ...stamped, queuedItemId: this._currentQueuedItemId } as EmitInput);
    }
    return this._emitter.emit(stamped);
  }

  /** @internal — number of registered listeners (for tests). */
  get _internalListenerCount(): number {
    return this._emitter.listenerCount;
  }

  /**
   * Mark a turn as in-flight and mint the AbortController the agent run will
   * use. `session.abort()` aborts this controller. If the caller supplied
   * their own `AbortSignal`, we forward it into the session controller so a
   * single signal reaches the agent.
   *
   * Returns the controller so the calling path can hand `controller.signal`
   * to `agent.stream` / `agent.generate` / `agent.resumeStream`.
   */
  private _beginTurn(
    callerSignal: AbortSignal | undefined,
    runIdentity?: { modeId?: string; modelId?: string },
  ): AbortController {
    const controller = new AbortController();
    this._currentTurnAbortController = controller;
    this._resetTurnTracking();
    // §5.1b: stamp the run's EFFECTIVE identity AFTER the reset above, so the
    // SessionRunProjection reports a per-turn `mode`/`model` override (or a queued
    // item's own mode/model) for the live run rather than the session default.
    this._currentRunModeId = runIdentity?.modeId;
    this._currentRunModelId = runIdentity?.modelId;
    controller.signal.addEventListener('abort', () => this._scheduleActiveTurnAbort(controller), { once: true });
    if (callerSignal) {
      if (callerSignal.aborted) {
        controller.abort(this._forwardCallerAbortReason(callerSignal));
      } else {
        const forwardAbort = () => controller.abort(this._forwardCallerAbortReason(callerSignal));
        callerSignal.addEventListener('abort', forwardAbort, { once: true });
        this._turnAbortSignalCleanups.set(controller, () => callerSignal.removeEventListener('abort', forwardAbort));
      }
    }
    return controller;
  }

  /**
   * Clear per-turn transient display-state fields. Cumulative aggregates
   * (`_tokenUsage`) intentionally persist across turns within a session.
   */
  private _resetTurnTracking(): void {
    this._currentRunId = undefined;
    this._currentRunStartedAt = undefined;
    this._currentRunModeId = undefined;
    this._currentRunModelId = undefined;
    this._currentRunSignalId = undefined;
    this._currentMessageId = undefined;
    this._currentTraceId = undefined;
    this._activeTools.clear();
    this._toolInputBuffers.clear();
    // `_activeSubagents` is keyed by parent tool call id and naturally drops
    // entries on subagent close; do not clear here so a long-running subagent
    // spanning multiple parent turns still renders.
  }

  /**
   * Clear the in-flight turn marker so `isRunning()` reports false and the
   * next `session.abort()` is a no-op. Run-only display fields (`currentRunId`,
   * active-tool map, input buffers) clear too so an idle session reports
   * idle state. Cumulative aggregates (`_tokenUsage`) are preserved.
   */
  private _endTurn(controller: AbortController): void {
    this._turnAbortSignalCleanups.get(controller)?.();
    this._turnAbortSignalCleanups.delete(controller);
    if (this._currentTurnAbortController === controller) {
      this._currentTurnAbortController = undefined;
      this._currentRunId = undefined;
      this._currentRunStartedAt = undefined;
      this._currentRunModeId = undefined;
      this._currentRunModelId = undefined;
      this._currentRunSignalId = undefined;
      this._currentMessageId = undefined;
      this._currentTraceId = undefined;
      this._activeTools.clear();
      this._toolInputBuffers.clear();
    }
    this._notifyMaybeIdle();
  }

  private _createActiveTurnWaiter(): ActiveTurnWaiter {
    let reject!: (err: unknown) => void;
    const activeTurn = this._currentTurnAbortController;
    const waiter: ActiveTurnWaiter = {
      promise: new Promise<never>((_, rej) => {
        reject = rej;
      }),
      controller: activeTurn,
      reject: err => reject(err),
      cleanup: () => {
        this._activeTurnWaiters.delete(waiter);
      },
    };
    this._activeTurnWaiters.add(waiter);
    if (this._state === 'deleted') {
      this._activeTurnWaiters.delete(waiter);
      waiter.reject(new HarnessSessionDeletedError(this.id, this._record.resourceId, this._record.threadId));
    }
    if (activeTurn?.signal.aborted) {
      this._activeTurnWaiters.delete(waiter);
      waiter.reject(this._activeTurnAbortError(activeTurn));
    }
    return waiter;
  }

  private _rejectActiveTurnWaiters(reason: unknown, waiters?: ActiveTurnWaiter[]): void {
    const selectedWaiters = waiters ?? Array.from(this._activeTurnWaiters);
    if (selectedWaiters.length === 0) return;
    if (waiters) {
      for (const waiter of selectedWaiters) {
        this._activeTurnWaiters.delete(waiter);
      }
    } else {
      this._activeTurnWaiters.clear();
    }
    for (const waiter of selectedWaiters) {
      waiter.reject(reason);
    }
  }

  private _activeTurnAbortError(controller: AbortController): Error {
    const reason = (controller.signal as { reason?: unknown }).reason;
    const message = typeof reason === 'string' && reason.length > 0 ? reason : 'session_aborted';
    return reason instanceof Error ? reason : new HarnessSessionCancelledError(this.id, message);
  }

  private _scheduleActiveTurnAbort(controller: AbortController): void {
    const err = this._activeTurnAbortError(controller);
    const waiters = Array.from(this._activeTurnWaiters).filter(waiter => waiter.controller === controller);
    setTimeout(() => {
      if (!controller.signal.aborted) return;
      this._rejectActiveTurnWaiters(err, waiters);
    }, 0);
  }

  private _agentEndReasonForFullOutput(full: FullOutput<unknown>): 'complete' | 'aborted' | 'error' | 'suspended' {
    if (full.finishReason === 'suspended') return 'suspended';
    if (full.finishReason === 'aborted') return 'aborted';
    if (full.finishReason === 'error') return 'error';
    return 'complete';
  }

  /**
   * §4.5 `HarnessOutputGenerationError`: classify a non-suspended structured
   * sync-generate result that cannot yield a successful public typed value.
   * Returns `undefined` when the result carries a valid object.
   */
  private _classifyStructuredOutputFailure(full: FullOutput<unknown>): HarnessOutputGenerationReason | undefined {
    // An output processor rejected the response (tripwire) — object is absent by
    // construction, so check this before the missing-object case.
    if (full.tripwire !== undefined) return 'tripwire';
    // Schema-bearing sync form but the model produced no structured object.
    if (full.object === undefined) return 'structured_output_missing_object';
    return undefined;
  }

  /**
   * §4.5: normalize an error thrown out of the structured sync-generate path.
   * Aborts and harness-domain errors propagate with their own type; an already-
   * typed generation error passes through; anything else is an opaque agent/
   * runtime failure wrapped as `model_error`. Harness errors are detected by the
   * `Harness…Error` name they all carry — not every one exposes a `harness.*`
   * code (e.g. `HarnessSessionDeletedError`/`HarnessSessionLockedError`, which
   * the active-turn waiter rejects with), so the code alone is not sufficient.
   */
  private _asStructuredOutputError(err: unknown, runId: string | undefined, abortSignal: AbortSignal): unknown {
    if (err instanceof HarnessOutputGenerationError) return err;
    if (abortSignal.aborted) return err;
    if (err instanceof Error && err.name.startsWith('Harness')) return err;
    // §4.5: the agent layer surfaces structured-output schema validation as a
    // MastraError carrying this stable id (stream/base/output-format-handlers.ts);
    // distinguish it from an opaque model failure so callers see the precise reason.
    const reason: HarnessOutputGenerationReason =
      (err as { id?: unknown } | null | undefined)?.id === 'STRUCTURED_OUTPUT_SCHEMA_VALIDATION_FAILED'
        ? 'structured_output_validation_failed'
        : 'model_error';
    return new HarnessOutputGenerationError(this.id, reason, runId, { cause: err });
  }

  /** §10.2 `agent_end.usage`: the run's token usage (zero when unavailable, e.g. an error path with no terminal output). */
  private _runUsage(full?: FullOutput<unknown>): TokenUsage {
    const delta = full ? this._tokenUsageDeltaFromFullOutput(full) : undefined;
    return delta ?? { promptTokens: 0, completionTokens: 0, totalTokens: 0 };
  }

  /**
   * §10.2 agent_start/agent_end require a `runId`. The run id is coalesced from
   * the explicit dispatch/full id and the captured `_currentRunId`; when no run
   * id exists at all (a turn that failed before a run was reserved), no
   * lifecycle event is emitted — there was no run to start or end.
   */
  private _emitAgentStart(runId: string | undefined, signalId?: string): void {
    const id = runId ?? this._currentRunId;
    if (id === undefined) return;
    // Stamp the active run id at agent_start so subsequent streamed chunks —
    // which (for the long-lived thread subscription) do NOT carry their own
    // runId — can be attributed to this run by `_emitForChunk` (§10.2
    // text_delta/tool_start/tool_end all require a runId).
    if (this._currentRunId === undefined) {
      this._currentRunId = id;
      // §5.1b: stamp the run-start time for the SessionRunProjection. agent_start
      // is the canonical run-start choke for all entry paths.
      this._currentRunStartedAt = Date.now();
      // §5.1b: a signal-backed run records its originating signalId so the live
      // projection's `operation` can link to the durable signal row.
      this._currentRunSignalId = signalId;
    }
    this._emitTurnEvent({ type: 'agent_start', runId: id });
  }

  private _emitAgentEnd(opts: {
    runId: string | undefined;
    finishReason: 'complete' | 'aborted' | 'error' | 'suspended';
    full?: FullOutput<unknown>;
  }): void {
    const id = opts.runId ?? this._currentRunId;
    if (id === undefined) return;
    this._emitTurnEvent({ type: 'agent_end', runId: id, finishReason: opts.finishReason, usage: this._runUsage(opts.full) });
  }

  /**
   * §10.2 suspension event: project a captured `PendingResume` into the matching
   * pending shape (tool_approval_required / tool_suspension_required /
   * question_pending / plan_approval_required). Sandbox/path-access prompts
   * project to `question_pending` (§10.2 defines no dedicated sandbox event).
   * Emitted after the durable-parking barrier so subscribers can reconstruct
   * the pending state from storage.
   */
  private _emitPendingEvent(pending: PendingResume): void {
    const sourceFields =
      pending.source === 'subagent'
        ? {
            source: 'subagent' as const,
            subagentToolCallId: pending.subagentToolCallId ?? '',
            subagentSessionId: this.id,
          }
        : { source: 'parent' as const };
    const base = {
      runId: pending.runId,
      itemId: pending.itemId ?? '',
      requestedAt: pending.requestedAt,
      toolCallId: pending.toolCallId,
      ...sourceFields,
    };
    const p = pending.payload ?? {};
    switch (pending.kind) {
      case 'tool-approval':
        this._emitTurnEvent({
          type: 'tool_approval_required',
          ...base,
          toolName: pending.toolName ?? '',
          ...(p.toolCategory !== undefined ? { toolCategory: p.toolCategory } : {}),
          approvalReasons: p.approvalReasons ?? [],
          // §10.2 (events.ts:202 "Public payloads are JSON-safe projections"):
          // `p.input` is the raw runtime `payload.args` from the suspended
          // output (Date/Map/class/bigint). Project it at emit so the first
          // live subscriber and the durable/replayed row carry the identical
          // value — the same live===replay guarantee as tool_start/tool_end.
          input: projectToolEventPayloadForJson(p.input, 'tool_approval_required.input'),
        } as EmitInput);
        return;
      case 'tool-suspension':
        this._emitTurnEvent({
          type: 'tool_suspension_required',
          ...base,
          toolName: pending.toolName ?? '',
          // §10.2: `suspendData` is the tool's raw `suspendPayload` and can hold
          // runtime objects, so project it at emit for live===replay parity.
          suspendData: projectToolEventPayloadForJson(p.suspendData, 'tool_suspension_required.suspendData'),
        } as EmitInput);
        return;
      case 'question':
        this._emitTurnEvent({
          type: 'question_pending',
          ...base,
          question: p.question ?? '',
          ...(p.options !== undefined ? { options: p.options } : {}),
          ...(p.selectionMode !== undefined ? { selectionMode: p.selectionMode } : {}),
        } as EmitInput);
        return;
      case 'plan-approval':
        this._emitTurnEvent({
          type: 'plan_approval_required',
          ...base,
          title: p.title ?? '',
          plan: p.plan ?? '',
        } as EmitInput);
        return;
      case 'sandbox-access':
        // §10.2: sandbox/path-access prompts project to question_pending.
        this._emitTurnEvent({
          type: 'question_pending',
          ...base,
          question: p.sandboxAccess?.reason ?? `Allow ${p.sandboxAccess?.semanticType ?? 'sandbox'} access?`,
        } as EmitInput);
        return;
    }
  }

  private _raceActiveTurnWaiter<T>(promise: Promise<T>, activeTurnWaiter?: Promise<never>): Promise<T> {
    return activeTurnWaiter ? Promise.race([promise, activeTurnWaiter]) : promise;
  }

  private _trackBackgroundTurnCompletion<T>(promise: Promise<T>): Promise<T> {
    this._backgroundTurnCompletions.add(promise);
    void promise
      .finally(() => {
        this._backgroundTurnCompletions.delete(promise);
      })
      .catch(() => {});
    return promise;
  }

  private _shouldWriteTurnFailureEvidence(err: unknown): boolean {
    return this._state !== 'deleted' && !(err instanceof HarnessSessionDeletedError);
  }

  private async _withActiveDeletedWaiter<T>(fn: (activeTurnWaiter: Promise<never>) => Promise<T>): Promise<T> {
    const activeTurnWaiter = this._createActiveTurnWaiter();
    void activeTurnWaiter.promise.catch(() => {});
    try {
      return await fn(activeTurnWaiter.promise);
    } finally {
      activeTurnWaiter.cleanup();
    }
  }

  /** Capture the first run id for the active turn display state. */
  private _captureTurnRunId(full: FullOutput<unknown>): void {
    if (this._currentTurnAbortController === undefined) return;
    if (full.runId && this._currentRunId === undefined) {
      this._currentRunId = full.runId;
    }
  }

  private _tokenUsageDeltaFromFullOutput(full: FullOutput<unknown>): TokenUsage | undefined {
    const usage = (full as { totalUsage?: unknown; usage?: unknown }).totalUsage ?? (full as { usage?: unknown }).usage;
    if (usage && typeof usage === 'object') {
      const u = usage as {
        promptTokens?: number;
        completionTokens?: number;
        totalTokens?: number;
        inputTokens?: number;
        outputTokens?: number;
      };
      const prompt = u.promptTokens ?? u.inputTokens;
      const completion = u.completionTokens ?? u.outputTokens;
      const promptNumber = typeof prompt === 'number' && Number.isInteger(prompt) && prompt >= 0 ? prompt : undefined;
      const completionNumber =
        typeof completion === 'number' && Number.isInteger(completion) && completion >= 0 ? completion : undefined;
      const delta: TokenUsage = { promptTokens: 0, completionTokens: 0, totalTokens: 0 };
      const incremented =
        promptNumber !== undefined ||
        completionNumber !== undefined ||
        (typeof u.totalTokens === 'number' && Number.isInteger(u.totalTokens) && u.totalTokens >= 0);
      if (promptNumber !== undefined) delta.promptTokens += promptNumber;
      if (completionNumber !== undefined) delta.completionTokens += completionNumber;
      const derivedTotalTokens = (promptNumber ?? 0) + (completionNumber ?? 0);
      if (typeof u.totalTokens === 'number' && Number.isInteger(u.totalTokens) && u.totalTokens >= 0) {
        delta.totalTokens += u.totalTokens < derivedTotalTokens ? derivedTotalTokens : u.totalTokens;
      } else if (promptNumber !== undefined || completionNumber !== undefined) {
        // Providers that only emit `inputTokens`/`outputTokens` leave `totalTokens`
        // off; derive it so the aggregate stays consistent with its parts.
        delta.totalTokens += derivedTotalTokens;
      }
      return incremented ? delta : undefined;
    }
    return undefined;
  }

  private _applyTokenUsageDelta(delta: TokenUsage | undefined): void {
    if (delta === undefined) return;
    this._tokenUsage = {
      promptTokens: this._tokenUsage.promptTokens + delta.promptTokens,
      completionTokens: this._tokenUsage.completionTokens + delta.completionTokens,
      totalTokens: this._tokenUsage.totalTokens + delta.totalTokens,
    };
    // §10.2 StateEvent: emit the new cumulative session total whenever a turn
    // commits a usage delta (covers message / signal / queue turn paths, which
    // all funnel through here).
    this._emit({ type: 'token_usage_changed', usage: { ...this._tokenUsage } });
  }

  private _clearPendingTokenUsageFlushErrorIfSaved(tokenUsage: TokenUsage | undefined): void {
    if (
      tokenUsage !== undefined &&
      tokenUsage.promptTokens === this._tokenUsage.promptTokens &&
      tokenUsage.completionTokens === this._tokenUsage.completionTokens &&
      tokenUsage.totalTokens === this._tokenUsage.totalTokens
    ) {
      this._pendingTokenUsageFlushError = undefined;
      if (this._pendingDurableTurnFlushError?.pendingResume === undefined) {
        this._pendingDurableTurnFlushError = undefined;
      }
    }
  }

  private _latchDurableTurnFlushError(err: unknown, full?: FullOutput<unknown>): void {
    if (!this._isExpectedFlushLifecycleError(err)) {
      this._pendingDurableTurnFlushError ??= { error: err, pendingResume: this._pendingResumeKeyFromOutput(full) };
    }
  }

  private _pendingResumeKeyFromOutput(
    full: FullOutput<unknown> | undefined,
  ): { runId: string; toolCallId: string } | undefined {
    if (!full?.runId || full.finishReason !== 'suspended') return undefined;
    const payload = full.suspendPayload as { toolCallId?: unknown } | undefined;
    return typeof payload?.toolCallId === 'string' ? { runId: full.runId, toolCallId: payload.toolCallId } : undefined;
  }

  private _clearPendingDurableTurnFlushErrorIfRepaired(full?: FullOutput<unknown>): void {
    const latched = this._pendingDurableTurnFlushError;
    if (latched === undefined) return;
    const latchedPending = latched.pendingResume;
    if (latchedPending === undefined) {
      this._pendingDurableTurnFlushError = undefined;
      return;
    }
    const repairedPending = this._pendingResumeKeyFromOutput(full);
    if (
      repairedPending?.runId === latchedPending.runId &&
      repairedPending.toolCallId === latchedPending.toolCallId &&
      this._record.pendingResume?.runId === latchedPending.runId &&
      this._record.pendingResume.toolCallId === latchedPending.toolCallId
    ) {
      this._pendingDurableTurnFlushError = undefined;
    }
  }

  private _recordTokenUsageMatchesLive(opts: { tolerateInvalidZero?: boolean } = {}): boolean {
    const stored = this._record.tokenUsage;
    const liveIsZero =
      this._tokenUsage.promptTokens === 0 &&
      this._tokenUsage.completionTokens === 0 &&
      this._tokenUsage.totalTokens === 0;
    if (
      stored === undefined ||
      !this._isValidTokenCount(stored.promptTokens) ||
      !this._isValidTokenCount(stored.completionTokens) ||
      !this._isValidTokenCount(stored.totalTokens)
    ) {
      return opts.tolerateInvalidZero === true && liveIsZero;
    }
    return (
      stored.promptTokens === this._tokenUsage.promptTokens &&
      stored.completionTokens === this._tokenUsage.completionTokens &&
      stored.totalTokens === this._tokenUsage.totalTokens
    );
  }

  private _isValidTokenCount(value: unknown): value is number {
    return typeof value === 'number' && Number.isInteger(value) && value >= 0;
  }

  private _recordTurnCompletion(full: FullOutput<unknown>, opts: { persist?: boolean } = {}): TokenUsage | undefined {
    this._captureTurnRunId(full);
    const delta = this._tokenUsageDeltaFromFullOutput(full);
    this._applyTokenUsageDelta(delta);
    if (delta !== undefined && opts.persist !== false) this._schedulePersistTokenUsage();
    return delta;
  }

  private _recordMessageTurnCompletion(
    full: FullOutput<unknown>,
    opts: { persist?: boolean } = {},
  ): { tokenUsageDelta?: TokenUsage; tokenUsageAccounted: boolean } {
    this._captureTurnRunId(full);
    if (full.runId && this._messageTokenAccountedRunIds.has(full.runId)) {
      return { tokenUsageAccounted: true };
    }
    const tokenUsageDelta = this._tokenUsageDeltaFromFullOutput(full);
    this._applyTokenUsageDelta(tokenUsageDelta);
    if (tokenUsageDelta !== undefined) {
      if (full.runId) this._messageTokenAccountedRunIds.add(full.runId);
      if (opts.persist !== false) this._schedulePersistTokenUsage();
      return { tokenUsageDelta, tokenUsageAccounted: true };
    }
    return { tokenUsageAccounted: false };
  }

  private _messageSuspendedTokenUsageDelta(full: FullOutput<unknown>): TokenUsage | undefined {
    if (
      full.runId &&
      (this._messageTokenAccountedRunIds.has(full.runId) || this._messageTokenAccountingRunIds.has(full.runId))
    ) {
      return undefined;
    }
    return this._tokenUsageDeltaFromFullOutput(full);
  }

  private _reserveMessageSuspendedTokenUsage(full: FullOutput<unknown>): {
    tokenUsageDelta?: TokenUsage;
    reservedRunId?: string;
    reservation?: Deferred<void>;
  } {
    const tokenUsageDelta = this._messageSuspendedTokenUsageDelta(full);
    if (tokenUsageDelta !== undefined && full.runId) {
      const reservation = createDeferred<void>();
      this._messageTokenAccountingRunIds.add(full.runId);
      this._messageTokenAccountingReservations.set(full.runId, reservation);
      void reservation.promise.catch(() => {});
      return { tokenUsageDelta, reservedRunId: full.runId, reservation };
    }
    return { tokenUsageDelta };
  }

  private _commitMessageSuspendedTokenUsage(
    full: FullOutput<unknown>,
    reservation: { tokenUsageDelta?: TokenUsage; reservedRunId?: string; reservation?: Deferred<void> },
  ): void {
    if (reservation.reservedRunId !== undefined) {
      this._messageTokenAccountingRunIds.delete(reservation.reservedRunId);
      if (this._messageTokenAccountingReservations.get(reservation.reservedRunId) === reservation.reservation) {
        this._messageTokenAccountingReservations.delete(reservation.reservedRunId);
      }
      this._messageTokenAccountedRunIds.add(reservation.reservedRunId);
      reservation.reservation?.resolve();
    } else if (reservation.tokenUsageDelta !== undefined && full.runId) {
      this._messageTokenAccountedRunIds.add(full.runId);
    }
  }

  private _rollbackMessageSuspendedTokenUsage(reservation: {
    reservedRunId?: string;
    reservation?: Deferred<void>;
  }): void {
    if (reservation.reservedRunId !== undefined) {
      this._messageTokenAccountingRunIds.delete(reservation.reservedRunId);
      if (this._messageTokenAccountingReservations.get(reservation.reservedRunId) === reservation.reservation) {
        this._messageTokenAccountingReservations.delete(reservation.reservedRunId);
      }
      reservation.reservation?.resolve();
    }
  }

  private async _waitForMessageSuspendedTokenUsageOwner(
    full: FullOutput<unknown>,
    activeTurnWaiter?: Promise<never>,
  ): Promise<void> {
    if (!full.runId) return;
    while (!this._messageTokenAccountedRunIds.has(full.runId)) {
      const reservation = this._messageTokenAccountingReservations.get(full.runId);
      if (reservation === undefined) return;
      await this._raceActiveTurnWaiter(reservation.promise, activeTurnWaiter);
    }
  }

  private async _captureMessageSuspendWithTokenUsage(
    full: FullOutput<unknown>,
    queuedItemId: string | undefined,
    modeId: string,
    modelId: string,
    activeTurnWaiter?: Promise<never>,
  ): Promise<void> {
    await this._waitForMessageSuspendedTokenUsageOwner(full, activeTurnWaiter);
    const reservation = this._reserveMessageSuspendedTokenUsage(full);
    try {
      await this._raceActiveTurnWaiter(
        this._maybeCaptureSuspend(full, queuedItemId, modeId, modelId, {
          tokenUsageDelta: reservation.tokenUsageDelta,
        }),
        activeTurnWaiter,
      );
      this._commitMessageSuspendedTokenUsage(full, reservation);
    } catch (err) {
      this._rollbackMessageSuspendedTokenUsage(reservation);
      throw err;
    }
  }

  /**
   * Trigger a no-op `_flushUpdate` so the latest `_tokenUsage` is overlaid into
   * `SessionRecord.tokenUsage` on disk. Fire-and-forget — serialized via
   * `_flushChain` so it never races concurrent setters, and skipped when the
   * session is no longer in a state that accepts writes. Non-lifecycle storage
   * failures are latched onto `_pendingTokenUsageFlushError` so
   * `_internalAwaitFlushChain()` can surface them to shutdown/test callers.
   */
  private _schedulePersistTokenUsage(): void {
    if (this._state !== 'live' && this._state !== 'closing') return;
    void this._persistTokenUsageOrLatch().catch(err => {
      if (this._isExpectedFlushLifecycleError(err)) return;
      this._pendingTokenUsageFlushError ??= err;
    });
  }

  private _persistTokenUsage(): Promise<void> {
    if (this._state !== 'live' && this._state !== 'closing') return Promise.resolve();
    return this._flushUpdate(prev => prev);
  }

  private async _persistTokenUsageOrLatch(): Promise<void> {
    try {
      await this._persistTokenUsage();
    } catch (err) {
      if (!this._isExpectedFlushLifecycleError(err)) this._pendingTokenUsageFlushError ??= err;
      throw err;
    }
  }

  async _internalPersistTokenUsageForShutdown(opts: { deadlineAt?: number } = {}): Promise<void> {
    const timeoutMs = opts.deadlineAt === undefined ? undefined : Math.max(0, opts.deadlineAt - Date.now());
    if (
      this._pendingTokenUsageFlushError === undefined &&
      this._recordTokenUsageMatchesLive({ tolerateInvalidZero: timeoutMs === 0 })
    ) {
      return;
    }
    const persist = this._persistTokenUsageOrLatch();
    void persist.catch(() => {});
    if (timeoutMs === undefined) {
      await persist;
      return;
    }
    if (timeoutMs === 0) {
      throw new HarnessValidationError('shutdown()', 'Session token usage did not flush before shutdown deadline');
    }
    const timedOut = Symbol('harness-token-usage-timeout');
    const result = await Promise.race([persist.then(() => undefined), delay(timeoutMs).then(() => timedOut)]);
    if (result === timedOut) {
      throw new HarnessValidationError('shutdown()', 'Session token usage did not flush before shutdown deadline');
    }
  }

  private _persistTokenUsageDelta(delta: TokenUsage | undefined): Promise<void> {
    if (delta === undefined) return Promise.resolve();
    if (this._state !== 'live' && this._state !== 'closing') return Promise.resolve();
    return this._flushUpdate(prev => prev, { tokenUsageDelta: delta });
  }

  private _isExpectedFlushLifecycleError(err: unknown): boolean {
    return (
      err instanceof HarnessSessionClosedError ||
      err instanceof HarnessSessionDeletedError ||
      err instanceof HarnessStateConflictError
    );
  }

  /**
   * Wait for any in-flight `_flushUpdate` writes (including the trailing
   * persist scheduled by `_recordTurnCompletion`) to settle. Throws if a
   * scheduled token-usage flush hit a non-lifecycle storage error so shutdown
   * and tests surface durability gaps instead of silently dropping them.
   *
   * @internal
   */
  async _internalAwaitFlushChain(opts: { deadlineAt?: number } = {}): Promise<void> {
    const waitUntilDeadline = async (promise: Promise<unknown>): Promise<boolean> => {
      if (opts.deadlineAt === undefined) {
        await promise;
        return true;
      }
      const timeoutMs = Math.max(0, opts.deadlineAt - Date.now());
      const timedOut = Symbol('harness-flush-timeout');
      const result = await Promise.race([promise.then(() => undefined), delay(timeoutMs).then(() => timedOut)]);
      return result !== timedOut;
    };

    while (true) {
      const backgroundTurnCompletions = Array.from(this._backgroundTurnCompletions);
      if (backgroundTurnCompletions.length > 0) {
        const completed = await waitUntilDeadline(Promise.allSettled(backgroundTurnCompletions));
        if (!completed) {
          throw new HarnessValidationError(
            'shutdown()',
            'Session background work did not flush before shutdown deadline',
          );
        }
      }
      const chain = this._flushChain;
      const completed = await waitUntilDeadline(chain);
      if (!completed) {
        throw new HarnessValidationError('shutdown()', 'Session storage writes did not flush before shutdown deadline');
      }
      if (this._flushChain === chain && this._backgroundTurnCompletions.size === 0) break;
    }
    const latched = this._pendingTokenUsageFlushError;
    if (latched !== undefined) {
      this._pendingTokenUsageFlushError = undefined;
      throw latched;
    }
    const durableTurnLatched = this._pendingDurableTurnFlushError;
    if (durableTurnLatched !== undefined) {
      if (
        durableTurnLatched.pendingResume !== undefined &&
        this._record.pendingResume?.runId === durableTurnLatched.pendingResume.runId &&
        this._record.pendingResume.toolCallId === durableTurnLatched.pendingResume.toolCallId
      ) {
        this._pendingDurableTurnFlushError = undefined;
        return;
      }
      throw durableTurnLatched.error;
    }
  }

  /** Latched storage error from a scheduled token-usage persist; surfaced by
   * `_internalAwaitFlushChain()` so shutdown and tests can act on it. */
  private _pendingTokenUsageFlushError: unknown;
  private _pendingDurableTurnFlushError:
    | { error: unknown; pendingResume?: { runId: string; toolCallId: string } }
    | undefined;

  /**
   * True while a turn (message or queued) is in flight against the agent.
   * Goes back to false on terminal completion, suspension, or abort.
   * Subscribers should drive UI affordances (e.g. spinner, ESC-to-cancel)
   * from this signal in combination with `lifecycleState`.
   */
  isRunning(): boolean {
    return this._currentTurnAbortController !== undefined;
  }

  /**
   * True when the session has any pending work — an in-flight turn, an
   * active queue drain, a queued item awaiting its turn, or a pending
   * `respondTo*` suspension. False only when the session is fully idle.
   *
   * Broader than `isRunning()`: a session can be `!isRunning()` but still
   * `isBusy()` (queue items not yet drained, awaiting `respondToQuestion`,
   * etc.). UI affordances that care about "anything happening at all"
   * (e.g. "session is working" indicators) should read this; affordances
   * tied to a single live turn (spinner, abort button) should read
   * `isRunning()`.
   */
  isBusy(): boolean {
    if (this._currentTurnAbortController !== undefined) return true;
    if (this._draining) return true;
    if (this._currentQueuedItemId !== undefined) return true;
    if ((this._record.pendingQueue?.length ?? 0) > 0) return true;
    if (this._record.pendingResume !== undefined && this._record.cancelRequest === undefined) return true;
    return false;
  }

  /** Classify why the session is busy for `HarnessBusyError` (§4.5a). */
  private _busyReason(): 'in_flight' | 'pending_approval' | 'pending_suspension' | 'pending_question' | 'pending_plan' {
    const pending = this._record.pendingResume;
    if (pending !== undefined && this._record.cancelRequest === undefined) {
      switch (pending.kind) {
        case 'tool-suspension':
          return 'pending_suspension';
        case 'question':
          return 'pending_question';
        case 'plan-approval':
          return 'pending_plan';
        // 'tool-approval' and 'sandbox-access' are both approval-style gates.
        default:
          return 'pending_approval';
      }
    }
    return 'in_flight';
  }

  private _isShutdownDrainBusy(): boolean {
    if (this._currentTurnAbortController !== undefined) return true;
    if (this._record.pendingResume !== undefined) return false;
    if (this._draining) return true;
    if (this._currentQueuedItemId !== undefined) return true;
    return (this._record.pendingQueue?.length ?? 0) > 0;
  }

  /**
   * Number of items currently waiting in `pendingQueue` (excluding any
   * queued item already drained into a live turn — that one is tracked
   * via `_currentQueuedItemId`). Cheap, synchronous, safe to poll from UI.
   */
  getQueueDepth(): number {
    return this._record.pendingQueue?.length ?? 0;
  }

  /**
   * §4.2c: the in-flight run's id, or `null` when no run is active. Reads the
   * live run marker stamped at `agent_start`. Spec also allows a reconciled
   * `SessionRecord.currentRun` snapshot fallback, but v1 does not yet persist
   * `currentRun` (that durable read-model is a separate lane), so this is
   * live-best-effort: a rehydrated idle session reports `null`.
   */
  getCurrentRunId(): string | null {
    return this._currentRunId ?? null;
  }

  /** §4.2c: the in-flight run's trace id, or `null` when absent (live-best-effort, see {@link getCurrentRunId}). */
  getCurrentTraceId(): string | null {
    return this._currentTraceId ?? null;
  }

  /**
   * Cumulative token usage for this session, accumulated across every
   * completed turn (manual or queued). Returns a fresh shallow copy so
   * callers can't mutate the running aggregate.
   *
   * Durable across rehydration: counters are persisted into
   * `SessionRecord.tokenUsage` on every save and seeded from there on
   * construction, so reopens after eviction or process restart carry the
   * accumulated value instead of resetting to zero.
   */
  getTokenUsage(): TokenUsage {
    return { ...this._tokenUsage };
  }

  async extendLease(opts: { ttlMs: number }): Promise<void> {
    if (this._state === 'deleted') {
      throw new HarnessSessionDeletedError(this.id, this._record.resourceId, this._record.threadId);
    }
    if (this._state !== 'live' && this._state !== 'closing') {
      throw new HarnessSessionClosedError(this.id);
    }
    if (!Number.isFinite(opts.ttlMs) || !Number.isInteger(opts.ttlMs) || opts.ttlMs <= 0) {
      throw new HarnessValidationError('extendLease().ttlMs', 'ttlMs must be a positive integer in milliseconds');
    }
    if (opts.ttlMs > MAX_LEASE_EXTENSION_MS) {
      throw new HarnessValidationError(
        'extendLease().ttlMs',
        `ttlMs must be at most ${MAX_LEASE_EXTENSION_MS}ms (24h)`,
      );
    }

    const effectiveTtl = Math.max(opts.ttlMs, this._harness._internalLeaseTtlMs);
    // §5.8: a child has no separately-renewable lease, so `extendLease` extends
    // the whole subtree through the root (capped at the root, never shrinking
    // it). The helper resolves the live root, serializes on the ROOT's lease
    // chain (so it can't race the background sweep), renews root + active
    // descendants atomically, advances the ROOT's extension deadline so the next
    // sweep keeps the extension, and fences the subtree (throwing locked/not-
    // found) if ownership can't be proven. We do NOT wrap this in the calling
    // session's own renewal chain: the helper already serializes on the root
    // chain, and a second wrap would deadlock when the caller IS the root.
    await this._harness._internalRenewProveSubtree(this, effectiveTtl, { propagateRootDeadline: true });
  }

  async withExtendedLease<T>(fn: () => Promise<T>, opts: { ttlMs: number }): Promise<T> {
    await this.extendLease(opts);
    return fn();
  }

  _getEffectiveLeaseTtlMs(defaultTtlMs: number): number {
    const deadline = this._leaseExtensionDeadline;
    if (deadline === undefined || !Number.isFinite(deadline)) return defaultTtlMs;
    const remaining = deadline - Date.now();
    return remaining > defaultTtlMs ? remaining : defaultTtlMs;
  }

  /**
   * @internal §5.8 — advance the root's effective-TTL deadline after a subtree
   * renewal so the next background sweep keeps the extension instead of
   * shortening it back to the default TTL. Advance-only.
   */
  _setLeaseExtensionDeadline(deadline: number): void {
    if (this._leaseExtensionDeadline === undefined || deadline > this._leaseExtensionDeadline) {
      this._leaseExtensionDeadline = deadline;
    }
  }

  _enqueueLeaseRenewal(run: () => Promise<void>): Promise<void> {
    const guardedRun = async () => {
      if (this._state !== 'live' && this._state !== 'closing') return;
      await run();
    };
    const next = this._leaseRenewalChain.then(guardedRun, guardedRun);
    this._leaseRenewalChain = next.catch(() => {});
    return next;
  }

  /**
   * Resolve when the session goes fully idle (`!isBusy()`). If the session
   * is already idle when called, resolves on the next microtask.
   *
   * Rejects with `HarnessValidationError` if `timeoutMs` is provided and
   * elapses before the session becomes idle. Rejects with
   * `HarnessSessionClosingError` if close starts while waiting,
   * `HarnessSessionClosedError` if the session closes first, or
   * `HarnessSessionDeletedError` if hard-delete removes the session first.
   *
   * Useful in tests and TUI flows that want to await a clean boundary
   * before tearing down or asserting final state.
   */
  waitForIdle(opts?: { timeoutMs?: number }): Promise<void> {
    this._assertLive('waitForIdle()');
    if (!this.isBusy()) return Promise.resolve();

    return new Promise<void>((resolve, reject) => {
      let timer: ReturnType<typeof setTimeout> | undefined;
      const waiter: IdleWaiter = {
        check: () => {
          if (!this.isBusy()) {
            cleanup();
            resolve();
            return true;
          }
          return false;
        },
        reject,
        cleanup: () => {},
      };
      const cleanup = () => {
        if (timer !== undefined) clearTimeout(timer);
        this._idleWaiters.delete(waiter);
      };
      waiter.cleanup = cleanup;
      this._idleWaiters.add(waiter);
      if (opts?.timeoutMs !== undefined) {
        timer = setTimeout(() => {
          cleanup();
          reject(new HarnessValidationError('waitForIdle()', `session did not become idle within ${opts.timeoutMs}ms`));
        }, opts.timeoutMs);
      }
    });
  }

  /**
   * Re-check every idle/drain waiter whose predicate is now satisfied. Cheap
   * when there are no waiters (common case). Called from every state
   * transition that might tip the session idle or durably parked: `_endTurn`,
   * queue drain shutdown, queued-turn settlement, suspension parking.
   */
  private _notifyMaybeIdle(): void {
    if (this._idleWaiters.size === 0) return;
    const waiters = Array.from(this._idleWaiters);
    for (const w of waiters) w.check();
  }

  /** @internal — close uses this after the durable closing marker commits. */
  _waitForCloseDrain(closeDeadlineAt: number): Promise<void> {
    void this._maybeDrainQueue();
    if (!this.isBusy()) return Promise.resolve();
    const timeoutMs = Math.max(0, closeDeadlineAt - Date.now());

    return new Promise<void>(resolve => {
      let timer: ReturnType<typeof setTimeout> | undefined;
      const resolveAfter = (settle?: Promise<void>) => {
        cleanup();
        if (settle) {
          void settle.finally(resolve);
          return;
        }
        resolve();
      };
      const waiter: IdleWaiter = {
        check: () => {
          if (!this.isBusy()) {
            resolveAfter();
            return true;
          }
          return false;
        },
        reject: () => {
          resolveAfter();
        },
        cleanup: () => {},
      };
      const cleanup = () => {
        if (timer !== undefined) clearTimeout(timer);
        this._idleWaiters.delete(waiter);
      };
      waiter.cleanup = cleanup;
      this._idleWaiters.add(waiter);
      if (timeoutMs === 0) {
        this._abortActiveTurn('session_closed');
        resolveAfter(this._failPendingQueueForClose(harnessSessionClosingError(this)));
      } else {
        timer = setTimeout(() => {
          timer = undefined;
          this._abortActiveTurn('session_closed');
          resolveAfter(this._failPendingQueueForClose(harnessSessionClosingError(this)));
        }, timeoutMs);
      }
    });
  }

  /** @internal — shutdown waits for admitted work without turning queued items into close failures. */
  _waitForShutdownDrain(drainDeadlineAt: number): Promise<void> {
    void this._maybeDrainQueue();
    if (!this._isShutdownDrainBusy()) return Promise.resolve();
    const timeoutMs = Math.max(0, drainDeadlineAt - Date.now());

    return new Promise<void>((resolve, reject) => {
      let timer: ReturnType<typeof setTimeout> | undefined;
      let waiter!: IdleWaiter;
      const cleanup = () => {
        if (timer !== undefined) clearTimeout(timer);
        this._idleWaiters.delete(waiter);
      };
      const resolveAfter = () => {
        cleanup();
        resolve();
      };
      waiter = {
        check: () => {
          if (!this._isShutdownDrainBusy()) {
            resolveAfter();
            return true;
          }
          return false;
        },
        reject: () => {
          resolveAfter();
        },
        cleanup,
      };
      this._idleWaiters.add(waiter);
      if (waiter.check()) return;
      const rejectAfterTimeout = () => {
        this._abortActiveTurn('process_restart');
        cleanup();
        reject(new HarnessValidationError('shutdown()', 'Session did not drain before shutdown deadline'));
      };
      if (timeoutMs === 0) {
        rejectAfterTimeout();
      } else {
        timer = setTimeout(() => {
          timer = undefined;
          rejectAfterTimeout();
        }, timeoutMs);
      }
    });
  }

  /**
   * Cancel the in-flight turn (if any). The agent receives the abort signal and
   * unwinds. No-op when no turn is running.
   *
   * §6.2: the tool-visible `abortSignal.reason` is always a typed
   * `HarnessAbortedError`. Public `abort()` is ordinary agent-layer cancellation
   * → `agent_aborted`. The optional `reason` string is accepted for source
   * compatibility but is NOT a structured reason — the harness owns the typed
   * `HarnessAbortReason`, so a caller cannot mislabel a lifecycle cause.
   */
  abort(_opts?: { reason?: string }): void {
    this._abortActiveTurn('agent_aborted');
  }

  /**
   * §6.2: abort the in-flight turn's controller with a typed `HarnessAbortedError`
   * so tools observe `abortSignal.reason instanceof HarnessAbortedError`. All
   * harness-internal abort sources (public abort, cancel, close/shutdown drain
   * timeout, eviction, parent propagation) route here. No-op when idle. Abort is
   * idempotent — the first selected reason wins (§6.2 "not replaced").
   */
  private _abortActiveTurn(reason: HarnessAbortReason, opts?: { parentSessionId?: string }): void {
    const controller = this._currentTurnAbortController;
    if (!controller) return;
    controller.abort(new HarnessAbortedError(this.id, reason, opts?.parentSessionId));
  }

  /**
   * §6.2: select the typed `HarnessAbortedError` to stamp on this turn's
   * controller when an external caller `AbortSignal` fires. A signal already
   * carrying THIS session's typed abort is forwarded unchanged (the harness-owned
   * reason "is not replaced by a later source"). A parent run's abort propagating
   * into a live subagent (built-in subagents share the parent tool's abort signal)
   * becomes child-local `parent_aborted`; a caller abort while this session is in
   * the close lifecycle is `session_closed`; any other external/raw caller abort
   * is ordinary `agent_aborted`.
   */
  private _forwardCallerAbortReason(callerSignal: AbortSignal): HarnessAbortedError {
    const callerReason = (callerSignal as { reason?: unknown }).reason;
    if (callerReason instanceof HarnessAbortedError && callerReason.sessionId === this.id) {
      return callerReason;
    }
    if (
      this.parentSessionId !== undefined &&
      callerReason instanceof HarnessAbortedError &&
      callerReason.sessionId === this.parentSessionId &&
      !this.isClosing
    ) {
      return new HarnessAbortedError(this.id, 'parent_aborted', this.parentSessionId);
    }
    if (this.isClosing) {
      return new HarnessAbortedError(this.id, 'session_closed');
    }
    return new HarnessAbortedError(this.id, 'agent_aborted');
  }

  private _currentCancelRequest(): SessionRecord['cancelRequest'] {
    return this._record.cancelRequest;
  }

  /**
   * Durably cancel this session. The first cancel request wins, aborts
   * in-flight work, drops queued items, and emits audit events. A queued item
   * that already started is failed durably here too; its live turn still
   * receives the abort signal and unwinds through the normal turn cleanup path.
   */
  async cancel(opts?: { reason?: string; requestedBy?: string }): Promise<void> {
    // Public entry: never parent-originated. The §6.2 `parent_aborted` source is
    // harness-owned — only the internal subagent cascade may select it (passing
    // the real parent id), so a caller cannot mislabel the abort by supplying a
    // matching `requestedBy`.
    return this._cancelInternal(opts, undefined);
  }

  private async _cancelInternal(
    opts?: { reason?: string; requestedBy?: string },
    parentOrigin?: string,
  ): Promise<void> {
    this._assertNotDeleted();
    if (this._currentCancelRequest() !== undefined) return;

    const reason = opts?.reason;
    const requestedBy = opts?.requestedBy;
    const requestedAt = Date.now();
    const removedItems: { queuedItemId: string; admissionId?: string }[] = [];
    const completedItems: { queuedItemId: string; result: AgentResult }[] = [];
    const completedUnfinalizedItems: { item: QueuedItem; result: FullOutput<unknown>; modeId: string }[] = [];
    const cancelErrorForReceipt = new HarnessSessionCancelledError(this.id, reason);
    let attemptedWrite = false;

    await this._flushUpdate(prev => {
      if (prev.cancelRequest !== undefined) return prev;

      attemptedWrite = true;
      const existingReceipts = prev.queueAdmissionReceipts ?? {};
      const nextReceipts: Record<string, QueueAdmissionReceipt> = { ...existingReceipts };

      for (const item of prev.pendingQueue ?? []) {
        const receipt = existingReceipts[item.id];
        if (receipt?.status === 'completed' && receipt.result !== undefined) {
          if (receipt.postRunFinalizedAt !== undefined) {
            completedItems.push({ queuedItemId: item.id, result: receipt.result as AgentResult });
          } else {
            completedUnfinalizedItems.push({
              item,
              result: receipt.result as FullOutput<unknown>,
              modeId: receipt.modeId ?? item.mode ?? prev.modeId,
            });
          }
          continue;
        }

        removedItems.push({ queuedItemId: item.id, admissionId: item.admissionId });
        if (receipt && receipt.status !== 'completed' && receipt.status !== 'failed' && receipt.status !== 'dead') {
          nextReceipts[item.id] = {
            ...receipt,
            status: 'failed',
            error: projectHarnessPublicError(cancelErrorForReceipt),
            failedAt: receipt.failedAt ?? requestedAt,
            updatedAt: requestedAt,
          };
        }
      }

      const next: SessionRecord = {
        ...prev,
        cancelRequest: {
          requestedAt,
          ...(reason !== undefined ? { reason } : {}),
          ...(requestedBy !== undefined ? { requestedBy } : {}),
        },
        pendingQueue: completedUnfinalizedItems.map(completed => completed.item),
      };
      if (Object.keys(nextReceipts).length > 0) {
        next.queueAdmissionReceipts = nextReceipts;
      }
      return next;
    });

    const committedCancel = this._currentCancelRequest();
    if (committedCancel === undefined) return;
    if (!attemptedWrite) return;
    if (committedCancel.requestedAt !== requestedAt) return;
    const durableReason = committedCancel.reason;
    this._clearQueueWakeTimer();

    // §10.2: cancellation/queue-drop are not public HarnessEventV1 events. The
    // observable effect is the rejected operation promises + cleared queue +
    // updated display snapshot below.
    for (const dropped of removedItems) {
      const resolver = this._queueResolvers.get(dropped.queuedItemId);
      if (resolver) {
        this._queueResolvers.delete(dropped.queuedItemId);
        resolver.reject(new HarnessSessionCancelledError(this.id, durableReason));
      }
    }
    for (const completed of completedItems) {
      const resolver = this._queueResolvers.get(completed.queuedItemId);
      if (resolver) {
        this._queueResolvers.delete(completed.queuedItemId);
        resolver.resolve(completed.result);
      }
    }
    if (
      this._currentTurnAbortController === undefined &&
      this._currentQueuedItemId !== undefined &&
      removedItems.some(item => item.queuedItemId === this._currentQueuedItemId)
    ) {
      this._currentQueuedItemId = undefined;
      this._currentQueuedItemSource = undefined;
    }
    // §6.2: the tool-visible abort reason is typed. A cancel cascaded from this
    // session's parent (harness-owned `parentOrigin`, matching this child's
    // parentSessionId) surfaces `parent_aborted` with the parent id; any other
    // cancel is ordinary `agent_aborted`. The durable `cancelRequest.reason` string
    // is preserved separately on the record; the caller-supplied `requestedBy` is an
    // audit field and is NOT the abort-source authority.
    if (parentOrigin !== undefined && parentOrigin === this.parentSessionId) {
      this._abortActiveTurn('parent_aborted', { parentSessionId: this.parentSessionId });
    } else {
      this._abortActiveTurn('agent_aborted');
    }

    for (const completed of completedUnfinalizedItems) {
      await this._settleCompletedQueuedItemAfterCancellation(completed.item, completed.result, completed.modeId);
    }

    this._notifyMaybeIdle();

    if (this._activeSubagents.size === 0) return;
    const childIds = Array.from(this._activeSubagents.values()).map(s => s.subagentSessionId);
    await Promise.all(
      childIds.map(async childId => {
        const child = this._harness._internalGetLiveSession(childId);
        if (!child) return;
        try {
          // Internal cascade: pass this session id as the harness-owned parent
          // origin so the child's live turn aborts with `parent_aborted`.
          await child._cancelInternal({ reason: reason ?? 'parent_cancelled', requestedBy: this.id }, this.id);
        } catch {
          // Cancellation has already committed for the parent. Child
          // propagation is best-effort and must not roll it back.
        }
      }),
    );
  }

  /**
   * Cancel a single queued turn before it starts. Unknown ids and active queue
   * heads are no-ops; active work is settled by session-level cancel.
   */
  async cancelQueuedItem(opts: { queuedItemId: string; reason?: string }): Promise<void> {
    this._assertNotDeleted();
    const targetId = opts.queuedItemId;
    if (!targetId) {
      throw new HarnessValidationError('cancelQueuedItem().queuedItemId', 'queuedItemId must be a non-empty string');
    }

    const reason = opts.reason;
    const now = Date.now();
    const cancelErrorForReceipt = new HarnessSessionCancelledError(this.id, reason);
    let removed: { queuedItemId: string; admissionId?: string } | undefined;

    await this._flushUpdate(prev => {
      const queue = prev.pendingQueue ?? [];
      const idx = queue.findIndex(item => item.id === targetId);
      const activeResumeId =
        prev.pendingResume !== undefined ? this._queuedItemIdForPendingResume(prev.pendingResume) : undefined;
      if (idx <= 0 || this._currentQueuedItemId === targetId || activeResumeId === targetId) return prev;

      const item = queue[idx]!;
      const receipts = prev.queueAdmissionReceipts ?? {};
      const receipt = receipts[item.id];
      if (
        receipt?.status === 'completed' ||
        receipt?.status === 'failed' ||
        receipt?.status === 'dead' ||
        receipt?.status === 'admission_failed'
      ) {
        return prev;
      }

      removed = { queuedItemId: item.id, admissionId: item.admissionId };
      const next: SessionRecord = {
        ...prev,
        pendingQueue: queue.slice(0, idx).concat(queue.slice(idx + 1)),
      };

      if (receipt) {
        next.queueAdmissionReceipts = {
          ...receipts,
          [item.id]: {
            ...receipt,
            status: 'admission_failed',
            error: projectHarnessPublicError(cancelErrorForReceipt),
            failedAt: receipt.failedAt ?? now,
            updatedAt: now,
          },
        };
      }
      return next;
    });

    if (!removed) return;

    // §10.2: queue-item cancellation is not a public event — the observable
    // effect is the rejected operation promise below + the cleared queue.
    const resolver = this._queueResolvers.get(removed.queuedItemId);
    if (resolver) {
      this._queueResolvers.delete(removed.queuedItemId);
      resolver.reject(new HarnessSessionCancelledError(this.id, reason));
    }
    if ((this._record.pendingQueue?.length ?? 0) > 0) {
      this._scheduleQueueWakeupForPendingQueue();
    } else {
      this._clearQueueWakeTimer();
    }
    this._notifyMaybeIdle();
  }

  /** @internal — emitter epoch (for tests). */
  get _internalEmitterEpoch(): string {
    return this._emitter.epochId;
  }

  // -------------------------------------------------------------------------
  // Identity / inspection — usable in any lifecycle state.
  // -------------------------------------------------------------------------

  /** Last-known `lastActivityAt`. Updated whenever the record is flushed. */
  get lastActivityAt(): number {
    return this._record.lastActivityAt;
  }

  /** Current lifecycle state. */
  get lifecycleState(): SessionLifecycleState {
    return this._state;
  }

  /** True once the session has reached a terminal local state. */
  get isClosed(): boolean {
    return this._state === 'closed' || this._state === 'deleted';
  }

  /** True while close is draining admitted flushes or after the durable closing marker commits. */
  get isClosing(): boolean {
    return this._state === 'closing' || (this._record.closingAt !== undefined && this._record.closedAt === undefined);
  }

  /**
   * True while a pending interaction (tool approval/suspension, question, plan)
   * parks the session, pinning it in memory so pressure/idle eviction skips it
   * (§5.4). The pin lifts when the prompt is answered or the session closes.
   */
  isPinned(): boolean {
    return this._record.pendingResume !== undefined && this._record.cancelRequest === undefined;
  }

  /** Epoch-ms when this session entered closing, if it has. Used to build `HarnessSessionClosingError` (§4.5b). */
  get closingAt(): number | undefined {
    return this._record.closingAt;
  }

  /** Epoch-ms close deadline for this session, if closing. Used to build `HarnessSessionClosingError` (§4.5b). */
  get closeDeadlineAt(): number | undefined {
    return this._record.closeDeadlineAt;
  }

  /** Read-only snapshot of the underlying record. */
  getRecord(): Readonly<SessionRecord> {
    return this._record;
  }

  // -------------------------------------------------------------------------
  // Lifecycle.
  // -------------------------------------------------------------------------

  /**
   * Soft-close: persist `closingAt`, reject new work, drain admitted turns
   * until the close deadline, terminalize descendants, set `closedAt`, release
   * the lease, and drop from the live map. The session remains **reopenable**
   * (§5.3): a later `harness.session({ sessionId })` or `{ threadId, resourceId }`
   * resolve transitions it Closed -> Active. Idempotent: a second call is a
   * no-op once `closed`. The cascade through descendants (§5.5) is driven by
   * the Harness, not by this method directly.
   */
  async close(): Promise<void> {
    if (this._state === 'closed') return;
    await this._harness._closeSession(this);
  }

  /**
   * Session-first delete (§4.1): removes this session and the durable
   * conversation/artifacts it owns, subject to ownership and safety guards.
   * This is the normal product destructive path; it delegates to the Harness
   * so the descendant cascade (§5.5) is enforced in one place. This is the
   * guarded delete: it fails with `HarnessSessionDeleteBlockedError` if the
   * session still has non-terminal dependents. The operator force-delete path
   * is intentionally NOT exposed here (§4.1) — operators call
   * `harness.deleteSession({ force: true })` directly.
   */
  async delete(): Promise<void> {
    await this._harness.deleteSession({ sessionId: this.id, resourceId: this.resourceId });
  }

  /**
   * Session-first rename (§4.1 / §13.3c): update the title of this session's
   * backing thread, authorized through the live session (it holds the write
   * lease). Asserts the session is live first so a closed/closing/deleted
   * session is rejected rather than mutating the thread out from under the
   * session lifecycle; reopen first to rename a closed conversation.
   */
  async rename(opts: { title: string }): Promise<void> {
    this._assertLive('rename()');
    await this._harness._threadOps.rename({ resourceId: this.resourceId, threadId: this.threadId, title: opts.title });
  }

  /**
   * Session-first clone (§4.2 / §2.2): copy this conversation into a new usable
   * session. Copies committed message history and, when `copyAppMetadata` is
   * requested, app-owned thread metadata only. Per §5.2a it copies NO
   * `SessionRecord` (no mode/model/grants/state/pending/run/goal/workspace/
   * channel rows) and never copies the lease, live process handles, active
   * streams, or in-flight tool execution ownership. The new session owns its
   * cloned thread and starts fresh from harness defaults.
   */
  async clone(opts?: { title?: string; copyAppMetadata?: boolean }): Promise<Session> {
    return this._harness._cloneSession(this, opts);
  }

  // -------------------------------------------------------------------------
  // Workspace — §2.7 / §4.2.
  // -------------------------------------------------------------------------

  /**
   * Resolve this session's workspace. Returns `undefined` when the harness
   * has no workspace configured. Caches the result for the lifetime of the
   * Session (the workspace is released on `close()` per the ownership model).
   *
   * Throws {@link HarnessWorkspaceLostError} when the session's
   * `per-session` workspace was provisioned by a non-resumable provider
   * and a process restart has dropped the underlying state. Callers can
   * decide whether to surface the error or call `clearWorkspaceLost()` and
   * try again with a fresh workspace.
   */
  async getWorkspace(): Promise<Workspace | undefined> {
    this._assertLive('getWorkspace()');
    return this._getWorkspaceUnchecked();
  }

  // ---------------------------------------------------------------------------
  // §4.2 / §6.1 workspace access primitives. These mirror the tool-facing
  // `HarnessRequestContext` accessors. The split lets a turn avoid cloud-sandbox
  // cold starts: `hasWorkspace()`/`isWorkspaceReady()`/`peekWorkspace()` are
  // sync, NON-materializing reads; `resolveWorkspace()` is the explicit async
  // materialize/resume path. NOTE: the legacy `getWorkspace()` above is async +
  // materializing (kept for its established callers/tests); `resolveWorkspace()`
  // is its spec-named alias. (Migrating `getWorkspace()` itself to a sync read is
  // a separate, larger §4.2 reconciliation — ~25 call sites — left as follow-up.)
  // ---------------------------------------------------------------------------

  /** §6.1: is a workspace CONFIGURED for this harness (regardless of whether it
   * has been materialized)? Sync, non-materializing. */
  hasWorkspace(): boolean {
    return this._harness._workspaceKind !== undefined;
  }

  /** §6.1: is a usable workspace handle already materialized (warm/cached)? Sync,
   * non-materializing — `false` does not mean "no workspace", only "not yet
   * resolved" (or lost). A workspace marked lost is NOT ready even if a stale
   * handle lingers. */
  isWorkspaceReady(): boolean {
    return this._workspace !== undefined && !this._workspaceLost;
  }

  /** §6.1: the already-materialized workspace handle, or `undefined` if it has not
   * been resolved yet (or was lost). Sync and NON-materializing — never triggers a
   * cold start and never throws; `resolveWorkspace()` surfaces a lost workspace. */
  peekWorkspace(): Workspace | undefined {
    return this._workspaceLost ? undefined : this._workspace;
  }

  /** §6.1: explicitly materialize/resume the workspace (a cloud sandbox cold start
   * may happen here). Throws `HarnessWorkspaceLostError` if the workspace was lost
   * across restart, or a validation error if no workspace is configured. */
  async resolveWorkspace(): Promise<Workspace> {
    const workspace = await this.getWorkspace();
    if (workspace === undefined) {
      throw new HarnessValidationError(
        'resolveWorkspace()',
        'no workspace is configured for this harness (set `workspace.kind` in HarnessConfig)',
      );
    }
    return workspace;
  }

  private async _getWorkspaceUnchecked(): Promise<Workspace | undefined> {
    if (this._workspace) return this._workspace;
    if (this._workspaceResolving) return this._workspaceResolving;

    const kind = this._harness._workspaceKind;
    if (!kind) return undefined;

    if (this._workspaceLost) {
      throw new HarnessWorkspaceLostError(this.id, {
        reason: 'restart',
        providerId: this._harness._workspaceRegistry.providerId,
      });
    }

    const resolve = async (): Promise<Workspace> => {
      if (kind === 'shared') {
        return this._harness._workspaceRegistry.acquireShared();
      }
      if (kind === 'per-resource') {
        return this._harness._workspaceRegistry.acquirePerResource({ resourceId: this.resourceId });
      }
      // kind === 'per-session'
      // Subagent sessions with `workspace: 'inherit'` reuse the parent's entry.
      // The spawn tool flips `_subagentFreshWorkspace` for the `fresh` case so
      // we don't accidentally inherit when a fresh workspace is requested.
      if (this.parentSessionId && this._subagentInheritWorkspace) {
        // §6.2 strict-lazy: a lazy parent turn may not have materialized its
        // workspace yet, but inheriting requires the parent's entry to exist.
        // Resolve the (live) parent's workspace first (idempotent).
        await this._harness._internalEnsureParentWorkspaceForInherit(this.parentSessionId);
        return this._harness._workspaceRegistry.inheritPerSession({
          parentSessionId: this.parentSessionId,
          childSessionId: this.id,
          resourceId: this.resourceId,
        });
      }
      const storedProviderId = this._record.workspace?.providerId;
      const storedState = this._record.workspace?.state;
      return this._harness._workspaceRegistry.acquirePerSession({
        resourceId: this.resourceId,
        sessionId: this.id,
        ...(this.parentSessionId ? { parentSessionId: this.parentSessionId } : {}),
        ...(storedProviderId ? { storedProviderId } : {}),
        ...(storedState !== undefined ? { storedState } : {}),
        onStateUpdate: async state => {
          await this._persistWorkspaceState(state);
        },
      });
    };

    this._workspaceResolving = resolve();
    try {
      this._workspace = await this._workspaceResolving;
      return this._workspace;
    } finally {
      this._workspaceResolving = undefined;
    }
  }

  /**
   * @internal — used by the harness during hydration when the stored record
   * carries workspace state but the configured provider is non-resumable.
   */
  _markWorkspaceLost(): void {
    this._workspaceLost = true;
  }

  /**
   * @internal — set by the spawn-subagent tool flow to indicate the child
   * session should inherit its parent's workspace rather than provisioning
   * a fresh one. `undefined` for top-level sessions; defaults to `true` for
   * subagent sessions unless the spawn definition opts into `'fresh'`.
   */
  _subagentInheritWorkspace?: boolean;

  /** @internal — writes the latest opaque workspace state into the session record. */
  private async _persistWorkspaceState(state: unknown): Promise<void> {
    const providerId = this._harness._workspaceRegistry.providerId;
    if (!providerId) return;
    await this._flushUpdate(record => ({
      ...record,
      workspace: { providerId, state },
    }));
  }

  // -------------------------------------------------------------------------
  // Skills — §4.6.
  //
  // Code-registered skills are merged ahead of workspace-discovered skills.
  // Workspace-discovered entries are projected from the configured
  // `WorkspaceSkills` source into `HarnessSkill` descriptors. Discovery runs
  // asynchronously on the first `list` / `get` / `use` call per in-memory
  // Session instance and the merged result is cached for the session's
  // lifetime. Concurrent calls during a generation share a single-flight
  // promise. `refresh()` drops the cache so the next call re-runs discovery.
  //
  // `use(ref, opts?)` resolves a code-registered or workspace skill by name
  // or relative path, validates declared args, appends a JSON code block
  // carrying the validated args to the skill body, and delegates to the
  // signal-driven message path. The returned `AgentResult` is the underlying
  // turn's result.
  // -------------------------------------------------------------------------

  /**
   * Skill discovery, inspection, and programmatic execution — see §4.6
   * and §4.2c.
   *
   * Code-registered skills are merged ahead of workspace-discovered skills.
   * Workspace-discovered skills are projected into `HarnessSkill`
   * descriptors. Discovery runs asynchronously on the first `list` /
   * `get` / `use` call per in-memory Session instance and is cached for
   * the session's lifetime. Concurrent callers share a single-flight
   * promise. `refresh()` drops the cache so the next call re-runs
   * discovery.
   */
  readonly skills = Object.freeze({
    /**
     * List skills available to this session.
     *
     * Returns code-registered skills plus workspace-discovered skills.
     * If the session has no workspace configured, only code-registered
     * skills are returned.
     */
    list: (): Promise<HarnessSkill[]> => this._skillsList(),
    /**
     * Look up a skill by name. Returns `undefined` when the name does not
     * resolve in the code or workspace catalogues.
     */
    get: (name: string): Promise<HarnessSkill | undefined> => this._skillsGet(name),
    /**
     * Drop the cached workspace-discovery result. The next `list` / `get`
     * / `use` call re-runs discovery through the configured workspace
     * skill source. Local-only — absent from `RemoteSession` (§13.5).
     */
    refresh: (): Promise<void> => this._skillsRefresh(),
    /**
     * Resolve a code-registered skill by name, or a workspace skill by name
     * or relative path, optionally validate provided arguments against the
     * skill's declared args schema, append a JSON code block of the validated
     * args to the skill instructions, and dispatch the result
     * through the signal-driven message path as a single turn. Resolves
     * to the underlying turn's `AgentResult`.
     *
     * Throws {@link HarnessSkillNotFoundError} when `ref` does not match
     * any skill, and {@link HarnessSkillArgsValidationError} when declared
     * args are invalid.
     */
    use: (ref: string, opts?: UseSkillOptions): Promise<AgentResult> => this._skillsUse(ref, opts),
  });

  // -------------------------------------------------------------------------
  // MCP catalog — PF-562 / PF-552 desktop integration inventory.
  //
  // This is an inventory snapshot over MCP servers registered on the Harness
  // Mastra instance. Tool descriptors use MCPServerBase.getToolListInfo() so
  // lazy MCP client proxies can populate remote tool catalogs on demand.
  // Execution permission remains enforced by the MCP tool runtime; this
  // catalog only lets desktop hosts render integrations.
  // -------------------------------------------------------------------------

  readonly mcp = Object.freeze({
    /** List MCP servers registered on the Harness Mastra instance. */
    listServers: (): HarnessMcpServerDescriptor[] => this._mcpListServers(),
    /** Look up one MCP server by Mastra registration key. */
    getServer: (key: string): HarnessMcpServerDescriptor | undefined => this._mcpGetServer(key),
    /** List MCP tool descriptors for one registered server key. */
    listTools: (key: string): Promise<HarnessMcpToolDescriptor[] | undefined> => {
      this._assertLive('mcp.listTools()');
      this._assertMcpServerKey('mcp.listTools()', key);
      return this._mcpListTools(key);
    },
  });

  // -------------------------------------------------------------------------
  // Action catalog — PF-576 / PF-552 desktop palette inventory.
  //
  // This is a read-only aggregate over skill action metadata and MCP tool
  // descriptors. It intentionally exposes no execution or lifecycle controls;
  // callers use the source surfaces referenced by each entry.
  // -------------------------------------------------------------------------

  readonly actions = Object.freeze({
    /** List/search local desktop action catalog entries. */
    list: (options?: HarnessActionCatalogListOptions): Promise<HarnessActionCatalogEntry[]> =>
      this._actionsList(options),
    /** Convenience wrapper for `list({ query, ...options })`. */
    search: (
      query: string,
      options?: Omit<HarnessActionCatalogListOptions, 'query'>,
    ): Promise<HarnessActionCatalogEntry[]> => this._actionsSearch(query, options),
    /** Refresh already-materialized workspace skills and clear cached action catalog discovery. */
    refresh: (): Promise<void> => this._actionsRefresh(),
  });

  private _mcpListServers(): HarnessMcpServerDescriptor[] {
    this._assertLive('mcp.listServers()');
    return this._harness._listMcpServers().map(([key, server]) => this._projectMcpServer(key, server));
  }

  private _mcpGetServer(key: string): HarnessMcpServerDescriptor | undefined {
    this._assertLive('mcp.getServer()');
    this._assertMcpServerKey('mcp.getServer()', key);
    const server = this._harness._getMcpServer(key);
    return server ? this._projectMcpServer(key, server) : undefined;
  }

  private async _mcpListTools(
    key: string,
    abortSignal: AbortSignal = new AbortController().signal,
    opts: { resolveWorkspace?: boolean } = {},
  ): Promise<HarnessMcpToolDescriptor[] | undefined> {
    const server = this._harness._getMcpServer(key);
    if (!server) return undefined;
    const requestContext = await this._buildRequestContext({
      modeId: this._record.modeId,
      modelId: this._record.modelId,
      abortSignal,
      resolveWorkspace: opts.resolveWorkspace,
    });
    const toolList = await server.getToolListInfo(requestContext);
    const convertedTools = server.tools();
    return toolList.tools.map(toolInfo => {
      const infoWithId = toolInfo as typeof toolInfo & { id?: unknown };
      const toolName = typeof infoWithId.id === 'string' && infoWithId.id.length > 0 ? infoWithId.id : toolInfo.name;
      const convertedTool = convertedTools[toolName];
      const inputSchema = cloneMcpSchemaLike(toolInfo.inputSchema ?? convertedTool?.parameters);
      const outputSchema = cloneMcpSchemaLike(toolInfo.outputSchema ?? convertedTool?.outputSchema);
      const meta = cloneMcpCatalogRecord(toolInfo._meta ?? convertedTool?.mcp?._meta);
      const toolType = toolInfo.toolType ?? convertedTool?.mcp?.toolType;
      return {
        serverKey: key,
        name: toolName,
        ...(toolInfo.description ? { description: toolInfo.description } : {}),
        ...(inputSchema !== undefined ? { inputSchema } : {}),
        ...(outputSchema !== undefined ? { outputSchema } : {}),
        ...(toolType ? { toolType } : {}),
        ...(meta ? { meta } : {}),
        ...(convertedTool?.strict !== undefined ? { strict: convertedTool.strict } : {}),
      };
    });
  }

  private _assertMcpServerKey(method: string, key: unknown): asserts key is string {
    if (typeof key !== 'string' || key.length === 0) {
      throw new HarnessValidationError(method, 'key must be a non-empty string');
    }
    if (RESERVED_MCP_SERVER_KEYS.has(key)) {
      throw new HarnessValidationError(method, 'key must not be a reserved object property name');
    }
  }

  private _projectMcpServer(
    key: string,
    server: NonNullable<ReturnType<Harness['_getMcpServer']>>,
  ): HarnessMcpServerDescriptor {
    const repository = cloneMcpCatalogRecord(server.repository);
    const packages = cloneMcpCatalogRecordArray(server.packages);
    const remotes = cloneMcpCatalogRecordArray(server.remotes);
    return {
      key,
      id: server.id,
      name: server.name,
      version: server.version,
      ...(server.description ? { description: server.description } : {}),
      ...(server.instructions ? { instructions: server.instructions } : {}),
      releaseDate: server.releaseDate,
      isLatest: server.isLatest,
      ...(repository ? { repository } : {}),
      ...(server.packageCanonical ? { packageCanonical: server.packageCanonical } : {}),
      ...(packages ? { packages } : {}),
      ...(remotes ? { remotes } : {}),
    };
  }

  private async _actionsSearch(
    query: string,
    options?: Omit<HarnessActionCatalogListOptions, 'query'>,
  ): Promise<HarnessActionCatalogEntry[]> {
    this._assertLive('actions.search()');
    if (typeof query !== 'string') {
      throw new HarnessValidationError('actions.search()', 'query must be a string');
    }
    if (options !== undefined && (!options || typeof options !== 'object' || Array.isArray(options))) {
      throw new HarnessValidationError('actions.search()', 'options must be an object');
    }
    return this._actionsList({ ...options, query });
  }

  private async _actionsList(options?: HarnessActionCatalogListOptions): Promise<HarnessActionCatalogEntry[]> {
    this._assertLive('actions.list()');
    const normalized = this._normalizeActionCatalogOptions(options);
    if (normalized.limit === 0) return [];
    const entries = await this._buildActionCatalogEntries(normalized.source);
    const filtered = entries.filter(entry => this._matchesActionCatalogOptions(entry, normalized));
    return filtered
      .sort((a, b) => {
        const sourceOrder = ACTION_CATALOG_SOURCE_ORDER[a.source.kind] - ACTION_CATALOG_SOURCE_ORDER[b.source.kind];
        return sourceOrder === 0 ? compareActionCatalogIds(a.id, b.id) : sourceOrder;
      })
      .slice(normalized.offset, normalized.offset + normalized.limit)
      .map(entry => cloneActionCatalogEntry(entry));
  }

  private async _actionsRefresh(): Promise<void> {
    this._assertLive('actions.refresh()');
    this._clearSkillAndActionCatalogCaches();
    try {
      await this._workspace?.skills?.refresh();
    } finally {
      this._clearSkillAndActionCatalogCaches();
    }
  }

  private _clearSkillAndActionCatalogCaches(): void {
    this._skillsCache = undefined;
    this._skillsResolving = undefined;
    this._actionsSkillEntriesCache = undefined;
    this._actionsSkillEntriesResolving = undefined;
    this._actionsMcpEntriesCacheByServer.clear();
    this._actionsMcpEntriesResolvingByServer.clear();
    this._actionsMcpTimedOutWorkByServer.clear();
    this._actionsMcpCatalogGeneration++;
  }

  private _normalizeActionCatalogOptions(options?: HarnessActionCatalogListOptions): {
    query: string;
    source?: HarnessActionCatalogSourceKind;
    limit: number;
    offset: number;
  } {
    if (options !== undefined && (!options || typeof options !== 'object' || Array.isArray(options))) {
      throw new HarnessValidationError('actions.list()', 'options must be an object');
    }
    const query = options?.query ?? '';
    if (typeof query !== 'string') {
      throw new HarnessValidationError('actions.list()', 'query must be a string');
    }
    const source = options?.source;
    if (source !== undefined && !ACTION_CATALOG_SOURCE_KINDS.includes(source)) {
      throw new HarnessValidationError(
        'actions.list()',
        `source must be one of ${ACTION_CATALOG_SOURCE_KINDS.join(' | ')}`,
      );
    }
    const limit = options?.limit ?? ACTION_CATALOG_DEFAULT_LIMIT;
    if (!Number.isInteger(limit) || limit < 0 || limit > ACTION_CATALOG_MAX_LIMIT) {
      throw new HarnessValidationError(
        'actions.list()',
        `limit must be an integer between 0 and ${ACTION_CATALOG_MAX_LIMIT}`,
      );
    }
    const offset = options?.offset ?? 0;
    if (!Number.isInteger(offset) || offset < 0) {
      throw new HarnessValidationError('actions.list()', 'offset must be a non-negative integer');
    }
    return { query: query.trim().toLowerCase(), source, limit, offset };
  }

  private _matchesActionCatalogOptions(
    entry: HarnessActionCatalogEntry,
    options: { query: string; source?: HarnessActionCatalogSourceKind; limit: number; offset: number },
  ): boolean {
    if (options.source !== undefined && entry.source.kind !== options.source) return false;
    if (options.query.length === 0) return true;
    const haystack = this._actionCatalogSearchText(entry).toLowerCase();
    return haystack.includes(options.query);
  }

  private _actionCatalogSearchText(entry: HarnessActionCatalogEntry): string {
    const parts = [entry.id, entry.label, entry.description, entry.category, entry.status, entry.statusReason];
    if (entry.source.kind === 'skill') {
      parts.push(entry.source.skillName, entry.source.filePath);
    } else if (entry.source.kind === 'mcp-server') {
      parts.push(entry.source.serverKey, entry.mcp?.serverName);
    } else {
      parts.push(entry.source.serverKey, entry.source.toolName, entry.mcp?.serverName);
    }
    if (entry.shortcuts) {
      for (const shortcut of entry.shortcuts) {
        parts.push(shortcut.id, shortcut.label, ...(shortcut.keys ?? []));
      }
    }
    return parts.filter((part): part is string => typeof part === 'string' && part.length > 0).join('\n');
  }

  private async _buildActionCatalogEntries(
    source?: HarnessActionCatalogSourceKind,
  ): Promise<HarnessActionCatalogEntry[]> {
    const skillEntries =
      source === 'mcp-tool' || source === 'mcp-server' ? [] : await this._resolveSkillActionCatalogEntries();
    const mcpEntries: HarnessActionCatalogEntry[] = [];
    if (source !== 'skill') {
      mcpEntries.push(...(await this._resolveMcpActionCatalogEntries()));
    }
    return [...skillEntries, ...mcpEntries];
  }

  private async _resolveSkillActionCatalogEntries(): Promise<HarnessActionCatalogEntry[]> {
    if (this._actionsSkillEntriesCache) return this._actionsSkillEntriesCache;
    if (this._actionsSkillEntriesResolving) return this._actionsSkillEntriesResolving;

    const build = async (): Promise<HarnessActionCatalogEntry[]> => {
      const codeSkills = this._harness._listCodeSkills();
      const entries: ActionCatalogSkillDescriptor[] = [...codeSkills];
      let workspace: Workspace | undefined;
      try {
        workspace = await this.getWorkspace();
      } catch (error) {
        if (!(error instanceof HarnessWorkspaceLostError)) {
          throw error;
        }
      }
      const workspaceSkills = workspace?.skills;
      if (workspaceSkills) {
        const workspaceEntries = await workspaceSkills.list();
        const codeActionNames = new Set(
          codeSkills.filter(skill => this._projectSkillActionCatalogEntry(skill).length > 0).map(skill => skill.name),
        );
        for (const meta of workspaceEntries) {
          if (codeActionNames.has(meta.name) && !meta.path) continue;
          entries.push({
            name: meta.name,
            description: meta.description,
            ...(meta.path ? { filePath: meta.path } : {}),
            ...(meta.metadata ? { metadata: meta.metadata } : {}),
          });
        }
      }
      return entries.flatMap(skill => this._projectSkillActionCatalogEntry(skill));
    };

    const pending = build();
    this._actionsSkillEntriesResolving = pending;
    try {
      const result = await pending;
      if (this._actionsSkillEntriesResolving === pending) {
        this._actionsSkillEntriesCache = result;
      }
      return result;
    } finally {
      if (this._actionsSkillEntriesResolving === pending) {
        this._actionsSkillEntriesResolving = undefined;
      }
    }
  }

  private _projectSkillActionCatalogEntry(skill: ActionCatalogSkillDescriptor): HarnessActionCatalogEntry[] {
    const action = skill.action ?? cloneActionMetadataLike(skill.metadata?.action);
    if (!action) return [];
    const idSource = skill.filePath ?? skill.name;
    return [
      {
        id: `skill:${encodeActionCatalogIdPart(idSource)}`,
        source: {
          kind: 'skill',
          ref: skill.filePath ?? skill.name,
          skillName: skill.name,
          ...(skill.filePath ? { filePath: skill.filePath } : {}),
        },
        status: 'available',
        label: action.displayName ?? skill.name,
        description: skill.description,
        ...(skill.category ? { category: skill.category } : {}),
        ...(action.icon ? { icon: action.icon } : {}),
        ...(action.shortcuts
          ? { shortcuts: cloneMcpCatalogValue(action.shortcuts) as HarnessActionCatalogEntry['shortcuts'] }
          : {}),
        ...(action.inputSchema ? { inputSchema: cloneMcpCatalogValue(action.inputSchema) } : {}),
        ...(action.outputSchema ? { outputSchema: cloneMcpCatalogValue(action.outputSchema) } : {}),
        ...(action.artifactTypes ? { artifactTypes: [...action.artifactTypes] } : {}),
        ...(action.permissions
          ? { permissions: cloneMcpCatalogValue(action.permissions) as HarnessActionCatalogEntry['permissions'] }
          : {}),
      },
    ];
  }

  private _projectMcpToolActionCatalogEntry(
    server: HarnessMcpServerDescriptor,
    tool: HarnessMcpToolDescriptor,
  ): HarnessActionCatalogEntry {
    return {
      id: `mcp-tool:${encodeActionCatalogIdPart(server.key)}:${encodeActionCatalogIdPart(tool.name)}`,
      source: { kind: 'mcp-tool', serverKey: server.key, toolName: tool.name },
      status: 'available',
      label: tool.name,
      ...(tool.description ? { description: tool.description } : {}),
      ...(tool.inputSchema !== undefined ? { inputSchema: cloneMcpCatalogValue(tool.inputSchema) } : {}),
      ...(tool.outputSchema !== undefined ? { outputSchema: cloneMcpCatalogValue(tool.outputSchema) } : {}),
      permissions: { mcpScopes: [server.key] },
      mcp: {
        serverName: server.name,
        serverVersion: server.version,
        ...(tool.toolType ? { toolType: tool.toolType } : {}),
        ...(tool.strict !== undefined ? { strict: tool.strict } : {}),
        ...(tool.meta ? { meta: cloneMcpCatalogValue(tool.meta) as Record<string, unknown> } : {}),
      },
    };
  }

  private _projectUnavailableMcpActionCatalogEntry(
    server: HarnessMcpServerDescriptor,
    reason: HarnessActionCatalogUnavailableReason,
  ): HarnessActionCatalogEntry {
    const statusMessage =
      reason === 'mcp_tool_catalog_timeout'
        ? 'MCP tool catalog timed out'
        : reason === 'mcp_tool_catalog_retry_suppressed'
          ? 'MCP tool catalog retry delayed'
          : 'MCP tool catalog unavailable';
    return {
      id: `mcp-server:${encodeActionCatalogIdPart(server.key)}`,
      source: { kind: 'mcp-server', serverKey: server.key },
      status: 'unavailable',
      statusReason: reason,
      statusMessage,
      label: server.name,
      ...(server.description ? { description: server.description } : {}),
      permissions: { mcpScopes: [server.key] },
      mcp: {
        serverName: server.name,
        serverVersion: server.version,
      },
    };
  }

  private _actionMcpCatalogCacheKey(server: HarnessMcpServerDescriptor, workspaceId: string): string {
    return [server.key, this._record.modeId, this._record.modelId ?? '', workspaceId].join('\0');
  }

  private _currentMcpActionCatalogWorkspaceId(): string {
    // MCP catalog reads pass `resolveWorkspace: false`; key only by a workspace
    // that was already materialized so catalog reads never provision one.
    return this._workspace?.id ?? '';
  }

  private _getCachedMcpActionCatalogEntries(cacheKey: string): HarnessActionCatalogEntry[] | undefined {
    const cached = this._actionsMcpEntriesCacheByServer.get(cacheKey);
    if (!cached) return undefined;
    if (cached.expiresAt === undefined || cached.expiresAt > Date.now()) {
      return cached.entries;
    }
    this._actionsMcpEntriesCacheByServer.delete(cacheKey);
    return undefined;
  }

  private async _resolveMcpActionCatalogEntries(): Promise<HarnessActionCatalogEntry[]> {
    const results = await Promise.all(
      this._mcpListServers().map(server => this._resolveMcpActionCatalogEntriesForServer(server)),
    );
    return results.flat();
  }

  private _startMcpActionCatalogEntriesForServer(
    server: HarnessMcpServerDescriptor,
    cacheKey: string,
    retryCount = 0,
  ): Promise<HarnessActionCatalogEntry[]> {
    const catalogGeneration = this._actionsMcpCatalogGeneration;
    let pending: Promise<HarnessActionCatalogEntry[]>;
    const started = startActionCatalogMcpListWithTimeout(
      async abortSignal => {
        const tools = await this._mcpListTools(server.key, abortSignal, { resolveWorkspace: false });
        return (tools ?? []).map(tool => this._projectMcpToolActionCatalogEntry(server, tool));
      },
      work => {
        if (this._actionsMcpEntriesResolvingByServer.get(cacheKey) === pending) {
          this._actionsMcpTimedOutWorkByServer.set(cacheKey, {
            work,
            retryAfter: Date.now() + ACTION_CATALOG_MCP_FAILURE_CACHE_MS,
            retryCount,
          });
        }
      },
    );
    pending = started.pending;
    const { work, didTimeout } = started;
    pending.catch(() => {
      // `pending` is observed below for cache/single-flight cleanup; attach
      // this noop handler immediately so timeout rejection is never unhandled.
    });
    this._actionsMcpEntriesResolvingByServer.set(cacheKey, pending);

    pending
      .then(entries => {
        if (this._actionsMcpEntriesResolvingByServer.get(cacheKey) === pending) {
          this._actionsMcpEntriesCacheByServer.set(cacheKey, {
            entries,
            expiresAt: Date.now() + ACTION_CATALOG_MCP_SUCCESS_CACHE_MS,
            successful: true,
          });
          this._actionsMcpEntriesResolvingByServer.delete(cacheKey);
        }
      })
      .catch(error => {
        if (this._actionsMcpEntriesResolvingByServer.get(cacheKey) !== pending) return;
        const timeoutWorkStillTracked = this._actionsMcpTimedOutWorkByServer.get(cacheKey)?.work === work;
        const didCatalogTimeout =
          error instanceof ActionCatalogMcpListTimeoutError || (didTimeout() && timeoutWorkStillTracked);
        const existingCache = this._actionsMcpEntriesCacheByServer.get(cacheKey);
        const hasSuccessfulTimedOutResult = existingCache?.successful === true;
        if ((!didCatalogTimeout || timeoutWorkStillTracked) && !hasSuccessfulTimedOutResult) {
          const reason: HarnessActionCatalogUnavailableReason = didCatalogTimeout
            ? 'mcp_tool_catalog_timeout'
            : 'mcp_tool_catalog_failed';
          this._actionsMcpEntriesCacheByServer.set(cacheKey, {
            entries: [this._projectUnavailableMcpActionCatalogEntry(server, reason)],
            expiresAt: Date.now() + ACTION_CATALOG_MCP_FAILURE_CACHE_MS,
          });
        }
        this._actionsMcpEntriesResolvingByServer.delete(cacheKey);
      });

    work
      .then(entries => {
        if (didTimeout() && this._actionsMcpCatalogGeneration === catalogGeneration) {
          this._actionsMcpEntriesCacheByServer.set(cacheKey, {
            entries,
            expiresAt: Date.now() + ACTION_CATALOG_MCP_SUCCESS_CACHE_MS,
            successful: true,
          });
        }
      })
      .catch(() => {
        if (didTimeout() && this._actionsMcpTimedOutWorkByServer.get(cacheKey)?.work === work) {
          this._actionsMcpEntriesCacheByServer.set(cacheKey, {
            entries: [this._projectUnavailableMcpActionCatalogEntry(server, 'mcp_tool_catalog_timeout')],
            expiresAt: Date.now() + ACTION_CATALOG_MCP_FAILURE_CACHE_MS,
          });
        }
      })
      .finally(() => {
        if (didTimeout() && this._actionsMcpTimedOutWorkByServer.get(cacheKey)?.work === work) {
          this._actionsMcpTimedOutWorkByServer.delete(cacheKey);
        }
      });

    return pending;
  }

  private async _resolveMcpActionCatalogEntriesForServer(
    server: HarnessMcpServerDescriptor,
  ): Promise<HarnessActionCatalogEntry[]> {
    const cacheKey = this._actionMcpCatalogCacheKey(server, this._currentMcpActionCatalogWorkspaceId());
    const cached = this._getCachedMcpActionCatalogEntries(cacheKey);
    if (cached) return cached;

    const timedOutWork = this._actionsMcpTimedOutWorkByServer.get(cacheKey);
    if (
      timedOutWork &&
      (timedOutWork.retryAfter > Date.now() || timedOutWork.retryCount >= ACTION_CATALOG_MCP_MAX_TIMEOUT_RETRIES)
    ) {
      const entries = [this._projectUnavailableMcpActionCatalogEntry(server, 'mcp_tool_catalog_retry_suppressed')];
      this._actionsMcpEntriesCacheByServer.set(cacheKey, {
        entries,
        expiresAt:
          timedOutWork.retryAfter > Date.now()
            ? timedOutWork.retryAfter
            : Date.now() + ACTION_CATALOG_MCP_FAILURE_CACHE_MS,
      });
      return entries;
    }
    if (timedOutWork) {
      this._actionsMcpTimedOutWorkByServer.delete(cacheKey);
    }

    const pending =
      this._actionsMcpEntriesResolvingByServer.get(cacheKey) ??
      this._startMcpActionCatalogEntriesForServer(server, cacheKey, (timedOutWork?.retryCount ?? -1) + 1);
    try {
      return await pending;
    } catch (error) {
      const reason: HarnessActionCatalogUnavailableReason =
        error instanceof ActionCatalogMcpListTimeoutError ? 'mcp_tool_catalog_timeout' : 'mcp_tool_catalog_failed';
      return [this._projectUnavailableMcpActionCatalogEntry(server, reason)];
    }
  }

  private async _skillsList(): Promise<HarnessSkill[]> {
    this._assertLive('skills.list()');
    return this._resolveSkills();
  }

  private async _skillsGet(name: string): Promise<HarnessSkill | undefined> {
    this._assertLive('skills.get()');
    if (typeof name !== 'string' || name.length === 0) {
      throw new HarnessValidationError('skills.get()', 'name must be a non-empty string');
    }
    const codeSkill = this._harness._getCodeSkill(name);
    if (codeSkill) return codeSkill;
    const skills = await this._resolveSkills();
    return skills.find(s => s.name === name);
  }

  private async _skillsRefresh(): Promise<void> {
    this._assertLive('skills.refresh()');
    this._clearSkillAndActionCatalogCaches();
    try {
      await this._workspace?.skills?.refresh();
    } finally {
      this._clearSkillAndActionCatalogCaches();
    }
  }

  /**
   * Resolve a code-registered skill by name, or a workspace skill by name or
   * relative path, validate any declared args schema, inject the validated
   * args as a JSON code block into the skill instructions, and dispatch the
   * result through `message()` as a single turn. Returns the underlying
   * `AgentResult`.
   *
   * Reference resolution mirrors Flue's workspace `session.skill(ref, ...)`
   * behavior for explicit workspace paths, then checks the static code
   * registry by name, then falls back to workspace skill names. A code skill
   * owns its name, but does not hide an otherwise shadowed workspace skill's
   * explicit path reference.
   */
  private _isExplicitWorkspaceSkillRef(ref: string): boolean {
    return (
      ref.includes('/') || ref.startsWith('./') || ref.startsWith('../') || /\.(?:md|mdx|txt|yaml|yml)$/i.test(ref)
    );
  }

  private async _skillsUse(ref: string, opts?: UseSkillOptions): Promise<AgentResult> {
    this._assertLive('skills.use()');
    if (typeof ref !== 'string' || ref.length === 0) {
      throw new HarnessValidationError('skills.use()', 'ref must be a non-empty string');
    }
    // §3: useSkill is a fail-fast operation — it must refuse on a busy session
    // rather than interleaving/queuing like a default message.
    if (this.isBusy()) {
      throw new HarnessBusyError(this.id, this._busyReason());
    }

    // §4.4c: validate caller request context before skill resolution so a bad
    // app bag fails fast (attributed to skills.use()); the normalized value is
    // forwarded to the delegated message() turn (which re-validates idempotently).
    const callerRequestContext = validateCallerRequestContext(opts?.requestContext, 'skills.use()');

    const tryWorkspaceSkill = async () => {
      const workspace = await this.getWorkspace();
      const skills = workspace?.skills;
      return skills ? skills.get(ref) : undefined;
    };

    if (this._isExplicitWorkspaceSkillRef(ref)) {
      const skill = await tryWorkspaceSkill();
      if (skill) {
        const args = opts?.args;
        this._validateSkillArgs(skill.name, skill.metadata, args);
        const expandedContent = this._buildSkillPrompt(skill.instructions, args);
        return this.message({
          content: expandedContent,
          ...(opts?.modelOverride ? { model: opts.modelOverride } : {}),
          ...(callerRequestContext ? { requestContext: callerRequestContext } : {}),
        });
      }
    }

    const codeSkill = this._harness._getCodeSkill(ref);
    if (codeSkill) {
      this._validateSkillArgs(codeSkill.name, codeSkill.metadata, opts?.args);
      const expandedContent = this._buildSkillPrompt(codeSkill.instructions, opts?.args);
      return this.message({
        content: expandedContent,
        ...(opts?.modelOverride ? { model: opts.modelOverride } : {}),
        ...(callerRequestContext ? { requestContext: callerRequestContext } : {}),
      });
    }

    // Force workspace materialization. Unlike `list` / `get`, `use` must
    // produce a definitive answer (start a turn or refuse with a typed
    // not-found error).
    const workspace = await this.getWorkspace();
    const skills = workspace?.skills;
    if (!skills) {
      throw new HarnessSkillNotFoundError(ref, ['code-registered']);
    }

    // `WorkspaceSkills.get` accepts either the frontmatter `name` or a
    // relative path under the configured skill source.
    const skill = await skills.get(ref);
    if (!skill) {
      throw new HarnessSkillNotFoundError(ref, ['code-registered', 'workspace']);
    }
    const args = opts?.args;
    this._validateSkillArgs(skill.name, skill.metadata, args);

    // Build the expanded prompt: skill instructions + (optional) JSON
    // code block carrying validated args. Skill authors reference the
    // args naturally in Markdown.
    const expandedContent = this._buildSkillPrompt(skill.instructions, args);

    return this.message({
      content: expandedContent,
      ...(opts?.modelOverride ? { model: opts.modelOverride } : {}),
      ...(callerRequestContext ? { requestContext: callerRequestContext } : {}),
    });
  }

  /**
   * Validate `metadata.args` as a small JSON-schema-ish object. The harness
   * supports the common prompt-arg fields used by workspace frontmatter:
   * `required`, `properties`, `type`, `enum`, `items`, and
   * `additionalProperties`. Unsupported or malformed schema shapes fail
   * before a skill turn starts.
   */
  private _validateSkillArgs(
    skillName: string,
    metadata: Record<string, unknown> | undefined,
    args: Record<string, unknown> | undefined,
  ): void {
    if (args !== undefined && (!args || typeof args !== 'object' || Array.isArray(args))) {
      throw new HarnessSkillArgsValidationError(skillName, ['args must be an object']);
    }

    const issues: string[] = [];
    if (args !== undefined) {
      this._validateJsonSerializableSkillArg('$', args, new WeakSet(), issues);
    }
    if (!metadata || typeof metadata !== 'object') {
      if (issues.length > 0) throw new HarnessSkillArgsValidationError(skillName, issues);
      return;
    }
    const argsField = (metadata as Record<string, unknown>).args;
    if (argsField === undefined) {
      if (issues.length > 0) throw new HarnessSkillArgsValidationError(skillName, issues);
      return;
    }
    if (!this._isPlainRecord(argsField)) {
      throw new HarnessSkillArgsValidationError(skillName, ['unsupported args schema: expected object']);
    }

    const issueCountBeforeSchemaShape = issues.length;
    this._validateSkillArgSchemaShape('$', argsField, issues, new WeakSet());
    if (issues.length > issueCountBeforeSchemaShape) {
      throw new HarnessSkillArgsValidationError(skillName, issues);
    }

    this._validateSkillArgSchemaValue('$', args ?? {}, argsField, issues);
    if (issues.length > 0) {
      throw new HarnessSkillArgsValidationError(skillName, issues);
    }
  }

  private _validateSkillArgSchemaShape(
    path: string,
    schema: Record<string, unknown>,
    issues: string[],
    seen: WeakSet<object>,
  ): void {
    if (seen.has(schema)) {
      issues.push(`${path} must not contain circular args schema references`);
      return;
    }
    seen.add(schema);

    for (const key of Object.keys(schema)) {
      if (!SUPPORTED_SKILL_ARG_SCHEMA_KEYS.has(key)) {
        issues.push(`${path}.${key} is not a supported args schema field`);
      }
    }

    const required = schema.required;
    if (
      required !== undefined &&
      (!Array.isArray(required) || required.some(k => typeof k !== 'string' || k.length === 0))
    ) {
      issues.push(`${path}.required must be an array of non-empty strings`);
    }

    const enumValues = schema.enum;
    if (enumValues !== undefined) {
      if (!Array.isArray(enumValues)) {
        issues.push(`${path}.enum must be an array`);
      } else {
        enumValues.forEach((candidate, index) => {
          this._validateJsonSerializableSkillArg(`${path}.enum[${index}]`, candidate, new WeakSet(), issues);
        });
      }
    }

    const declaredType = schema.type;
    if (declaredType !== undefined) {
      this._validateSkillArgDeclaredType(path, declaredType, issues);
    }

    const properties = schema.properties;
    if (properties !== undefined) {
      if (!this._isPlainRecord(properties)) {
        issues.push(`${path}.properties must be an object`);
      } else {
        for (const [key, childSchema] of Object.entries(properties)) {
          if (!this._isPlainRecord(childSchema)) {
            issues.push(`${path}.properties.${key} must be an object`);
            continue;
          }
          this._validateSkillArgSchemaShape(path === '$' ? key : `${path}.${key}`, childSchema, issues, seen);
        }
      }
    }

    const additionalProperties = schema.additionalProperties;
    if (additionalProperties !== undefined && additionalProperties !== true && additionalProperties !== false) {
      issues.push(`${path}.additionalProperties must be boolean`);
    }

    const items = schema.items;
    if (items !== undefined) {
      if (!this._isPlainRecord(items)) {
        issues.push(`${path}.items must be an object`);
      } else {
        this._validateSkillArgSchemaShape(`${path}[]`, items, issues, seen);
      }
    }

    seen.delete(schema);
  }

  private _validateSkillArgSchemaValue(
    path: string,
    value: unknown,
    schema: Record<string, unknown>,
    issues: string[],
  ): void {
    const required = schema.required;
    if (Array.isArray(required) && this._isPlainRecord(value)) {
      for (const key of required) {
        if (!Object.prototype.hasOwnProperty.call(value, key) || value[key] === undefined) {
          issues.push(`missing required arg: "${path === '$' ? key : `${path}.${key}`}"`);
        }
      }
    }

    const enumValues = schema.enum;
    if (Array.isArray(enumValues) && !enumValues.some(candidate => this._skillArgValuesEqual(candidate, value))) {
      issues.push(`${path} must be one of ${JSON.stringify(enumValues)}`);
    }

    const declaredType = schema.type;
    if (declaredType !== undefined && !this._matchesSkillArgType(value, declaredType, path, issues)) return;

    const properties = schema.properties;
    const additionalProperties = schema.additionalProperties;
    if (this._isPlainRecord(properties) && this._isPlainRecord(value)) {
      for (const [key, childSchema] of Object.entries(properties)) {
        if (!Object.prototype.hasOwnProperty.call(value, key)) continue;
        if (!this._isPlainRecord(childSchema)) continue;
        this._validateSkillArgSchemaValue(path === '$' ? key : `${path}.${key}`, value[key], childSchema, issues);
      }
    }
    if (additionalProperties === false && this._isPlainRecord(value)) {
      const allowedKeys = this._isPlainRecord(properties) ? new Set(Object.keys(properties)) : new Set<string>();
      for (const key of Object.keys(value)) {
        if (!allowedKeys.has(key)) issues.push(`unsupported arg: "${path === '$' ? key : `${path}.${key}`}"`);
      }
    }

    const items = schema.items;
    if (this._isPlainRecord(items) && Array.isArray(value)) {
      value.forEach((item, index) => {
        this._validateSkillArgSchemaValue(`${path}[${index}]`, item, items, issues);
      });
    }
  }

  private _validateSkillArgDeclaredType(path: string, declaredType: unknown, issues: string[]): void {
    const allowedTypes = Array.isArray(declaredType) ? declaredType : [declaredType];
    const supported = new Set(['string', 'number', 'integer', 'boolean', 'object', 'array', 'null']);
    if (allowedTypes.some(type => typeof type !== 'string' || !supported.has(type))) {
      issues.push(`${path}.type must be a supported JSON schema type`);
    }
  }

  private _matchesSkillArgType(value: unknown, declaredType: unknown, path: string, issues: string[]): boolean {
    const allowedTypes = Array.isArray(declaredType) ? declaredType : [declaredType];
    const supported = new Set(['string', 'number', 'integer', 'boolean', 'object', 'array', 'null']);
    if (allowedTypes.some(type => typeof type !== 'string' || !supported.has(type))) {
      issues.push(`${path}.type must be a supported JSON schema type`);
      return false;
    }

    const actualMatches = allowedTypes.some(type => {
      switch (type) {
        case 'string':
          return typeof value === 'string';
        case 'number':
          return typeof value === 'number' && Number.isFinite(value);
        case 'integer':
          return Number.isInteger(value);
        case 'boolean':
          return typeof value === 'boolean';
        case 'object':
          return this._isPlainRecord(value);
        case 'array':
          return Array.isArray(value);
        case 'null':
          return value === null;
        default:
          return false;
      }
    });
    if (!actualMatches) {
      issues.push(`${path} must be ${allowedTypes.join(' | ')}`);
    }
    return actualMatches;
  }

  private _isPlainRecord(value: unknown): value is Record<string, unknown> {
    if (!value || typeof value !== 'object' || Array.isArray(value)) return false;
    const prototype = Object.getPrototypeOf(value);
    return prototype === Object.prototype || prototype === null;
  }

  private _skillArgValuesEqual(left: unknown, right: unknown): boolean {
    if (Object.is(left, right)) return true;
    if (Array.isArray(left) && Array.isArray(right)) {
      return (
        left.length === right.length && left.every((value, index) => this._skillArgValuesEqual(value, right[index]))
      );
    }
    if (this._isPlainRecord(left) && this._isPlainRecord(right)) {
      const leftKeys = Object.keys(left);
      const rightKeys = Object.keys(right);
      if (leftKeys.length !== rightKeys.length) return false;
      return leftKeys.every(
        key => Object.prototype.hasOwnProperty.call(right, key) && this._skillArgValuesEqual(left[key], right[key]),
      );
    }
    return false;
  }

  private _validateJsonSerializableSkillArg(
    path: string,
    value: unknown,
    seen: WeakSet<object>,
    issues: string[],
  ): void {
    if (value === null || typeof value === 'string' || typeof value === 'boolean') return;
    if (typeof value === 'number') {
      if (!Number.isFinite(value)) issues.push(`${path} must be JSON-serializable`);
      return;
    }
    if (value === undefined || typeof value === 'bigint' || typeof value === 'function' || typeof value === 'symbol') {
      issues.push(`${path} must be JSON-serializable`);
      return;
    }
    if (typeof value !== 'object') return;
    if (
      Object.prototype.hasOwnProperty.call(value, 'toJSON') &&
      typeof (value as { toJSON?: unknown }).toJSON === 'function'
    ) {
      issues.push(`${path}.toJSON is not supported in skill args`);
      return;
    }
    if (seen.has(value)) {
      issues.push(`${path} must not contain circular references`);
      return;
    }
    seen.add(value);
    if (Array.isArray(value)) {
      value.forEach((item, index) => this._validateJsonSerializableSkillArg(`${path}[${index}]`, item, seen, issues));
    } else if (this._isPlainRecord(value)) {
      for (const [key, child] of Object.entries(value)) {
        this._validateJsonSerializableSkillArg(path === '$' ? key : `${path}.${key}`, child, seen, issues);
      }
    } else {
      issues.push(`${path} must be JSON-serializable`);
    }
    seen.delete(value);
  }

  /**
   * Compose the skill prompt body. When args are supplied, append a JSON
   * code block carrying them. No delimiters beyond the Markdown fence —
   * skill authors reference args inline in their instructions.
   */
  private _buildSkillPrompt(instructions: string, args: Record<string, unknown> | undefined): string {
    if (!args || Object.keys(args).length === 0) return instructions;
    const json = JSON.stringify(args, null, 2);
    return `${instructions}\n\n\`\`\`json\n${json}\n\`\`\``;
  }

  /**
   * Internal: resolve the skill catalog for this session, sharing a
   * single-flight promise across concurrent callers.
   */
  private async _resolveSkills(): Promise<HarnessSkill[]> {
    if (this._skillsCache) return this._skillsCache;
    if (this._skillsResolving) return this._skillsResolving;

    const build = async (): Promise<HarnessSkill[]> => {
      const codeSkills = this._harness._listCodeSkills();
      const workspace = await this.getWorkspace();
      const workspaceSkills = workspace?.skills;
      if (!workspaceSkills) {
        // No workspace, or workspace has no skill source configured.
        return codeSkills;
      }
      const entries = await workspaceSkills.list();
      const codeNames = new Set(codeSkills.map(skill => skill.name));
      const projected = await Promise.all(
        entries
          .filter(meta => !codeNames.has(meta.name))
          .map(async meta => {
            const skill = await workspaceSkills.get(meta.path ?? meta.name);
            return {
              name: meta.name,
              description: meta.description,
              instructions: skill?.instructions ?? '',
              ...(meta.path ? { filePath: meta.path } : {}),
              // Pass through arbitrary skill frontmatter metadata so callers can
              // discover skill-level flags (e.g. `metadata.goal === true` for
              // goal-mode skills). Workspace's `SkillMetadata.metadata` is
              // typed `Record<string, unknown>` and is already JSON-serialisable.
              ...(meta.metadata ? { metadata: meta.metadata } : {}),
            };
          }),
      );
      return [...codeSkills, ...projected];
    };

    const pending = build();
    this._skillsResolving = pending;
    try {
      const result = await pending;
      // Only populate the cache when our own promise is still the
      // session-tracked one. If `skills.refresh()` ran while we were
      // resolving, `_skillsResolving` was cleared (or replaced by a
      // newer build) and we must not stomp it.
      if (this._skillsResolving === pending) {
        this._skillsCache = result;
      }
      return result;
    } finally {
      if (this._skillsResolving === pending) {
        this._skillsResolving = undefined;
      }
    }
  }

  // -------------------------------------------------------------------------
  // Signal-routing helpers (§4.2). One long-lived thread subscription per
  // Session multiplexes every run on the thread into a single chunk
  // stream. `message()` calls `agent.sendSignal()`, gets a `runId` back,
  // and awaits the matching entry in `_runCompletionPromises`. Completion
  // settlement is handled by `_watchRunCompletion()`; the drain loop only
  // emits harness events from stream chunks.
  // -------------------------------------------------------------------------

  /**
   * Lazy-acquire the thread subscription against the given agent. Idempotent
   * when called with the same agent. If the agent changed (cross-agent mode
   * switch on the same thread), tears down the existing subscription and
   * opens a new one against the new agent so the chunk stream stays in
   * sync with the run the next `sendSignal()` will land on.
   */
  private async _ensureThreadSubscription(agent: Agent): Promise<AgentThreadSubscription<unknown>> {
    if (this._threadSubscriptionClosed) {
      if (this._state === 'deleted') {
        throw new HarnessSessionDeletedError(this.id, this._record.resourceId, this._record.threadId);
      }
      if (this._state === 'closed' || this._state === 'evicted') {
        throw new HarnessSessionClosedError(this.id);
      }
      throw new HarnessValidationError(
        '_ensureThreadSubscription()',
        'Session is closed; cannot re-open thread subscription.',
      );
    }
    if (this._threadSubscription && this._threadSubscriptionAgent?.id === agent.id) {
      return this._threadSubscription;
    }
    if (this._threadSubscription) {
      // Cross-agent mode switch: tear down the old subscription so we don't
      // mix chunks from two agents on the same thread.
      this._threadSubscription.unsubscribe();
      if (this._threadSubscriptionDrain) {
        await this._threadSubscriptionDrain.catch(() => {});
      }
      this._threadSubscription = undefined;
      this._threadSubscriptionAgent = undefined;
      this._threadSubscriptionDrain = undefined;
    }
    const sub = await agent.subscribeToThread({ resourceId: this.resourceId, threadId: this.threadId });
    if (this._threadSubscriptionClosed) {
      try {
        sub.unsubscribe();
      } catch {
        // Best-effort — hard-delete or close won while subscribeToThread was pending.
      }
      if (this._state === 'deleted') {
        throw new HarnessSessionDeletedError(this.id, this._record.resourceId, this._record.threadId);
      }
      if (this._state === 'closed' || this._state === 'evicted') {
        throw new HarnessSessionClosedError(this.id);
      }
      throw new HarnessValidationError(
        '_ensureThreadSubscription()',
        'Session is closed; cannot install thread subscription.',
      );
    }
    this._threadSubscription = sub;
    this._threadSubscriptionAgent = agent;
    this._threadSubscriptionDrain = this._drainSubscriptionStream(sub);
    // Surface drain rejections to outstanding awaiters; the drain loop itself
    // swallows them in its `finally` block.
    void this._threadSubscriptionDrain.catch(() => {});
    return sub;
  }

  private async _ensureThreadSubscriptionOrDeleted(agent: Agent): Promise<AgentThreadSubscription<unknown>> {
    const activeTurnWaiter = this._createActiveTurnWaiter();
    void activeTurnWaiter.promise.catch(() => {});
    try {
      return await this._raceActiveTurnWaiter(this._ensureThreadSubscription(agent), activeTurnWaiter.promise);
    } finally {
      activeTurnWaiter.cleanup();
    }
  }

  /**
   * Returns a Promise that resolves with a synthetic `FullOutput` when the
   * run with the given id terminates. The drain loop resolves (or rejects)
   * the entry. If `close()` runs while the entry is pending, the entry is
   * rejected with a typed error.
   */
  private _awaitRunCompletion(runId: string): Promise<FullOutput<unknown>> {
    // Fast path: the run may have already terminated before this call ran.
    // Keep the cached result reusable so duplicate admission callers that
    // converge on the same runId can still observe the terminal output even
    // if another waiter arrived first.
    const cached = this._completedRuns.get(runId);
    if (cached) {
      return cached.ok ? Promise.resolve(cached.full) : Promise.reject(cached.err);
    }
    // Multiple callers can await the same run (e.g. `message()` followed by
    // an active-delivery `signal()` that drains into the same run). Memoize
    // the promise so they all see the same resolution.
    const existing = this._runCompletionPromises.get(runId);
    if (existing) return existing.promise;
    let resolve!: (full: FullOutput<unknown>) => void;
    let reject!: (err: unknown) => void;
    const promise = new Promise<FullOutput<unknown>>((res, rej) => {
      resolve = res;
      reject = rej;
    });
    this._runCompletionPromises.set(runId, { promise, resolve, reject });
    // Single canonical settler: wait for the runtime to register the run's
    // `MastraModelOutput`, then await its `_waitUntilFinished()`. The drain
    // loop emits events from chunks; it does NOT settle completion. This
    // keeps event emission and completion delivery on independent paths and
    // is robust to runs that finish without emitting an explicit terminal
    // chunk in `fullStream` (test doubles, abort-before-first-chunk, etc.).
    void this._watchRunCompletion(runId);
    return promise;
  }

  /**
   * Canonical completion watcher. Acquires the run's `MastraModelOutput`
   * from the runtime via `waitForRunOutput()` (event-driven — no polling),
   * awaits `_waitUntilFinished()`, then settles the outstanding completion
   * promise (or stashes the result in `_completedRuns` if the waiter has not
   * been registered yet, e.g. for very fast runs).
   *
   * The captured `out` reference is threaded through to `_handleRunTerminal`
   * because the runtime drops the record from `getRunOutput()` after
   * `_waitUntilFinished()` resolves.
   */
  private async _watchRunCompletion(runId: string): Promise<void> {
    const agent = this._threadSubscriptionAgent;
    if (!agent) return;
    let out: MastraModelOutput<unknown> & { _waitUntilFinished?: () => Promise<void> };
    try {
      out = (await agent.waitForRunOutput(runId)) as MastraModelOutput<unknown> & {
        _waitUntilFinished?: () => Promise<void>;
      };
    } catch (err) {
      const waiter = this._runCompletionPromises.get(runId);
      this._runCompletionPromises.delete(runId);
      this._rememberCompletedRun(runId, { ok: false, err });
      waiter?.reject(err);
      return;
    }
    try {
      if (typeof out._waitUntilFinished === 'function') {
        await out._waitUntilFinished();
      }
    } catch {
      // Ignore — settlement happens via `_handleRunTerminal` below, which
      // will pick up the run's own error state via `getFullOutput()`.
    }
    await this._handleRunTerminal(runId, out as MastraModelOutput<unknown>);
  }

  /**
   * Drain the long-lived subscription stream. The drain is the **sole event
   * emitter** for the session — each chunk is translated into the matching
   * harness event(s) via `_emitForChunk`. Completion delivery is handled
   * elsewhere (`_watchRunCompletion` driven by `_waitUntilFinished()`); this
   * loop deliberately does not inspect terminal chunks.
   *
   * On drain shutdown (stream end or unhandled error) every outstanding
   * completion promise is rejected so callers don't hang.
   */
  private async _drainSubscriptionStream(sub: AgentThreadSubscription<unknown>): Promise<void> {
    try {
      for await (const chunk of sub.stream) {
        const runId = (chunk as { runId?: string }).runId;
        if (runId && this._currentRunId === undefined) {
          // First chunk for a run marks our "current run" for getDisplayState().
          this._currentRunId = runId;
        }
        this._emitForChunk(chunk);
      }
    } catch (err) {
      for (const [, entry] of this._runCompletionPromises) {
        entry.reject(err);
      }
      this._runCompletionPromises.clear();
    } finally {
      // Stream ended normally — any caller still waiting for a runId whose
      // completion we never observed would hang forever otherwise.
      for (const [, entry] of this._runCompletionPromises) {
        entry.reject(
          new HarnessValidationError('_drainSubscriptionStream()', 'Thread subscription closed before run completion'),
        );
      }
      this._runCompletionPromises.clear();
    }
  }

  /**
   * Drain a RESUME run's own `fullStream` through `_emitForChunk` so the
   * approved tool's `tool_end` and any post-approval `text_delta` surface LIVE
   * (§10.4) to subscribers. Unlike the initial run, a resume reuses the
   * suspended run's `runId` (`pending.runId`), which the long-lived thread
   * subscription has already recorded in its `seenRunIds`; the subscription
   * therefore dedups the re-registered resume run and never re-drains it, so
   * this local drain is the SOLE consumer of the resumed segment's chunks (no
   * double-emit). This is an independent evented reader over the output's
   * shared chunk buffer; it does NOT gate completion delivery, which stays on
   * the `getFullOutput()` / `_waitUntilFinished()` path. `_emitForChunk` never
   * emits a terminal `agent_end` — the resume's terminal is emitted explicitly
   * by `respondTo*` after this drain — so a re-suspend or error mid-resume does
   * not produce a spurious terminal here.
   */
  private async _drainResumeStream(out: MastraModelOutput<unknown>): Promise<void> {
    const stream = (out as { fullStream?: ReadableStream<unknown> }).fullStream;
    if (!stream) return;
    // The resume run replays the approved tool's `tool-result` chunk WITHOUT a
    // preceding `tool-call` chunk in this segment (the `tool-call` landed in the
    // initial, suspended turn), so `_activeTools` (seeded only by `tool-call`)
    // does not necessarily hold the resumed tool. `_emitForChunk` now reads the
    // `toolName` from the chunk payload itself (which always carries it), so the
    // live `tool_end` surfaces the real name without re-seeding `_activeTools`.
    for await (const chunk of stream as AsyncIterable<{
      type: string;
      payload?: unknown;
      data?: unknown;
      runId?: string;
    }>) {
      this._emitForChunk(chunk);
    }
  }

  /**
   * Settle the outstanding completion waiter for `runId` with the bundled
   * `FullOutput`. Always called from `_watchRunCompletion` with the output
   * reference captured at registration time (runtime cleanup may have
   * already dropped it from `getRunOutput()` by now).
   *
   * If no waiter is registered yet (very fast run), the result is stashed
   * in `_completedRuns` so later `_awaitRunCompletion(runId)` calls can
   * observe it.
   */
  private async _handleRunTerminal(runId: string, out: MastraModelOutput<unknown>): Promise<void> {
    const waiter = this._runCompletionPromises.get(runId);
    this._runCompletionPromises.delete(runId);
    const cached = this._completedRuns.get(runId);
    if (cached && !cached.ok) {
      if (waiter) waiter.reject(cached.err);
      return;
    }
    try {
      const full = (await out.getFullOutput()) as FullOutput<unknown>;
      this._rememberCompletedRun(runId, { ok: true, full });
      if (waiter) waiter.resolve(full);
    } catch (err) {
      this._rememberCompletedRun(runId, { ok: false, err });
      if (waiter) waiter.reject(err);
    }
  }

  private _rememberCompletedRun(
    runId: string,
    entry: { ok: true; full: FullOutput<unknown> } | { ok: false; err: unknown },
  ): void {
    if (this._completedRuns.has(runId)) return;
    this._completedRuns.set(runId, entry);
    while (this._completedRuns.size > 64) {
      const oldest = this._completedRuns.keys().next().value;
      if (oldest === undefined) return;
      this._completedRuns.delete(oldest);
      this._messageTokenAccountedRunIds.delete(oldest);
      this._messageTokenAccountingRunIds.delete(oldest);
      this._messageTokenAccountingReservations.delete(oldest);
    }
  }

  /**
   * Translate a single fullStream chunk into the matching harness event(s).
   * Extracted from `_drainStreamToEvents` so the long-lived subscription
   * drain is the single consumer of chunks.
   */
  private _emitForChunk(chunk: { type: string; payload?: unknown; data?: unknown; runId?: string }): void {
    // §10.2 TurnEvent/ToolEvent — runId identifies the streaming run. Mid-run
    // chunks carry it; fall back to the session's current run id.
    const runId = chunk.runId ?? this._currentRunId;
    switch (chunk.type) {
      case 'text-start': {
        // §10.2 defines no message-boundary event; we still track the in-flight
        // message id for the display snapshot.
        const payload = chunk.payload as { id: string };
        this._currentMessageId = payload.id;
        return;
      }
      case 'text-delta': {
        const payload = chunk.payload as { id: string; text?: string };
        if (runId !== undefined && typeof payload?.text === 'string' && payload.text.length > 0) {
          this._emitTurnEvent({ type: 'text_delta', runId, delta: payload.text });
        }
        return;
      }
      case 'text-end': {
        const payload = chunk.payload as { id: string };
        if (this._currentMessageId === payload.id) {
          this._currentMessageId = undefined;
        }
        return;
      }
      case 'tool-call-input-streaming-start': {
        // §10.2 has no tool-input-streaming events; keep the buffer for the
        // display snapshot of in-flight args, but emit nothing.
        const payload = chunk.payload as { toolCallId: string; toolName: string };
        this._toolInputBuffers.set(payload.toolCallId, { toolName: payload.toolName, text: '' });
        return;
      }
      case 'tool-call-delta': {
        const payload = chunk.payload as { toolCallId: string; argsTextDelta: string; toolName?: string };
        const prev = this._toolInputBuffers.get(payload.toolCallId);
        const toolName = prev?.toolName ?? payload.toolName ?? '';
        this._toolInputBuffers.set(payload.toolCallId, {
          toolName,
          text: (prev?.text ?? '') + payload.argsTextDelta,
        });
        return;
      }
      case 'tool-call-input-streaming-end': {
        const payload = chunk.payload as { toolCallId: string };
        this._toolInputBuffers.delete(payload.toolCallId);
        return;
      }
      case 'tool-call': {
        const payload = chunk.payload as { toolCallId: string; toolName: string; args: unknown };
        this._activeTools.set(payload.toolCallId, {
          toolCallId: payload.toolCallId,
          toolName: payload.toolName,
          args: payload.args,
          startedAt: Date.now(),
        });
        this._toolInputBuffers.delete(payload.toolCallId);
        if (runId !== undefined) {
          // §10.2: project `input` to its JSON-safe replay shape AT EMIT so the
          // live subscriber and the durable/replayed row carry the identical
          // value (the raw `args` may hold Date/Map/Set/class/bigint/cycles).
          this._emitTurnEvent({
            type: 'tool_start',
            runId,
            toolCallId: payload.toolCallId,
            toolName: payload.toolName,
            input: projectToolEventPayloadForJson(payload.args, 'tool_start.input'),
          });
        }
        return;
      }
      case 'tool-result': {
        const payload = chunk.payload as { toolCallId: string; toolName?: string; result: unknown; isError?: boolean };
        // §10.4: the resume segment replays the approved tool's `tool-result`
        // WITHOUT a preceding `tool-call` in this segment, so `_activeTools` (seeded
        // only by `tool-call`) misses on resume and `tool_end.toolName` would be ''.
        // The chunk payload itself carries the required `toolName` (ToolResultPayload,
        // stream/types.ts) on BOTH the initial and resume paths, so prefer it; fall
        // back to the `_activeTools` entry for any chunk shape that omits it.
        const toolName = payload.toolName || this._activeTools.get(payload.toolCallId)?.toolName || '';
        this._activeTools.delete(payload.toolCallId);
        if (runId !== undefined) {
          // Project `output` at emit so live === replay (§10.2). Shared/aliased
          // refs are split into copies, Date->ISO, Map/Set->{}, class->plain.
          this._emitTurnEvent({
            type: 'tool_end',
            runId,
            toolCallId: payload.toolCallId,
            toolName,
            output: projectToolEventPayloadForJson(payload.result, 'tool_end.output'),
            isError: payload.isError ?? false,
          });
        }
        return;
      }
      case 'tool-error': {
        const payload = chunk.payload as { toolCallId: string; toolName?: string; error: unknown };
        // Same resume gap as `tool-result`: prefer the payload's own `toolName`
        // (ToolErrorPayload, stream/types.ts) so a resumed/replayed `tool-error`
        // carries the real name; fall back to `_activeTools` for chunk shapes
        // that omit it.
        const toolName = payload.toolName || this._activeTools.get(payload.toolCallId)?.toolName || '';
        this._activeTools.delete(payload.toolCallId);
        if (runId !== undefined) {
          // §13.3f.1: a tool's OWN error is faithfully preserved (the shared
          // `harnessEventJsonReplacer` keeps name/code/message — NOT flattened
          // into `harness.internal`), and projected at emit so the raw thrown
          // Error (stack/cause/prototype) never reaches a live subscriber while
          // replay sees only {name,code,message}.
          this._emitTurnEvent({
            type: 'tool_end',
            runId,
            toolCallId: payload.toolCallId,
            toolName,
            output: projectToolEventPayloadForJson(payload.error, 'tool_end.output'),
            isError: true,
          });
        }
        return;
      }
      default: {
        // §10.2 defines no tool-progress / shell-output / task-update built-in
        // events. Tool authors surface progress via §10.3 custom events
        // (dotted `${string}.${string}` types), which flow through the custom
        // event path — the harness does not synthesize built-ins from `data-*`
        // writer chunks.
        return;
      }
    }
  }

  // -------------------------------------------------------------------------
  // message() — §4.2.
  //
  // Always-accept signal-driven entry point. Three return shapes:
  //
  //   * default                          → AgentResult (await everything)
  //   * { stream: true }                 → live MastraModelOutput
  //   * { output: schema, sync: true }   → fail-fast structured object
  //
  // Default + stream paths route through `agent.sendSignal()` (Slice A).
  // Structured + sync path stays on `agent.generate()` so typed-output
  // turn boundaries remain fail-fast and uncoupled from the subscription
  // multiplexer.
  // -------------------------------------------------------------------------

  /** Default: bundle the full agent output and return when the run finishes. */
  async message(opts: MessageOptionsDefault): Promise<AgentResult>;
  /** Streaming: hand the live `MastraModelOutput` back to the caller. */
  async message(opts: MessageOptionsStream): Promise<AgentStream>;
  /** Structured + sync: fail-fast typed object output. */
  async message<S extends z.ZodTypeAny>(opts: MessageOptionsStructured<S>): Promise<z.infer<S>>;
  async message(opts: MessageOptions): Promise<AgentResult | AgentStream | unknown> {
    this._assertLive('message()');
    this._assertOpenForTurn('message()');

    if (opts.stream === true && opts.output !== undefined) {
      throw new HarnessConfigError('message()', '`stream: true` and `output` are mutually exclusive');
    }
    if (opts.output !== undefined && opts.sync !== true) {
      throw new HarnessConfigError('message()', 'structured `output` requires `sync: true` (typed turn boundary)');
    }
    if (opts.admissionId !== undefined && opts.output !== undefined) {
      throw new HarnessValidationError(
        'message().admissionId',
        'admissionId is not supported with sync structured output',
      );
    }
    if (opts.admissionId !== undefined && opts.additionalTools !== undefined) {
      throw new HarnessValidationError('message().admissionId', 'admissionId cannot be combined with additionalTools');
    }
    if (opts.admissionId !== undefined && opts.admissionId.length === 0) {
      throw new HarnessValidationError('message().admissionId', 'admissionId must be a non-empty string');
    }

    // §3 / §4.4a: the sync structured-output form is the one fail-fast signal
    // form — it needs a clean turn boundary, so refuse on a busy session rather
    // than launching a concurrent generate. Default message()/signal()/queue()
    // stay busy-independent.
    if (opts.output !== undefined && opts.sync === true && this.isBusy()) {
      throw new HarnessBusyError(this.id, this._busyReason());
    }

    // §4.4c: validate caller-supplied request context before any admission
    // side-effect (hash, evidence reservation, dispatch). Only `app` is allowed;
    // reserved/infrastructure keys are rejected here with HarnessValidationError.
    const callerRequestContext = validateCallerRequestContext(opts.requestContext, 'message()');
    // When a turn is already in flight, the signal-routed path interleaves into
    // that active run and its streamOptions (which carry the request context)
    // are ignored, so a caller `app` could never reach the running tools. Reject
    // rather than silently drop it. We key off an already-active turn — NOT the
    // broader isBusy() (which also covers a pending queue / pending resume with
    // no active run, where this message wakes a fresh run that DOES receive the
    // context). This guard runs before this turn's own _beginTurn, so the
    // controller here reflects a pre-existing active turn. (The structured+sync
    // form already rejected busy above.)
    if (callerRequestContext !== undefined && this._currentTurnAbortController !== undefined) {
      throw new HarnessConfigError(
        'message().requestContext',
        'cannot be supplied while a run is active on this thread — it would interleave into the running execution and could not reach its tools',
      );
    }
    const persistedRequestContext = callerRequestContextToPersisted(callerRequestContext);

    // Resolve the effective mode (per-call override wins, else session's).
    const effectiveModeId = opts.mode ?? this._record.modeId;
    const effectiveModelId = opts.model ?? this._record.modelId;
    const mode = this._harness._getMode(effectiveModeId);
    const agent = this._harness.getAgentForMode(effectiveModeId);
    const admissionHashes =
      opts.admissionId !== undefined
        ? this._computeMessageAdmissionHashes(
            opts,
            {
              modeId: effectiveModeId,
              modelId: effectiveModelId,
            },
            persistedRequestContext,
          )
        : undefined;
    const admissionHash = admissionHashes?.primary;
    const compatibleAdmissionHashes = admissionHashes?.legacyCompatible;
    const duplicate =
      opts.admissionId !== undefined
        ? await this._resolveMessageAdmissionDuplicate({
            admissionId: opts.admissionId,
            admissionHash: admissionHash!,
            compatibleAdmissionHashes,
          })
        : undefined;
    if (duplicate) {
      this._assertOpenForTurn('message()');
      return this._returnDuplicateMessageResult(duplicate, opts);
    }
    const admissionIdentity =
      opts.admissionId !== undefined ? this._messageAdmissionIdentity(opts.admissionId) : undefined;

    // Per-turn additionalTools merge with the mode's surface, never replace.
    const toolsets = this._buildToolsets(mode, opts.additionalTools);

    const admissionStart =
      opts.admissionId !== undefined
        ? createDeferred<AgentSignalResultEvidence | OperationAdmissionTombstone>()
        : undefined;
    if (admissionStart) void admissionStart.promise.catch(() => {});
    if (admissionIdentity !== undefined && admissionHash !== undefined && admissionStart !== undefined) {
      const existingStart = this._messageAdmissionStarts.get(opts.admissionId!);
      if (existingStart) {
        if (existingStart.admissionHash !== admissionHash) {
          throw new HarnessAdmissionConflictError(
            this.id,
            opts.admissionId!,
            existingStart.admissionHash,
            admissionHash,
          );
        }
        const evidence = await existingStart.promise;
        return this._returnDuplicateMessageResult(evidence, opts);
      }
      this._messageAdmissionStarts.set(opts.admissionId!, {
        admissionHash,
        modeId: effectiveModeId,
        modelId: effectiveModelId,
        promise: admissionStart.promise,
      });
    }

    // Every turn runs under a session-owned AbortController so
    // `session.abort()` can cancel the in-flight run. If the caller passes
    // their own AbortSignal, we forward it into the session controller so
    // both paths converge on a single signal handed to the agent.
    const turnAbortController = this._beginTurn(opts.abortSignal, {
      modeId: effectiveModeId,
      modelId: effectiveModelId,
    });
    const turnAbortSignal = turnAbortController.signal;
    const activeTurnWaiter = this._createActiveTurnWaiter();
    void activeTurnWaiter.promise.catch(() => {});
    const finishOwnedMessageTurn = () => {
      activeTurnWaiter.cleanup();
      this._endTurn(turnAbortController);
    };
    const failOwnedMessageTurnBeforeDispatch = (err: unknown) => {
      finishOwnedMessageTurn();
      admissionStart?.reject(err);
      if (opts.admissionId !== undefined) this._messageAdmissionStarts.delete(opts.admissionId);
    };
    const assertOwnedMessageTurnNotDeleted = () => {
      if (this._state === 'deleted') {
        const err = new HarnessSessionDeletedError(this.id, this._record.resourceId, this._record.threadId);
        failOwnedMessageTurnBeforeDispatch(err);
        throw err;
      }
    };
    try {
      this._assertOpenForTurn('message()');
    } catch (err) {
      failOwnedMessageTurnBeforeDispatch(err);
      throw err;
    }
    let requestContext;
    try {
      requestContext = await Promise.race([
        this._buildRequestContext({
          modeId: effectiveModeId,
          modelId: effectiveModelId,
          abortSignal: turnAbortSignal,
          ...(persistedRequestContext ? { persistedRequestContext } : {}),
        }),
        activeTurnWaiter.promise,
      ]);
    } catch (err) {
      failOwnedMessageTurnBeforeDispatch(err);
      throw err;
    }
    assertOwnedMessageTurnNotDeleted();

    const baseExecOptions: AgentExecutionOptionsBase<unknown> = {
      memory: { thread: this.threadId, resource: this.resourceId },
      abortSignal: turnAbortSignal,
      requestContext,
      ...(toolsets ? { toolsets } : {}),
      ...(mode.instructions ? { instructions: mode.instructions } : {}),
    };

    // Structured + sync path: agent.generate with structuredOutput.
    if (opts.output !== undefined && opts.sync === true) {
      let agentEndEmitted = false;
      let agentStartRunId: string | undefined;
      // Hoisted so the catch can emit `agent_end.usage` from the run's actual
      // output when a usable result existed but turned out to be a generation
      // failure (tripwire / missing object). Undefined when generate threw.
      let observedFull: FullOutput<unknown> | undefined;
      try {
        const result = await Promise.race([
          agent.generate(opts.content, {
            ...baseExecOptions,
            structuredOutput: { schema: opts.output as never },
          }),
          activeTurnWaiter.promise,
        ]);
        const full = result as FullOutput<unknown>;
        observedFull = full;
        // §10.2 agent_start carries the runId; generate is atomic, so we learn
        // the runId only once it returns. (If generate threw, no run terminal
        // was observed — no agent_start/agent_end, per §10.2's runId rule.)
        agentStartRunId = full.runId;
        this._emitAgentStart(full.runId);
        if (full.finishReason === 'suspended') {
          await this._captureMessageSuspendWithTokenUsage(
            full,
            undefined,
            effectiveModeId,
            effectiveModelId,
            activeTurnWaiter.promise,
          );
        } else {
          // Account token usage for the run regardless of whether it produced a
          // usable object — the model consumed tokens either way. This runs before
          // the §4.5 failure throw so a tripwire/missing-object turn is not dropped
          // from the durable counter.
          const tokenUsageDelta = this._recordTurnCompletion(full, { persist: false });
          if (tokenUsageDelta !== undefined) {
            await Promise.race([this._persistTokenUsageOrLatch(), activeTurnWaiter.promise]);
          }
          // §4.5: a non-suspended structured result that carries no usable object
          // (or was rejected by a processor tripwire) is a generation failure. Throw
          // after accounting but before emitting a terminal so the catch below
          // surfaces an `error` agent_end rather than a spurious `complete`.
          const outputFailure = this._classifyStructuredOutputFailure(full);
          if (outputFailure !== undefined) {
            throw new HarnessOutputGenerationError(this.id, outputFailure, full.runId);
          }
        }
        this._emitAgentEnd({ runId: full.runId, finishReason: this._agentEndReasonForFullOutput(full), full });
        agentEndEmitted = true;
        await Promise.race([this._runGoalJudge(full, false), activeTurnWaiter.promise]);
        return full.object;
      } catch (err) {
        if (!agentEndEmitted && agentStartRunId !== undefined) {
          this._emitTurnEvent({
            type: 'agent_end',
            runId: agentStartRunId,
            finishReason: turnAbortSignal.aborted ? 'aborted' : 'error',
            usage: this._runUsage(observedFull),
          });
        }
        throw this._asStructuredOutputError(err, agentStartRunId, turnAbortSignal);
      } finally {
        finishOwnedMessageTurn();
      }
    }

    // Signal-routed path: every non-structured message goes through
    // `agent.sendSignal()`. The long-lived thread subscription is the
    // single chunk consumer for this Session; the drain loop emits
    // per-chunk harness events and resolves `_runCompletionPromises[runId]`
    // when the run terminates.
    //
    // On an idle thread the agent starts a fresh run with
    // `agent.stream(signal, streamOptions)`; on an active same-agent run
    // the signal drains mid-flight into the running execution loop. Both
    // paths surface chunks through the same subscription stream.
    let sub;
    try {
      sub = await Promise.race([this._ensureThreadSubscription(agent), activeTurnWaiter.promise]);
    } catch (err) {
      failOwnedMessageTurnBeforeDispatch(err);
      throw err;
    }
    assertOwnedMessageTurnNotDeleted();

    // §4.4c: authoritative active-delivery check, using the same `sub.activeRunId()` predicate
    // signal() relies on, now that the live subscription is open. The early
    // `_currentTurnAbortController` guard is a cheap fast path; this catches a run that started
    // during the intervening request-context/admission awaits (where an interleaving signal would
    // ignore the streamOptions carrying the caller `app`). It runs BEFORE the durable admission
    // reservation below, so rejecting here writes nothing durable and the owned-turn cleanup
    // suffices — and, crucially, it does NOT poison an idempotent retry with a spurious `failed`
    // evidence row. The single residual window is the awaited reservation itself (admissionId
    // path only): a run starting precisely during that write is admission-scoped only and is not
    // delivered to the interleaved run, matching how `abortSignal` behaves on active-delivery —
    // we deliberately do not reject post-reservation, because that would poison the retry.
    if (callerRequestContext !== undefined && sub.activeRunId() !== null) {
      const err = new HarnessConfigError(
        'message().requestContext',
        'cannot be supplied while a run is active on this thread — it would interleave into the running execution and could not reach its tools',
      );
      failOwnedMessageTurnBeforeDispatch(err);
      throw err;
    }

    if (admissionIdentity !== undefined && admissionHash !== undefined && admissionStart !== undefined) {
      try {
        const reservation = await Promise.race([
          this._writeMessageResultEvidence(
            {
              status: 'pending',
              signalId: admissionIdentity.signalId,
              runId: admissionIdentity.runId,
              modeId: effectiveModeId,
              modelId: effectiveModelId,
              admissionId: opts.admissionId!,
              admissionHash,
            },
            { compatibleAdmissionHashes },
          ),
          activeTurnWaiter.promise,
        ]);
        if (!reservation.created) {
          this._messageAdmissionStarts.delete(opts.admissionId!);
          const existing =
            reservation.evidence ??
            (await this._resolveMessageAdmissionDuplicate({
              admissionId: opts.admissionId!,
              admissionHash,
              compatibleAdmissionHashes,
            }));
          if (existing) {
            admissionStart.resolve(existing);
            try {
              return await this._returnDuplicateMessageResult(existing, opts);
            } finally {
              finishOwnedMessageTurn();
            }
          }
          const conflict = new HarnessAdmissionConflictError(this.id, opts.admissionId!, '', admissionHash);
          admissionStart.reject(conflict);
          throw conflict;
        }
      } catch (err) {
        failOwnedMessageTurnBeforeDispatch(err);
        throw err;
      }
      assertOwnedMessageTurnNotDeleted();
    }

    let signal;
    try {
      signal = agent.sendSignal(
        {
          ...(admissionIdentity ? { id: admissionIdentity.signalId } : {}),
          type: 'user-message',
          contents: opts.content as never,
        },
        {
          ...(admissionIdentity ? { runId: admissionIdentity.runId } : {}),
          resourceId: this.resourceId,
          threadId: this.threadId,
          ifIdle: { behavior: 'wake', streamOptions: baseExecOptions as never },
        },
      );
    } catch (err) {
      let thrown = err;
      if (admissionIdentity !== undefined && admissionHash !== undefined) {
        try {
          await Promise.race([
            this._writeMessageResultEvidence(
              {
                status: 'failed',
                signalId: admissionIdentity.signalId,
                runId: admissionIdentity.runId,
                modeId: effectiveModeId,
                modelId: effectiveModelId,
                admissionId: opts.admissionId!,
                admissionHash,
                error: projectHarnessPublicError(err),
              },
              { compatibleAdmissionHashes },
            ).catch(() => {}),
            activeTurnWaiter.promise,
          ]);
        } catch (evidenceErr) {
          if (evidenceErr instanceof HarnessSessionDeletedError) thrown = evidenceErr;
        }
      }
      failOwnedMessageTurnBeforeDispatch(thrown);
      throw thrown;
    }

    // §10.2 agent_start carries the runId — emit it now that the run is
    // dispatched (signal.runId known) and before the drain surfaces any chunk.
    // signal.signal.id is the originating signalId — stamped for the run projection.
    this._emitAgentStart(signal.runId, signal.signal.id);

    // Register the completion waiter BEFORE the drain has a chance to see
    // a terminal chunk for this runId (the run can start synchronously on
    // the wake path).
    const completion = this._awaitRunCompletion(signal.runId);
    void completion.catch(() => {});
    let admissionStartSettled = false;
    const resolveMessageAdmissionStart = () => {
      if (
        admissionStartSettled ||
        admissionStart === undefined ||
        admissionIdentity === undefined ||
        admissionHash === undefined
      ) {
        return;
      }
      admissionStartSettled = true;
      const now = Date.now();
      admissionStart.resolve({
        status: 'pending',
        harnessName: this._record.harnessName,
        sessionId: this.id,
        resourceId: this.resourceId,
        threadId: this.threadId,
        signalId: signal.signal.id,
        runId: signal.runId,
        modeId: effectiveModeId,
        modelId: effectiveModelId,
        admissionId: opts.admissionId!,
        admissionHash,
        createdAt: now,
        updatedAt: now,
      });
    };
    const rejectMessageAdmissionStart = (err: unknown) => {
      if (admissionStartSettled || admissionStart === undefined) return;
      admissionStartSettled = true;
      admissionStart.reject(err);
    };

    const pendingEvidenceWrite =
      admissionIdentity !== undefined
        ? this._writeMessageResultEvidence(
            {
              status: 'pending',
              signalId: signal.signal.id,
              runId: signal.runId,
              modeId: effectiveModeId,
              modelId: effectiveModelId,
              ...(opts.admissionId !== undefined ? { admissionId: opts.admissionId } : {}),
              ...(admissionHash !== undefined ? { admissionHash } : {}),
            },
            { compatibleAdmissionHashes },
          )
        : Promise.resolve();
    void pendingEvidenceWrite.catch(() => {});

    const failDispatchedMessageTurn = async (err: unknown) => {
      turnAbortController.abort(err);
      finishOwnedMessageTurn();
      rejectMessageAdmissionStart(err);
      void completion.catch(() => {});
      const waiter = this._runCompletionPromises.get(signal.runId);
      this._runCompletionPromises.delete(signal.runId);
      this._rememberCompletedRun(signal.runId, { ok: false, err });
      waiter?.reject(err);
      if (admissionIdentity !== undefined && this._shouldWriteTurnFailureEvidence(err)) {
        this._writeMessageResultEvidenceBestEffortInBackground(
          {
            status: 'failed',
            signalId: signal.signal.id,
            runId: signal.runId,
            modeId: effectiveModeId,
            modelId: effectiveModelId,
            error: projectHarnessPublicError(err),
            admissionId: opts.admissionId!,
            admissionHash: admissionHash!,
          },
          { compatibleAdmissionHashes },
        );
      }
      if (opts.admissionId !== undefined) this._messageAdmissionStarts.delete(opts.admissionId);
      void this._maybeDrainQueue();
    };

    const awaitPendingMessageEvidence = async () => {
      await Promise.race([pendingEvidenceWrite, activeTurnWaiter.promise]);
      resolveMessageAdmissionStart();
    };

    // Streaming path: hand the live `MastraModelOutput` back. The drain
    // loop is responsible for harness events; we still keep the turn
    // in-flight (so `isRunning()` reports true) until the run completes.
    if (opts.stream === true) {
      let out = agent.getRunOutput(signal.runId) as MastraModelOutput<unknown> | undefined;
      if (!out && (signal.output || admissionIdentity !== undefined)) {
        try {
          await awaitPendingMessageEvidence();
        } catch (err) {
          await failDispatchedMessageTurn(err);
          throw err;
        }
        try {
          out = signal.output
            ? ((await Promise.race([signal.output, activeTurnWaiter.promise])) as MastraModelOutput<unknown>)
            : ((await Promise.race([
                agent.waitForRunOutput(signal.runId) as Promise<MastraModelOutput<unknown>>,
                activeTurnWaiter.promise,
                completion.then(
                  () => undefined,
                  () => undefined,
                ),
                delay(MESSAGE_ADMISSION_DURABLE_WAIT_TIMEOUT_MS).then(() => undefined),
              ])) as MastraModelOutput<unknown> | undefined);
        } catch (err) {
          finishOwnedMessageTurn();
          void completion.catch(() => {});
          const waiter = this._runCompletionPromises.get(signal.runId);
          this._runCompletionPromises.delete(signal.runId);
          this._rememberCompletedRun(signal.runId, { ok: false, err });
          waiter?.reject(err);
          if (admissionIdentity !== undefined && this._shouldWriteTurnFailureEvidence(err)) {
            this._writeMessageResultEvidenceBestEffortInBackground(
              {
                status: 'failed',
                signalId: signal.signal.id,
                runId: signal.runId,
                modeId: effectiveModeId,
                modelId: effectiveModelId,
                error: projectHarnessPublicError(err),
                admissionId: opts.admissionId!,
                admissionHash: admissionHash!,
              },
              { compatibleAdmissionHashes },
            );
          }
          if (opts.admissionId !== undefined) this._messageAdmissionStarts.delete(opts.admissionId);
          void this._maybeDrainQueue();
          throw err;
        }
      }
      if (!out) {
        const err = new HarnessConfigError('message()', 'agent did not register a run for the dispatched signal');
        // Drop the completion waiter so duplicate retries do not treat an
        // unregistered run as live forever.
        await failDispatchedMessageTurn(err);
        throw err;
      }
      try {
        await awaitPendingMessageEvidence();
      } catch (err) {
        await failDispatchedMessageTurn(err);
        throw err;
      }
      let streamCompletedEvidenceWriteFailed = false;
      let streamAgentEndEmitted = false;
      const streamBookkeeping = Promise.race([completion, activeTurnWaiter.promise])
        .then(async full => {
          try {
            if (full.finishReason === 'suspended') {
              await this._captureMessageSuspendWithTokenUsage(
                full,
                undefined,
                effectiveModeId,
                effectiveModelId,
                activeTurnWaiter.promise,
              );
            } else {
              const { tokenUsageAccounted } = this._recordMessageTurnCompletion(full, { persist: false });
              if (tokenUsageAccounted) {
                await Promise.race([this._persistTokenUsageOrLatch(), activeTurnWaiter.promise]);
              }
            }
          } catch (err) {
            this._latchDurableTurnFlushError(err, full);
            throw err;
          }
          if (admissionIdentity === undefined) return full;
          await Promise.race([
            this._writeMessageResultEvidence(
              {
                status: 'completed',
                signalId: signal.signal.id,
                runId: signal.runId,
                modeId: effectiveModeId,
                modelId: effectiveModelId,
                result: full,
                admissionId: opts.admissionId!,
                admissionHash: admissionHash!,
              },
              { compatibleAdmissionHashes },
            ).catch(err => {
              streamCompletedEvidenceWriteFailed = true;
              throw err;
            }),
            activeTurnWaiter.promise,
          ]);
          return full;
        })
        .then(async full => {
          this._emitAgentEnd({ runId: full.runId, finishReason: this._agentEndReasonForFullOutput(full), full });
          streamAgentEndEmitted = true;
          await Promise.race([this._runGoalJudge(full, false), activeTurnWaiter.promise]);
        })
        .catch(err => {
          if (
            admissionIdentity !== undefined &&
            !streamCompletedEvidenceWriteFailed &&
            this._shouldWriteTurnFailureEvidence(err)
          ) {
            void this._writeMessageResultEvidence(
              {
                status: 'failed',
                signalId: signal.signal.id,
                runId: signal.runId,
                modeId: effectiveModeId,
                modelId: effectiveModelId,
                error: projectHarnessPublicError(err),
                admissionId: opts.admissionId!,
                admissionHash: admissionHash!,
              },
              { compatibleAdmissionHashes },
            ).catch(() => {});
          }
          if (!streamAgentEndEmitted) {
            this._emitTurnEvent({
              type: 'agent_end',
              finishReason: turnAbortSignal.aborted ? 'aborted' : 'error',
              runId: signal.runId,
              usage: this._runUsage(),
            });
          }
          // The caller owns the visible stream; swallow drain-side errors.
        })
        .finally(() => {
          if (opts.admissionId !== undefined) this._deleteMessageAdmissionStartSoon(opts.admissionId);
          finishOwnedMessageTurn();
          void this._maybeDrainQueue();
        });
      void this._trackBackgroundTurnCompletion(streamBookkeeping);
      return out;
    }

    // Default path: wait for stream startup and the completion watcher to
    // deliver this run's bundled `FullOutput`, then run post-turn bookkeeping.
    let streamStarted = signal.output === undefined;
    let completedEvidenceWriteFailed = false;
    let agentEndEmitted = false;
    try {
      // The pre-dispatch reservation is the durable admission barrier here.
      // Keep the post-dispatch pending refresh best-effort so completion
      // evidence remains the authoritative default-path result.
      await Promise.race([pendingEvidenceWrite.catch(() => {}), activeTurnWaiter.promise]);
      resolveMessageAdmissionStart();
      if (signal.output) {
        await Promise.race([signal.output, activeTurnWaiter.promise]);
        streamStarted = true;
      }
      const full = await Promise.race([completion, activeTurnWaiter.promise]);
      try {
        if (full.finishReason === 'suspended') {
          await this._captureMessageSuspendWithTokenUsage(
            full,
            undefined,
            effectiveModeId,
            effectiveModelId,
            activeTurnWaiter.promise,
          );
        } else {
          const { tokenUsageAccounted } = this._recordMessageTurnCompletion(full, { persist: false });
          if (tokenUsageAccounted) {
            await Promise.race([this._persistTokenUsageOrLatch(), activeTurnWaiter.promise]);
          }
        }
        if (admissionIdentity !== undefined) {
          await Promise.race([
            this._writeMessageResultEvidenceBestEffort(
              {
                status: 'completed',
                signalId: signal.signal.id,
                runId: signal.runId,
                modeId: effectiveModeId,
                modelId: effectiveModelId,
                result: full,
                admissionId: opts.admissionId!,
                admissionHash: admissionHash!,
              },
              { compatibleAdmissionHashes },
            ).catch(err => {
              completedEvidenceWriteFailed = true;
              throw err;
            }),
            activeTurnWaiter.promise,
          ]);
        }
      } catch (err) {
        throw err;
      }
      this._emitAgentEnd({ runId: full.runId, finishReason: this._agentEndReasonForFullOutput(full), full });
      agentEndEmitted = true;
      await Promise.race([this._runGoalJudge(full, false), activeTurnWaiter.promise]);
      return full;
    } catch (err) {
      if (!streamStarted) {
        void completion.catch(() => {});
        const waiter = this._runCompletionPromises.get(signal.runId);
        this._runCompletionPromises.delete(signal.runId);
        this._rememberCompletedRun(signal.runId, { ok: false, err });
        waiter?.reject(err);
      }
      if (
        admissionIdentity !== undefined &&
        !completedEvidenceWriteFailed &&
        this._shouldWriteTurnFailureEvidence(err)
      ) {
        await Promise.race([
          this._writeMessageResultEvidence(
            {
              status: 'failed',
              signalId: signal.signal.id,
              runId: signal.runId,
              modeId: effectiveModeId,
              modelId: effectiveModelId,
              error: projectHarnessPublicError(err),
              admissionId: opts.admissionId!,
              admissionHash: admissionHash!,
            },
            { compatibleAdmissionHashes },
          ).catch(() => {}),
          activeTurnWaiter.promise,
        ]);
      }
      if (!agentEndEmitted) {
        this._emitTurnEvent({
          type: 'agent_end',
          finishReason: turnAbortSignal.aborted ? 'aborted' : 'error',
          runId: signal.runId,
          usage: this._runUsage(),
        });
      }
      // §13.3f.1 — `message()` is a public §4.2b boundary; its promise
      // rejection must not leak raw provider/driver/SQL/path text. Redact a raw
      // cause into a `HarnessExecutionError` (safe `.message`, raw `.cause`
      // local-only); Harness-own errors pass through with their typed message.
      // The wire/event/durable surfaces are unchanged (they already project via
      // `projectHarnessPublicError`, which maps both shapes to `harness.internal`).
      throw redactPublicBoundaryRejection(err);
    } finally {
      if (opts.admissionId !== undefined) this._messageAdmissionStarts.delete(opts.admissionId);
      finishOwnedMessageTurn();
      // Now that the manual turn has cleared the in-flight guard, kick
      // the queue drain so any item that was admitted mid-turn can run.
      void this._maybeDrainQueue();
    }
  }

  /**
   * Admit a default message turn and return the durable signal identity
   * without awaiting the eventual AgentResult. Remote HTTP routes use this
   * surface to preserve local `message(...)` promise semantics in the SDK:
   * the POST only proves admission, and SSE/result lookup settle the result.
   */
  async admitMessage(opts: MessageOptionsDefault): Promise<MessageAdmissionResult> {
    this._assertLive('admitMessage()');
    if (opts.admissionId === undefined || opts.admissionId.length === 0) {
      throw new HarnessValidationError('admitMessage().admissionId', 'admissionId must be a non-empty string');
    }
    if (opts.output !== undefined || opts.sync !== undefined || opts.stream !== undefined) {
      throw new HarnessConfigError('admitMessage()', 'admitMessage only accepts default message options');
    }
    if (opts.additionalTools !== undefined) {
      throw new HarnessValidationError(
        'admitMessage().admissionId',
        'admissionId cannot be combined with additionalTools',
      );
    }

    const effectiveModeId = opts.mode ?? this._record.modeId;
    const admissionHashes = this._computeMessageAdmissionHashes(opts, {
      modeId: effectiveModeId,
      modelId: opts.model ?? this._record.modelId,
    });
    const duplicate = await this._resolveMessageAdmissionDuplicate({
      admissionId: opts.admissionId,
      admissionHash: admissionHashes.primary,
      compatibleAdmissionHashes: admissionHashes.legacyCompatible,
    });
    if (duplicate) {
      this._assertOpenForTurn('admitMessage()');
      const signalId = duplicate.signalId;
      if (signalId === undefined) {
        throw new HarnessValidationError('admitMessage().admissionId', 'duplicate message result evidence has expired');
      }
      return {
        accepted: true,
        signalId,
        ...(duplicate.runId !== undefined ? { runId: duplicate.runId } : {}),
        duplicate: true,
      };
    }

    const existingStart = this._messageAdmissionStarts.get(opts.admissionId);
    if (existingStart) {
      if (existingStart.admissionHash !== admissionHashes.primary) {
        throw new HarnessAdmissionConflictError(
          this.id,
          opts.admissionId,
          existingStart.admissionHash,
          admissionHashes.primary,
        );
      }
      const evidence = await existingStart.promise;
      const signalId = evidence.signalId;
      if (signalId === undefined) {
        throw new HarnessValidationError('admitMessage().admissionId', 'message admission evidence has expired');
      }
      return {
        accepted: true,
        signalId,
        ...(evidence.runId !== undefined ? { runId: evidence.runId } : {}),
        duplicate: true,
      };
    }

    const streamPromise = this.message({ ...opts, stream: true });
    void streamPromise.catch(() => {});
    const admissionStart = await this._waitForMessageAdmissionStart(opts.admissionId, streamPromise);
    const evidence =
      admissionStart.started !== undefined
        ? await admissionStart.started.promise
        : await this._resolveMessageAdmissionDuplicate({
            admissionId: opts.admissionId,
            admissionHash: admissionHashes.primary,
            compatibleAdmissionHashes: admissionHashes.legacyCompatible,
          });
    if (evidence === undefined && admissionStart.streamError !== undefined) {
      throw admissionStart.streamError;
    }
    if (evidence === undefined) {
      throw new HarnessConfigError('admitMessage()', 'message admission evidence was not recorded');
    }
    const signalId = evidence.signalId;
    if (signalId === undefined) {
      throw new HarnessValidationError('admitMessage().admissionId', 'message admission evidence has expired');
    }
    return {
      accepted: true,
      signalId,
      ...(evidence.runId !== undefined ? { runId: evidence.runId } : {}),
      duplicate: admissionStart.started === undefined,
    };
  }

  private async _waitForMessageAdmissionStart(
    admissionId: string,
    streamPromise: Promise<unknown>,
  ): Promise<{ started?: MessageAdmissionStart; streamError?: unknown }> {
    const settled: { status: 'pending' | 'fulfilled' | 'rejected'; error?: unknown } = { status: 'pending' };
    void streamPromise.then(
      () => {
        settled.status = 'fulfilled';
      },
      error => {
        settled.status = 'rejected';
        settled.error = error;
      },
    );

    while (true) {
      const started = this._messageAdmissionStarts.get(admissionId);
      if (started) return { started };
      if (settled.status === 'rejected') return { streamError: settled.error };
      if (settled.status === 'fulfilled') {
        return {};
      }
      await delay(0);
    }
  }

  private _deleteMessageAdmissionStartSoon(admissionId: string): void {
    const timer = setTimeout(() => {
      this._messageAdmissionStarts.delete(admissionId);
    }, 0);
    timer.unref?.();
  }

  private async _resolveMessageAdmissionDuplicate({
    admissionId,
    admissionHash,
    compatibleAdmissionHashes,
  }: {
    admissionId: string;
    admissionHash: string;
    compatibleAdmissionHashes?: readonly string[];
  }): Promise<AgentSignalResultEvidence | OperationAdmissionTombstone | undefined> {
    const resolved = await this._storage.resolveOperationAdmissionEvidence({
      harnessName: this._record.harnessName,
      sessionId: this.id,
      resourceId: this.resourceId,
      threadId: this.threadId,
      kind: 'signal',
      admissionId,
      attemptedAdmissionHash: admissionHash,
    });
    if (resolved.status === 'none') return undefined;
    if (resolved.status === 'conflict') {
      if (
        resolved.storedAdmissionHash !== undefined &&
        compatibleAdmissionHashes?.includes(resolved.storedAdmissionHash)
      ) {
        return resolved.evidence as AgentSignalResultEvidence | OperationAdmissionTombstone | undefined;
      }
      throw new HarnessAdmissionConflictError(this.id, admissionId, resolved.storedAdmissionHash ?? '', admissionHash);
    }
    return resolved.evidence as AgentSignalResultEvidence | OperationAdmissionTombstone | undefined;
  }

  private async _returnDuplicateMessageResult(
    evidence: AgentSignalResultEvidence | OperationAdmissionTombstone,
    opts: MessageOptions,
  ): Promise<AgentResult | AgentStream | unknown> {
    return this._withActiveDeletedWaiter(async activeDeleted => {
      if ('status' in evidence) {
        if (opts.stream === true) {
          if (evidence.status === 'pending') {
            const duplicateModeId = this._messageDuplicateModeId(evidence, opts);
            const duplicateModelId = this._messageDuplicateModelId(evidence, opts);
            const agent = this._harness.getAgentForMode(duplicateModeId);
            await this._raceActiveTurnWaiter(this._ensureThreadSubscription(agent), activeDeleted);
            const runId = await this._pendingMessageRunId(evidence);
            if (runId && this._completedRuns.has(runId)) {
              const cached = this._completedRuns.get(runId);
              if (cached?.ok && evidence.admissionId !== undefined && evidence.admissionHash !== undefined) {
                await this._prepareCachedDuplicateMessageCompletion(cached.full, evidence, opts, activeDeleted);
                await this._writeMessageResultEvidenceBestEffort({
                  status: 'completed',
                  signalId: evidence.signalId,
                  runId,
                  modeId: duplicateModeId,
                  modelId: duplicateModelId,
                  result: cached.full,
                  admissionId: evidence.admissionId,
                  admissionHash: evidence.admissionHash,
                });
              }
              throw new HarnessValidationError('message().admissionId', 'duplicate stream is no longer live');
            }
            let output = runId ? (agent.getRunOutput(runId) as AgentStream | undefined) : undefined;
            let retainedCompletedOutput = false;
            if (
              output &&
              (output as { status?: string }).status !== undefined &&
              (output as { status?: string }).status !== 'running'
            ) {
              retainedCompletedOutput = true;
              output = undefined;
            }
            if (runId && !output && !retainedCompletedOutput) {
              const waitAbortController = new AbortController();
              const completion = this._runCompletionPromises.get(runId)?.promise.then(
                () => undefined,
                () => undefined,
              );
              try {
                output = (await Promise.race([
                  this._raceActiveTurnWaiter(
                    (
                      agent.waitForRunOutput(runId, { abortSignal: waitAbortController.signal }) as Promise<AgentStream>
                    ).catch(() => undefined),
                    activeDeleted,
                  ),
                  ...(completion ? [completion] : []),
                  delay(MESSAGE_ADMISSION_DURABLE_WAIT_TIMEOUT_MS, waitAbortController.signal).then(
                    () => undefined,
                    () => undefined,
                  ),
                ])) as AgentStream | undefined;
              } finally {
                waitAbortController.abort(
                  new HarnessValidationError('message().admissionId', 'duplicate stream wait ended'),
                );
              }
            }
            if (output) return output;
          }
          throw new HarnessValidationError('message().admissionId', 'duplicate stream is no longer live');
        }
        if (evidence.status === 'completed') return evidence.result as AgentResult;
        if (evidence.status === 'failed') throw publicErrorProjectionToError(evidence.error);
        const runId = await this._pendingMessageRunId(evidence);
        if (runId) {
          const duplicateModeId = this._messageDuplicateModeId(evidence, opts);
          const duplicateModelId = this._messageDuplicateModelId(evidence, opts);
          const agent = this._harness.getAgentForMode(duplicateModeId);
          await this._raceActiveTurnWaiter(this._ensureThreadSubscription(agent), activeDeleted);
          const cached = this._completedRuns.get(runId);
          if (cached) {
            if (!cached.ok) throw cached.err;
            if (evidence.admissionId !== undefined && evidence.admissionHash !== undefined) {
              await this._prepareCachedDuplicateMessageCompletion(cached.full, evidence, opts, activeDeleted);
              await this._writeMessageResultEvidenceBestEffort({
                status: 'completed',
                signalId: evidence.signalId,
                runId,
                modeId: duplicateModeId,
                modelId: duplicateModelId,
                result: cached.full,
                admissionId: evidence.admissionId,
                admissionHash: evidence.admissionHash,
              });
            }
            return cached.full;
          }
          if (!this._hasLiveMessageRun(agent, runId)) {
            return this._raceActiveTurnWaiter(this._awaitDurableMessageResult(evidence, opts), activeDeleted);
          }
          return this._raceActiveTurnWaiter(this._awaitRunCompletion(runId), activeDeleted);
        }
      }
      throw new HarnessValidationError('message().admissionId', 'duplicate message result evidence has expired');
    });
  }

  private async _prepareCachedDuplicateMessageCompletion(
    full: FullOutput<unknown>,
    evidence: AgentSignalResultEvidence,
    opts: MessageOptions,
    activeDeleted?: Promise<never>,
  ): Promise<void> {
    if (full.finishReason === 'suspended') {
      await this._captureMessageSuspendWithTokenUsage(
        full,
        undefined,
        this._messageDuplicateModeId(evidence, opts),
        this._messageDuplicateModelId(evidence, opts),
        activeDeleted,
      );
      return;
    }

    const { tokenUsageAccounted } = this._recordMessageTurnCompletion(full, { persist: false });
    if (tokenUsageAccounted) {
      await this._raceActiveTurnWaiter(this._persistTokenUsageOrLatch(), activeDeleted);
    }
  }

  private async _pendingMessageRunId(evidence: AgentSignalResultEvidence): Promise<string | undefined> {
    if (evidence.status !== 'pending') return evidence.runId;
    const starting = evidence.admissionId ? this._messageAdmissionStarts.get(evidence.admissionId) : undefined;
    if (!starting) return evidence.runId;
    try {
      const startingEvidence = await starting.promise;
      return startingEvidence.runId ?? evidence.runId;
    } catch {
      return evidence.runId;
    }
  }

  private _messageDuplicateModeId(evidence: AgentSignalResultEvidence, opts: MessageOptions): string {
    const starting = evidence.admissionId ? this._messageAdmissionStarts.get(evidence.admissionId) : undefined;
    return starting?.modeId ?? evidence.modeId ?? opts.mode ?? this._record.modeId;
  }

  private _messageDuplicateModelId(evidence: AgentSignalResultEvidence, opts: MessageOptions): string {
    const starting = evidence.admissionId ? this._messageAdmissionStarts.get(evidence.admissionId) : undefined;
    return starting?.modelId ?? evidence.modelId ?? opts.model ?? this._record.modelId;
  }

  private _hasLiveMessageRun(agent: Agent, runId: string): boolean {
    return Boolean(
      agent.getRunOutput(runId) || this._runCompletionPromises.has(runId) || this._completedRuns.has(runId),
    );
  }

  private async _awaitDurableMessageResult(
    evidence: AgentSignalResultEvidence,
    opts: MessageOptions,
  ): Promise<AgentResult> {
    const deadline = Date.now() + MESSAGE_ADMISSION_DURABLE_WAIT_TIMEOUT_MS;
    while (true) {
      throwIfAborted(opts.abortSignal, 'message().admissionId');
      const latest = await this._storage.loadMessageResultEvidence({
        harnessName: this._record.harnessName,
        sessionId: this.id,
        resourceId: this.resourceId,
        threadId: this.threadId,
        signalId: evidence.signalId,
      });
      if (!latest) {
        throw new HarnessValidationError('message().admissionId', 'duplicate message result evidence has expired');
      }
      if ('status' in latest) {
        if (latest.status === 'completed') return latest.result as AgentResult;
        if (latest.status === 'failed') throw publicErrorProjectionToError(latest.error);
      } else {
        throw new HarnessValidationError('message().admissionId', 'duplicate message result evidence has expired');
      }
      if (Date.now() >= deadline) {
        throw new HarnessValidationError('message().admissionId', 'pending message admission is not live');
      }
      await delay(MESSAGE_ADMISSION_DURABLE_WAIT_INTERVAL_MS, opts.abortSignal);
    }
  }

  private _messageAdmissionIdentity(admissionId: string): MessageAdmissionIdentity {
    const digest = sha256CanonicalJson({
      kind: 'message-admission',
      harnessName: this._record.harnessName,
      sessionId: this.id,
      resourceId: this.resourceId,
      threadId: this.threadId,
      admissionId,
    });
    return {
      signalId: `harness-message-${digest.slice(0, 32)}`,
      runId: `harness-message-${digest.slice(32, 64)}`,
    };
  }

  /**
   * @internal §14.2 deterministic per-`admissionId` signal id for the channel
   * signal-delivery admission path. Unlike {@link _messageAdmissionIdentity},
   * only the `signalId` is deterministic — a signal interleaves into whatever
   * run is active, so its `runId` is not predetermined and is recorded on the
   * reservation after dispatch. The id is namespaced (`harness-channel-signal-`)
   * so it never collides with the message/queue admission id spaces.
   */
  private _channelSignalAdmissionSignalId(admissionId: string): string {
    const digest = sha256CanonicalJson({
      kind: 'channel-signal-admission',
      harnessName: this._record.harnessName,
      sessionId: this.id,
      resourceId: this.resourceId,
      threadId: this.threadId,
      admissionId,
    });
    return `harness-channel-signal-${digest.slice(0, 32)}`;
  }

  private async _writeMessageResultEvidence(
    status: AgentSignalResultStatus & { admissionId?: string; admissionHash?: string },
    options?: { compatibleAdmissionHashes?: readonly string[] },
  ): Promise<{ created: boolean; evidence?: AgentSignalResultEvidence | OperationAdmissionTombstone }> {
    const now = Date.now();
    this._operationEvidenceSignalIds.add(status.signalId);
    try {
      const result = await this._storage.writeMessageResultEvidence({
        ...status,
        harnessName: this._record.harnessName,
        sessionId: this.id,
        resourceId: this.resourceId,
        threadId: this.threadId,
        createdAt: now,
        updatedAt: now,
      });
      await this._cleanupOperationEvidenceIfDeleted(status);
      return result;
    } catch (err) {
      if (err instanceof HarnessStorageAdmissionConflictError && status.admissionId && status.admissionHash) {
        const duplicate = await this._resolveMessageAdmissionDuplicate({
          admissionId: status.admissionId,
          admissionHash: status.admissionHash,
          compatibleAdmissionHashes: options?.compatibleAdmissionHashes,
        });
        if (duplicate) return { created: false, evidence: duplicate };
        throw new HarnessAdmissionConflictError(this.id, status.admissionId, '', status.admissionHash);
      }
      throw err;
    }
  }

  private async _cleanupOperationEvidenceIfDeleted(status: { signalId: string }): Promise<void> {
    if (this._state !== 'deleted') return;
    await this._storage
      .deleteOperationAdmissionTombstonesForSession({
        harnessName: this._record.harnessName,
        sessionId: this.id,
        resourceId: this.resourceId,
        threadId: this.threadId,
        signalId: status.signalId,
      })
      .catch(() => {});
  }

  private async _writeMessageResultEvidenceBestEffort(
    status: AgentSignalResultStatus & { admissionId?: string; admissionHash?: string },
    options?: { compatibleAdmissionHashes?: readonly string[] },
  ): Promise<void> {
    try {
      await this._writeMessageResultEvidence(status, options);
    } catch (err) {
      if (status.admissionId !== undefined) throw err;
      // The initial pre-dispatch admission reservation is the durable barrier.
      // Non-idempotent callers have no durable replay contract, so storage
      // evidence is only best-effort for them.
    }
  }

  private _writeMessageResultEvidenceBestEffortInBackground(
    status: AgentSignalResultStatus & { admissionId?: string; admissionHash?: string },
    options?: { compatibleAdmissionHashes?: readonly string[] },
  ): void {
    const write = this._writeMessageResultEvidenceBestEffort(status, options);
    let timer: ReturnType<typeof setTimeout> | undefined;
    const timeout = new Promise<void>(resolve => {
      timer = setTimeout(resolve, MESSAGE_RESULT_EVIDENCE_BACKGROUND_OBSERVE_TIMEOUT_MS);
      timer.unref?.();
    });
    void Promise.race([write, timeout])
      .catch(() => {})
      .finally(() => {
        if (timer !== undefined) clearTimeout(timer);
      });
  }

  /**
   * §4.2f / §10.2 signal-result boundary. Settle the durable per-`signalId`
   * result evidence and project the matching OperationEvent. Owned-turn (1:1)
   * signals call this from the run terminal — the run's output IS the answer
   * attributable to that signal, so `agent_end` is *not* the settlement
   * boundary; `signal_completed` / `signal_failed` are. Evidence is best-effort
   * for non-`admissionId` callers (the run terminal is the live settlement and
   * `lookupMessageResult(signalId)` is the durable recovery path).
   *
   * Interleaved active-run signals ALSO settle through this helper, but with a
   * documented caveat: a run can answer several drained signals, so each
   * interleaved `signalId` is currently settled from the SHARED run terminal
   * (a run-aggregate result, not a per-segment distinct answer). This is the
   * interim until the runtime emits per-signal terminal markers
   * (`AgentThreadStreamRuntime` per-segment attribution); the alternative —
   * leaving interleaved evidence `pending` forever — is worse for recovery, so
   * the shared-run terminal is used as the best available answer. See the
   * call-site comment in `signal()`'s active-delivery path.
   */
  private async _settleSignalResult(
    signalId: string,
    outcome:
      | { status: 'completed'; runId: string; result: AgentResult }
      | { status: 'failed'; runId?: string; error: { code: string; message: string } },
  ): Promise<void> {
    if (outcome.status === 'completed') {
      await this._writeMessageResultEvidenceBestEffort({
        status: 'completed',
        signalId,
        runId: outcome.runId,
        result: outcome.result,
      });
      this._emit({ type: 'signal_completed', runId: outcome.runId, signalId, result: outcome.result });
      return;
    }
    await this._writeMessageResultEvidenceBestEffort({
      status: 'failed',
      signalId,
      ...(outcome.runId !== undefined ? { runId: outcome.runId } : {}),
      error: outcome.error,
    });
    this._emit({
      type: 'signal_failed',
      signalId,
      ...(outcome.runId !== undefined ? { runId: outcome.runId } : {}),
      error: outcome.error,
    });
  }

  private _computeMessageAdmissionHashes(
    opts: MessageOptions,
    stable: { modeId: string; modelId: string },
    persistedRequestContext?: PersistedRequestContextInput,
  ): MessageAdmissionHashes {
    const primary = sha256CanonicalJson(
      this._messageAdmissionHashInput(opts, undefined, { hashVersion: 2 }, persistedRequestContext),
    );
    // Pre-v2 evidence hashed the effective mode/model. Keep compatibility
    // candidates for the current effective tuple only; old evidence does not
    // persist enough metadata to safely infer previous defaults after drift.
    const legacyCompatible = [
      sha256CanonicalJson(this._messageAdmissionHashInput(opts, stable, undefined, persistedRequestContext)),
      sha256CanonicalJson(
        this._messageAdmissionHashInput(opts, stable, { includeAttachmentMetadata: false }, persistedRequestContext),
      ),
    ];
    return {
      primary,
      legacyCompatible: [...new Set(legacyCompatible)].filter(hash => hash !== primary),
    };
  }

  private _messageAdmissionHashInput(
    opts: MessageOptions,
    stable?: { modeId: string; modelId: string },
    options?: { hashVersion?: number; includeAttachmentMetadata?: boolean },
    persistedRequestContext?: PersistedRequestContextInput,
  ) {
    return {
      // Operation-kind discriminator in the admission hash (signal vs queue);
      // §5.1d token is `'signal'`. Distinct from the queue path's `kind: 'queue'`.
      kind: 'signal',
      ...(options?.hashVersion !== undefined ? { hashVersion: options.hashVersion } : {}),
      content: opts.content,
      ...(stable !== undefined
        ? { mode: stable.modeId, model: stable.modelId }
        : {
            ...(opts.mode !== undefined ? { mode: opts.mode } : {}),
            ...(opts.model !== undefined ? { model: opts.model } : {}),
          }),
      attachments: (opts.attachments ?? []).map(attachment => ({
        attachmentId: attachment.attachmentId,
        resourceId: attachment.resourceId,
        ...(attachment.ownerSessionId !== undefined ? { ownerSessionId: attachment.ownerSessionId } : {}),
        ...(attachment.bytes !== undefined ? { bytes: attachment.bytes } : {}),
        ...(attachment.sha256 !== undefined ? { sha256: attachment.sha256 } : {}),
        ...(attachment.source !== undefined ? { source: attachment.source } : {}),
        ...(options?.includeAttachmentMetadata !== false
          ? {
              ...(attachment.kind !== undefined ? { kind: attachment.kind } : {}),
              ...(attachment.name !== undefined ? { name: attachment.name } : {}),
              ...(attachment.mimeType !== undefined ? { mimeType: attachment.mimeType } : {}),
              ...(attachment.primitiveType !== undefined ? { primitiveType: attachment.primitiveType } : {}),
              ...(attachment.elementType !== undefined ? { elementType: attachment.elementType } : {}),
              ...(attachment.renderer !== undefined ? { renderer: attachment.renderer } : {}),
              ...(attachment.schemaId !== undefined ? { schemaId: attachment.schemaId } : {}),
              ...(attachment.metadata !== undefined ? { metadata: cloneAttachmentMetadata(attachment.metadata) } : {}),
              ...(attachment.object !== undefined ? { object: attachment.object } : {}),
            }
          : {}),
      })),
      // §4.4c / §5.1: caller request context is part of admission identity when
      // present. Absent => omitted => the hash is byte-identical to pre-feature
      // evidence (backward-compatible). Mirrors the queue path's requestContext.
      ...(persistedRequestContext
        ? { requestContext: clonePersistedRequestContext(persistedRequestContext) }
        : {}),
    };
  }

  // -------------------------------------------------------------------------
  // signal() — §4.2.
  //
  // Optimistic user-message primitive. Resolves with the routing decision
  // (`id`, `runId`, `willInterleave`) on the first await tick so callers
  // can render an optimistic transcript row before the turn completes,
  // then await `result` for the eventual `AgentResult`.
  //
  // Two delivery shapes:
  //
  //   * Idle thread → wakes a fresh run. This call owns the turn:
  //     `_beginTurn`, `agent_start`, await completion in a background
  //     continuation, `agent_end` + judge + `_endTurn` + drain.
  //
  //   * Active-delivery → an existing run is in flight on this thread.
  //     The signal drains mid-flight into the running execution loop;
  //     no new turn boundary, no `agent_start`/`agent_end`. `result`
  //     resolves with the existing run's `AgentResult`.
  //
  // Per-turn overrides (`mode`, `additionalTools`) on an active-delivery
  // dispatch reject at admission with `HarnessOverrideConflictError` —
  // the in-flight run's surface was committed when it started and cannot
  // be changed mid-flight.
  // -------------------------------------------------------------------------
  async signal(
    opts: SessionSignalOptions,
    /**
     * @internal §14.2 channel-ingress trusted hook. The channel bridge supplies
     * a pre-built trusted `requestContext.channel` projection (NOT caller input —
     * it bypasses {@link validateCallerRequestContext}) and the already-persisted
     * inbox attachments. On idle-wake the trusted context rides the fresh run; on
     * active-delivery (§21 shared terminal) the content interleaves and the
     * in-flight run's committed context stands — the channel context cannot reach
     * the in-flight run's already-committed tools, so it is intentionally not
     * re-applied there. No SDK caller supplies both `opts.requestContext` and
     * this (channel ingress has no caller app bag).
     */
    internal?: {
      persistedRequestContext?: PersistedRequestContextInput;
      attachments?: PersistedAttachment[];
      /**
       * §14.2 channel-ingress idempotency: the durable per-`admissionId` signal
       * id minted by {@link _admitChannelSignalTurn}. Stamping the dispatched
       * signal with this deterministic id (instead of a fresh random one) ties
       * the in-flight run to the reservation written before dispatch, so a
       * recovery replay short-circuits to the SAME run instead of firing a
       * second interleave/wake. No SDK caller supplies this — only channel
       * ingress.
       */
      signalId?: string;
    },
  ): Promise<SessionSignalResult> {
    this._assertLive('signal()');
    this._assertOpenForTurn('signal()');
    if (typeof opts.content !== 'string') {
      throw new HarnessValidationError('signal()', '`content` must be a string');
    }

    // §4.4c: validate caller request context before the thread subscription
    // (an observable side effect) and before the routing decision. The trusted
    // channel projection (internal) is a top-level sibling of the caller `app`
    // bag — combine, never deep-merge (mirrors `_admitQueue`).
    const callerRequestContext = validateCallerRequestContext(opts.requestContext, 'signal()');
    const callerPersistedRequestContext = callerRequestContextToPersisted(callerRequestContext);
    const persistedRequestContext: PersistedRequestContextInput | undefined =
      internal?.persistedRequestContext !== undefined || callerPersistedRequestContext !== undefined
        ? { ...(internal?.persistedRequestContext ?? {}), ...(callerPersistedRequestContext ?? {}) }
        : undefined;

    // Resolve effective mode + backing agent.
    const effectiveModeId = opts.mode ?? this._record.modeId;
    const mode = this._harness._getMode(effectiveModeId);
    const agent = this._harness.getAgentForMode(effectiveModeId);

    // Open the thread subscription before reading `activeRunId()` so the
    // routing decision sees the live runtime state.
    const subscriptionWaiter = this._createActiveTurnWaiter();
    void subscriptionWaiter.promise.catch(() => {});
    const subscription = this._ensureThreadSubscription(agent);
    void subscription.catch(() => {});
    const sub = await Promise.race([subscription, subscriptionWaiter.promise]).finally(() => {
      subscriptionWaiter.cleanup();
    });
    this._assertLive('signal()');
    this._assertOpenForTurn('signal()');

    const activeRunId = sub.activeRunId();
    const willInterleave = activeRunId !== null;

    // Active-delivery + per-turn overrides → reject at admission.
    if (willInterleave) {
      // §3 / §4.5a: the in-flight run's surface (mode/model/tools) is committed
      // at start, so a signal that *carries* a per-turn mode override and drains
      // into the active run is rejected regardless of the override's value —
      // keying off `opts.mode` presence, not whether it differs from the session
      // default. (The session-default comparison missed the case where the
      // active run itself started with a per-turn mode override.)
      if (opts.mode !== undefined) {
        throw new HarnessOverrideConflictError(this.id, activeRunId!, ['mode']);
      }
      if (opts.additionalTools !== undefined) {
        throw new HarnessOverrideConflictError(this.id, activeRunId!, ['addTools']);
      }
      // Request context cannot reach an in-flight run's tools (its streamOptions
      // were committed at start), so reject rather than silently drop the app bag.
      if (callerRequestContext !== undefined) {
        throw new HarnessConfigError(
          'signal().requestContext',
          'cannot be supplied on an active-delivery signal — the in-flight run already committed its request context and could not receive a new app bag',
        );
      }
    }

    if (!willInterleave) {
      // Owned-turn path: same bookkeeping as the message() default path.
      // `signal()` supports a per-turn `mode` override but not `model`, so the
      // effective model is the session default.
      const turnAbortController = this._beginTurn(opts.abortSignal, {
        modeId: effectiveModeId,
        modelId: this._record.modelId,
      });
      const turnAbortSignal = turnAbortController.signal;
      const activeTurnWaiter = this._createActiveTurnWaiter();
      void activeTurnWaiter.promise.catch(() => {});
      const finishOwnedSignalTurn = () => {
        activeTurnWaiter.cleanup();
        this._endTurn(turnAbortController);
      };
      const assertOwnedSignalTurnNotDeleted = () => {
        if (this._state === 'deleted') {
          throw new HarnessSessionDeletedError(this.id, this._record.resourceId, this._record.threadId);
        }
      };
      let dispatched;
      try {
        const toolsets = this._buildToolsets(mode, opts.additionalTools);
        const requestContext = await Promise.race([
          this._buildRequestContext({
            modeId: effectiveModeId,
            modelId: this._record.modelId,
            abortSignal: turnAbortSignal,
            ...(persistedRequestContext ? { persistedRequestContext } : {}),
          }),
          activeTurnWaiter.promise,
        ]);
        const baseExecOptions: AgentExecutionOptionsBase<unknown> = {
          memory: { thread: this.threadId, resource: this.resourceId },
          abortSignal: turnAbortSignal,
          requestContext,
          ...(toolsets ? { toolsets } : {}),
          ...(mode.instructions ? { instructions: mode.instructions } : {}),
        };
        assertOwnedSignalTurnNotDeleted();
        this._assertOpenForTurn('signal()');

        // §13.7/§14.2: attach persisted attachment bytes as model file-parts on
        // the idle-wake run. No attachments → bare `opts.content` (unchanged).
        const signalContents = await Promise.race([
          this._buildSignalContentsWithAttachments(opts.content, internal?.attachments),
          activeTurnWaiter.promise,
        ]);
        assertOwnedSignalTurnNotDeleted();
        this._assertOpenForTurn('signal()');

        dispatched = agent.sendSignal(
          {
            ...(internal?.signalId !== undefined ? { id: internal.signalId } : {}),
            type: 'user-message',
            contents: signalContents as never,
          },
          {
            resourceId: this.resourceId,
            threadId: this.threadId,
            ifIdle: { behavior: 'wake', streamOptions: baseExecOptions as never },
          },
        );
      } catch (err) {
        finishOwnedSignalTurn();
        throw err;
      }

      // §10.2 agent_start carries the runId — emit it now the run is dispatched.
      // dispatched.signal.id is the originating signalId — stamped for the run projection.
      this._emitAgentStart(dispatched.runId, dispatched.signal.id);

      // §4.2f: record durable per-`signalId` pending evidence on acceptance so
      // `lookupMessageResult(signalId)` can answer across restarts. Best-effort
      // for this non-`admissionId` path — the run terminal below is the live
      // settlement.
      const ownedSignalId = dispatched.signal.id;
      this._writeMessageResultEvidenceBestEffortInBackground({
        status: 'pending',
        signalId: ownedSignalId,
        runId: dispatched.runId,
      });

      // Register the completion waiter before any terminal chunks land.
      const completion = this._awaitRunCompletion(dispatched.runId);
      const completionOrDelete = Promise.race([completion, activeTurnWaiter.promise]);
      let agentEndEmitted = false;
      let signalSettled = false;

      // Background continuation runs the post-turn bookkeeping so the
      // caller's `result` promise resolves with the final AgentResult.
      const result: Promise<AgentResult> = this._trackBackgroundTurnCompletion(
        completionOrDelete
          .then(async full => {
            const tokenUsageDelta =
              full.finishReason === 'suspended'
                ? this._tokenUsageDeltaFromFullOutput(full)
                : this._recordTurnCompletion(full, { persist: false });
            await Promise.race([
              this._maybeCaptureSuspend(full, undefined, effectiveModeId, this._record.modelId, {
                tokenUsageDelta: full.finishReason === 'suspended' ? tokenUsageDelta : undefined,
                originSignalId: ownedSignalId,
              }).catch(err => {
                this._latchDurableTurnFlushError(err, full);
                throw err;
              }),
              activeTurnWaiter.promise,
            ]);
            if (tokenUsageDelta !== undefined && full.finishReason !== 'suspended') {
              await Promise.race([
                this._persistTokenUsageOrLatch().catch(err => {
                  if (!this._isExpectedFlushLifecycleError(err)) this._pendingTokenUsageFlushError ??= err;
                  throw err;
                }),
                activeTurnWaiter.promise,
              ]);
            }
            this._emitAgentEnd({ runId: full.runId, finishReason: this._agentEndReasonForFullOutput(full), full });
            agentEndEmitted = true;
            // §4.2f/§10.2 — the owned run's terminal IS this signal's answer
            // (1:1). Settle the durable per-`signalId` evidence and project
            // `signal_completed` (the promise/SDK boundary, not `agent_end`).
            //
            // A SUSPENDED turn is parked (pendingResume + suspension_required),
            // NOT answered — leave the evidence `pending` until resume
            // terminalizes; do not write completed evidence or emit
            // `signal_completed`. Settle BEFORE the interruptible goal judge so a
            // goal-judge / active-turn-waiter rejection cannot flip an already
            // answered signal to `signal_failed`.
            if (full.finishReason !== 'suspended') {
              await this._settleSignalResult(ownedSignalId, {
                status: 'completed',
                runId: dispatched.runId,
                result: full as AgentResult,
              });
              signalSettled = true;
            }
            await Promise.race([this._runGoalJudge(full, false), activeTurnWaiter.promise]);
            return full as AgentResult;
          })
          .catch(async err => {
            if (!agentEndEmitted) {
              this._emitTurnEvent({
                type: 'agent_end',
                finishReason: turnAbortSignal.aborted ? 'aborted' : 'error',
                runId: dispatched.runId,
                usage: this._runUsage(),
              });
            }
            // Don't re-settle a signal already answered as completed (a post-run
            // goal-judge/abort rejection must not flip a terminal result).
            if (!signalSettled) {
              await this._settleSignalResult(ownedSignalId, {
                status: 'failed',
                runId: dispatched.runId,
                error: projectHarnessPublicError(err),
              });
            }
            // §13.3f.1 — `handle.result` is a public §4.2b boundary; redact a raw
            // cause before rejecting it (safe `.message`, raw `.cause`
            // local-only). The durable `signal_failed` evidence above is already
            // projected via `projectHarnessPublicError`, so only the in-process
            // promise rejection changes.
            throw redactPublicBoundaryRejection(err);
          })
          .finally(() => {
            finishOwnedSignalTurn();
            void this._maybeDrainQueue();
          }),
      );

      // Swallow `result` rejections at the inner level so the
      // background continuation doesn't surface as an unhandled
      // rejection if the caller never awaits `result`. The caller's
      // copy still rejects.
      void result.catch(() => {});

      return {
        id: dispatched.signal.id,
        runId: dispatched.runId,
        willInterleave: false,
        accepted: true,
        signal: dispatched.signal,
        result,
      };
    }

    // Active-delivery path: signal drains into the existing run. No turn
    // bookkeeping owned here; the in-flight run owns its own completion.
    // Pass empty streamOptions — the runtime ignores them when active. The
    // interleaved content still carries its attachment file-parts; the §21
    // shared-terminal run absorbs them mid-flight (the in-flight run's committed
    // requestContext/surface is unchanged — see the `internal` doc above).
    this._assertOpenForTurn('signal()');
    const interleavedContents = await this._buildSignalContentsWithAttachments(opts.content, internal?.attachments);
    this._assertOpenForTurn('signal()');
    const dispatched = agent.sendSignal(
      {
        ...(internal?.signalId !== undefined ? { id: internal.signalId } : {}),
        type: 'user-message',
        contents: interleavedContents as never,
      },
      {
        resourceId: this.resourceId,
        threadId: this.threadId,
        ifIdle: { behavior: 'wake', streamOptions: {} as never },
      },
    );

    // §4.2f: record durable per-`signalId` pending evidence on acceptance so
    // `lookupMessageResult(signalId)` answers across restarts (not just the
    // process-local run map).
    const interleavedSignalId = dispatched.signal.id;
    this._writeMessageResultEvidenceBestEffortInBackground({
      status: 'pending',
      signalId: interleavedSignalId,
      runId: dispatched.runId,
    });

    // Shared completion promise with whichever caller owns the run.
    const completion = this._awaitRunCompletion(dispatched.runId);
    void completion.catch(() => {});

    // §4.2f/§10.2: settle this signal's durable per-`signalId` evidence from the
    // run it drained into and project signal_completed/failed. The result is the
    // shared run terminal — per-segment distinct-answer attribution for
    // concurrent interleaved signals is a documented runtime refinement (it
    // requires per-segment terminal markers from `AgentThreadStreamRuntime`); a
    // suspended run leaves the evidence pending until resume terminalizes.
    const result = this._trackBackgroundTurnCompletion(
      completion
        .then(async full => {
          if (full.finishReason !== 'suspended') {
            await this._settleSignalResult(interleavedSignalId, {
              status: 'completed',
              runId: dispatched.runId,
              result: full as AgentResult,
            });
          }
          return full as AgentResult;
        })
        .catch(async err => {
          await this._settleSignalResult(interleavedSignalId, {
            status: 'failed',
            runId: dispatched.runId,
            error: projectHarnessPublicError(err),
          });
          throw err;
        }),
    );
    void result.catch(() => {});

    return {
      id: dispatched.signal.id,
      runId: dispatched.runId,
      willInterleave: true,
      accepted: true,
      signal: dispatched.signal,
      result,
    };
  }

  // -------------------------------------------------------------------------
  // injectSystemReminder() — §4.2.
  //
  // System-reminder injection primitive used by goal-judge continuations
  // and other harness-internal nudges. Behaves like `signal()` but with
  // signal type `'system-reminder'` and no exposed `result` promise — the
  // caller doesn't await the run's `AgentResult`. When the reminder wakes
  // an idle thread, full turn bookkeeping still runs in the background
  // (`agent_start`/`agent_end` are emitted, the judge runs, etc.). When
  // it drains into an active run, the active run's lifecycle absorbs it.
  // -------------------------------------------------------------------------
  async injectSystemReminder(
    content: string,
    opts?: SessionInjectSystemReminderOptions,
  ): Promise<SessionInjectSystemReminderResult> {
    this._assertLive('injectSystemReminder()');
    this._assertOpenForTurn('injectSystemReminder()');
    if (typeof content !== 'string' || content.length === 0) {
      throw new HarnessValidationError('injectSystemReminder()', '`content` must be a non-empty string');
    }

    const effectiveModeId = this._record.modeId;
    const mode = this._harness._getMode(effectiveModeId);
    const agent = this._harness.getAgentForMode(effectiveModeId);

    const subscriptionWaiter = this._createActiveTurnWaiter();
    void subscriptionWaiter.promise.catch(() => {});
    const subscription = this._ensureThreadSubscription(agent);
    void subscription.catch(() => {});
    const sub = await Promise.race([subscription, subscriptionWaiter.promise]).finally(() => {
      subscriptionWaiter.cleanup();
    });
    this._assertLive('injectSystemReminder()');
    this._assertOpenForTurn('injectSystemReminder()');
    const activeRunId = sub.activeRunId();
    const willInterleave = activeRunId !== null;

    if (!willInterleave) {
      // Owned-turn path: full turn bookkeeping in a background
      // continuation. Caller doesn't get a result handle.
      const turnAbortController = this._beginTurn(undefined);
      const turnAbortSignal = turnAbortController.signal;
      const activeTurnWaiter = this._createActiveTurnWaiter();
      void activeTurnWaiter.promise.catch(() => {});
      const finishOwnedReminderTurn = () => {
        activeTurnWaiter.cleanup();
        this._endTurn(turnAbortController);
      };
      const assertOwnedReminderTurnNotDeleted = () => {
        if (this._state === 'deleted') {
          throw new HarnessSessionDeletedError(this.id, this._record.resourceId, this._record.threadId);
        }
      };
      let dispatched;
      try {
        const toolsets = this._buildToolsets(mode, undefined);
        const requestContext = await Promise.race([
          this._buildRequestContext({
            modeId: effectiveModeId,
            modelId: this._record.modelId,
            abortSignal: turnAbortSignal,
          }),
          activeTurnWaiter.promise,
        ]);
        const baseExecOptions: AgentExecutionOptionsBase<unknown> = {
          memory: { thread: this.threadId, resource: this.resourceId },
          abortSignal: turnAbortSignal,
          requestContext,
          ...(toolsets ? { toolsets } : {}),
          ...(mode.instructions ? { instructions: mode.instructions } : {}),
        };
        assertOwnedReminderTurnNotDeleted();
        this._assertOpenForTurn('injectSystemReminder()');

        dispatched = agent.sendSignal(
          {
            type: 'system-reminder',
            contents: content,
            ...(opts?.attributes ? { attributes: opts.attributes } : {}),
            ...(opts?.metadata ? { metadata: opts.metadata } : {}),
          },
          {
            resourceId: this.resourceId,
            threadId: this.threadId,
            ifIdle: { behavior: 'wake', streamOptions: baseExecOptions as never },
          },
        );
      } catch (err) {
        finishOwnedReminderTurn();
        throw err;
      }

      // §10.2 agent_start carries the runId — emit it now the run is dispatched.
      // dispatched.signal.id is the originating signalId — stamped for the run projection.
      this._emitAgentStart(dispatched.runId, dispatched.signal.id);

      const completion = this._awaitRunCompletion(dispatched.runId);
      const completionOrDelete = Promise.race([completion, activeTurnWaiter.promise]);
      let agentEndEmitted = false;
      const result = this._trackBackgroundTurnCompletion(
        completionOrDelete
          .then(async full => {
            const tokenUsageDelta =
              full.finishReason === 'suspended'
                ? this._tokenUsageDeltaFromFullOutput(full)
                : this._recordTurnCompletion(full, { persist: false });
            await Promise.race([
              this._maybeCaptureSuspend(full, undefined, effectiveModeId, this._record.modelId, {
                tokenUsageDelta: full.finishReason === 'suspended' ? tokenUsageDelta : undefined,
              }).catch(err => {
                this._latchDurableTurnFlushError(err, full);
                throw err;
              }),
              activeTurnWaiter.promise,
            ]);
            if (tokenUsageDelta !== undefined && full.finishReason !== 'suspended') {
              await Promise.race([
                this._persistTokenUsageOrLatch().catch(err => {
                  if (!this._isExpectedFlushLifecycleError(err)) this._pendingTokenUsageFlushError ??= err;
                  throw err;
                }),
                activeTurnWaiter.promise,
              ]);
            }
            this._emitAgentEnd({ runId: full.runId, finishReason: this._agentEndReasonForFullOutput(full), full });
            agentEndEmitted = true;
            await Promise.race([this._runGoalJudge(full, false), activeTurnWaiter.promise]);
          })
          .catch(err => {
            if (!agentEndEmitted) {
              this._emitTurnEvent({
                type: 'agent_end',
                finishReason: turnAbortSignal.aborted ? 'aborted' : 'error',
                runId: dispatched.runId,
                usage: this._runUsage(),
              });
            }
            throw err;
          })
          .finally(() => {
            finishOwnedReminderTurn();
            void this._maybeDrainQueue();
          }),
      );
      void result.catch(() => {});

      return {
        id: dispatched.signal.id,
        runId: dispatched.runId,
        willInterleave: false,
        accepted: true,
        signal: dispatched.signal,
      };
    }

    // Active-delivery path: drain into the live run.
    this._assertOpenForTurn('injectSystemReminder()');
    const dispatched = agent.sendSignal(
      {
        type: 'system-reminder',
        contents: content,
        ...(opts?.attributes ? { attributes: opts.attributes } : {}),
        ...(opts?.metadata ? { metadata: opts.metadata } : {}),
      },
      {
        resourceId: this.resourceId,
        threadId: this.threadId,
        ifIdle: { behavior: 'wake', streamOptions: {} as never },
      },
    );

    return {
      id: dispatched.signal.id,
      runId: dispatched.runId,
      willInterleave: true,
      accepted: true,
      signal: dispatched.signal,
    };
  }

  /**
   * If the agent run finished suspended, persist a `PendingResume` pointer
   * derived from `FullOutput.suspendPayload` + `runId`. Subsequent calls to
   * `respondTool*` use this pointer to call `agent.resumeStream(...)`.
   *
   * Maps the agent's `tool-call-approval` / `tool-call-suspended` chunks to
   * the four harness-layer kinds:
   *   - tool name `ask_user`     → 'question'
   *   - tool name `submit_plan`  → 'plan-approval'
   *   - payload has `suspendPayload` → 'tool-suspension'
   *   - else                          → 'tool-approval'
   *
   * No-op when the run did not suspend.
   */
  private async _maybeCaptureSuspend(
    full: FullOutput<unknown>,
    queuedItemId = this._currentQueuedItemId,
    modeId = this._record.modeId,
    modelId = this._modelIdForQueuedItem(queuedItemId),
    opts: { tokenUsageDelta?: TokenUsage; originSignalId?: string } = {},
  ): Promise<void> {
    if (full.finishReason !== 'suspended') return;
    this._captureTurnRunId(full);
    const payload = full.suspendPayload as
      | { toolCallId: string; toolName: string; args?: unknown; suspendPayload?: unknown; approvalReasons?: string[] }
      | undefined;
    if (!payload || !full.runId) {
      await this._persistTokenUsageDelta(opts.tokenUsageDelta);
      this._clearPendingDurableTurnFlushErrorIfRepaired(full);
      return;
    }

    const pending = this._pendingResumeFromSuspendedOutput(
      full,
      payload,
      queuedItemId,
      modeId,
      modelId,
      opts.originSignalId,
    );
    if (pending === undefined) return;
    const existing = this._record.pendingResume;
    if (existing && existing.runId === full.runId && existing.toolCallId === payload.toolCallId) {
      await this._flushUpdate(prev => prev, { tokenUsageDelta: opts.tokenUsageDelta });
      this._clearPendingDurableTurnFlushErrorIfRepaired(full);
      return;
    }
    await this._flushUpdate(prev => ({ ...prev, pendingResume: pending }), { tokenUsageDelta: opts.tokenUsageDelta });
    this._clearPendingDurableTurnFlushErrorIfRepaired(full);

    // Emit the §10.2 pending event AFTER the durable-parking barrier (§5.4) so
    // any subscriber observing it can reconstruct the pending state from storage.
    this._emitPendingEvent(pending);
  }

  private _pendingResumeFromSuspendedOutput(
    full: FullOutput<unknown>,
    payload: { toolCallId: string; toolName: string; args?: unknown; suspendPayload?: unknown; approvalReasons?: string[] },
    queuedItemId = this._currentQueuedItemId,
    modeId = this._record.modeId,
    modelId = this._modelIdForQueuedItem(queuedItemId),
    originSignalId?: string,
  ): PendingResume | undefined {
    if (!full.runId) return undefined;
    const kind = this._classifyResumeKind(payload);
    const pending: PendingResume = {
      kind,
      itemId: `${kind}:${payload.toolCallId}`,
      runId: full.runId,
      toolCallId: payload.toolCallId,
      toolName: payload.toolName,
      source: 'parent',
      requestedAt: Date.now(),
      ...(queuedItemId !== undefined ? { queuedItemId } : {}),
      ...(originSignalId !== undefined ? { originSignalId } : {}),
      modeId,
      runtimeDependencies: this._harness._runtimeDependenciesForMode(modeId, modelId),
      payload: this._buildResumePayload(kind, payload),
    };

    if (kind === 'plan-approval') {
      const mode = this._harness._getMode(modeId);
      if (mode.transitionsTo) pending.transitionModeId = mode.transitionsTo;
    }
    return pending;
  }

  private _classifyResumeKind(payload: { toolName: string; suspendPayload?: unknown }): PendingResume['kind'] {
    if (payload.toolName === ASK_USER_TOOL_NAME) return 'question';
    if (payload.toolName === SUBMIT_PLAN_TOOL_NAME) return 'plan-approval';
    if ('suspendPayload' in payload && payload.suspendPayload !== undefined) return 'tool-suspension';
    return 'tool-approval';
  }

  private _buildResumePayload(
    kind: PendingResume['kind'],
    payload: { args?: unknown; suspendPayload?: unknown; approvalReasons?: string[] },
  ): PendingResume['payload'] {
    switch (kind) {
      case 'tool-approval':
        return {
          input: payload.args,
          ...(payload.approvalReasons !== undefined ? { approvalReasons: payload.approvalReasons } : {}),
        };
      case 'tool-suspension':
        return { input: payload.args, suspendData: payload.suspendPayload };
      case 'question': {
        const args = (payload.args ?? {}) as {
          question?: string;
          options?: { label: string; description?: string }[];
          selectionMode?: 'single_select' | 'multi_select';
        };
        return {
          question: args.question,
          options: args.options,
          selectionMode: args.selectionMode,
        };
      }
      case 'plan-approval': {
        const args = (payload.args ?? {}) as { title?: string; plan?: string };
        return { title: args.title, plan: args.plan };
      }
    }
  }

  // -------------------------------------------------------------------------
  // Mode / model getters + setters — §4.2.
  //
  // The session is the local authority for the active mode/model id; the
  // backing agent is selected via Harness lookup. Setters CAS-write through
  // storage so a concurrent harness instance that holds the lease cannot
  // observe a stale value.
  // -------------------------------------------------------------------------

  /** §4.2a: the session's current mode id (the durable default modeId). */
  getCurrentModeId(): string {
    this._assertLive('getCurrentModeId()');
    return this._record.modeId;
  }

  /** Resolved active mode (per the session record). */
  getCurrentMode(): HarnessMode {
    this._assertLive('getCurrentMode()');
    return this._harness._getMode(this._record.modeId);
  }

  /**
   * Switch the active mode for subsequent turns. The backing agent flips
   * with the next `message()`/`queue()` call. Throws if the mode id is
   * unknown.
   */
  async switchMode(opts: { mode: string }): Promise<void> {
    this._assertLive('switchMode()');
    // Validates and throws on unknown id.
    this._harness._getMode(opts.mode);
    const previousModeId = this._record.modeId;
    if (previousModeId === opts.mode) return;
    await this._flushUpdate(prev => ({ ...prev, modeId: opts.mode }));
    this._emitter.emit({ type: 'mode_changed', modeId: opts.mode, previousModeId });
  }

  /**
   * Session model namespace (§4.2a). Surfaced as a namespace for symmetry
   * with `harness.models.*` (§9). Mutators write under the session lease
   * and resolve only after the durable transition commits.
   */
  readonly models = Object.freeze({
    current: (): string => this._modelsCurrent(),
    hasSelected: (): boolean => this._modelsHasSelected(),
    currentAuthStatus: (): Promise<ModelAuthStatus> => this._modelsCurrentAuthStatus(),
    switch: (opts: { model: string }): Promise<void> => this._modelsSwitch(opts),
    setSubagent: (opts: { agentType: string; model: string }): Promise<void> => this._modelsSetSubagent(opts),
    getSubagent: (opts: { agentType: string }): string | null => this._modelsGetSubagent(opts),
  });

  /** Resolved model id for the next turn. Falls back to `''` when nothing has been selected. */
  private _modelsCurrent(): string {
    this._assertLive('models.current()');
    return this._record.modelId;
  }

  /**
   * True once any model has been chosen for this session — either an
   * explicit `models.switch()` call or a `models.setSubagent()` pin. Useful
   * for boot flows that want to gate UI on "has the user picked yet?"
   * without inspecting raw record fields.
   */
  private _modelsHasSelected(): boolean {
    this._assertLive('models.hasSelected()');
    if (this._record.modelId && this._record.modelId.length > 0) return true;
    if (Object.keys(this._record.subagentModelOverrides ?? {}).length > 0) return true;
    return false;
  }

  /**
   * Auth status for the currently resolved model. Routed through
   * `harness.models.getAuthStatus()` when the current model is in the
   * catalog; returns `'unknown'` when no model is selected or the model
   * isn't registered (we don't want the auth-status check to throw on a
   * free-form id the agent layer will accept anyway).
   */
  private async _modelsCurrentAuthStatus(): Promise<ModelAuthStatus> {
    this._assertLive('models.currentAuthStatus()');
    const modelId = this._record.modelId;
    if (!modelId) return 'unknown';
    const entry = await this._harness.models.get(modelId);
    if (!entry) return 'unknown';
    return this._harness.models.getAuthStatus(modelId);
  }

  /** Switch the session's default model id. Free-form string — validated by the agent layer. */
  private async _modelsSwitch(opts: { model: string }): Promise<void> {
    this._assertLive('models.switch()');
    assertModelId('models.switch', opts.model);
    const previousModelId = this._record.modelId;
    if (previousModelId === opts.model) return;
    await this._flushUpdate(prev => ({ ...prev, modelId: opts.model }));
    this._emitter.emit({ type: 'model_changed', modelId: opts.model, previousModelId });
  }

  /**
   * Pin a model for spawned subagents of a given `agentType`. Override is
   * persisted in `SessionRecord.subagentModelOverrides` and read back by
   * the spawn machinery via `models.getSubagent()`. Emits
   * `model_override_set`. No-op when the same mapping is already set.
   */
  private async _modelsSetSubagent(opts: { agentType: string; model: string }): Promise<void> {
    this._assertLive('models.setSubagent()');
    assertAgentType('models.setSubagent', opts.agentType);
    assertModelId('models.setSubagent', opts.model);
    const previousModelId = this._record.subagentModelOverrides?.[opts.agentType] ?? null;
    if (previousModelId === opts.model) return;
    await this._flushUpdate(prev => ({
      ...prev,
      subagentModelOverrides: {
        ...(prev.subagentModelOverrides ?? {}),
        [opts.agentType]: opts.model,
      },
    }));
    // §10.2: subagent model overrides are not a public event — the override is
    // reflected in `SessionRecord.subagentModelOverrides` and the display
    // snapshot (the StateEvent set has only the session-level `model_changed`).
  }

  /** Read the pinned subagent model for an `agentType`, or `null` when unset. */
  private _modelsGetSubagent(opts: { agentType: string }): string | null {
    this._assertLive('models.getSubagent()');
    assertAgentType('models.getSubagent', opts.agentType);
    return this._record.subagentModelOverrides?.[opts.agentType] ?? null;
  }

  // -------------------------------------------------------------------------
  // Custom state (§4.2 / §6.1).
  //
  // The session holds an opaque typed state blob persisted alongside the
  // SessionRecord. The two write forms are equivalent surfaces, but the
  // functional form gives tools an atomic read-modify-write that doesn't
  // stomp concurrent writes from earlier in the same turn.
  // -------------------------------------------------------------------------

  /** Returns the current state. Always resolves with the latest persisted value. */
  async getState<TState = unknown>(): Promise<TState> {
    this._assertLive('getState()');
    return (this._record.state ?? {}) as TState;
  }

  /**
   * Replace or merge the session state. The object form does a shallow merge;
   * the functional form atomically reads the current state, runs the updater,
   * and writes the result. The functional form is the right choice for tools
   * that bump counters or otherwise depend on the previous value.
   */
  setState<TState = unknown>(updates: Partial<TState>, opts?: SetStateOptions): Promise<void>;
  setState<TState = unknown>(updater: (prev: TState) => TState, opts?: SetStateOptions): Promise<void>;
  async setState<TState = unknown>(
    updatesOrUpdater: Partial<TState> | ((prev: TState) => TState),
    opts?: SetStateOptions,
  ): Promise<void> {
    this._assertLive('setState()');
    await this._flushUpdate(prev => {
      const current = (prev.state ?? {}) as TState;
      const next =
        typeof updatesOrUpdater === 'function'
          ? (updatesOrUpdater as (prev: TState) => TState)(current)
          : ({ ...(current as object), ...(updatesOrUpdater as object) } as TState);
      // §5.1: reject non-JSON-serializable state BEFORE the durable commit so a
      // function/bigint/Date/circular value never silently corrupts the row.
      const badPath = firstNonJsonStatePath(next, '$', new Set());
      if (badPath !== undefined) throw new HarnessStateSerializationError(this.id, badPath);
      return { ...prev, state: next };
    }, opts);
  }

  // -------------------------------------------------------------------------
  // getDisplayState — §4.2.
  //
  // Point-in-time snapshot used by TUIs / Studio. Reads off the in-memory
  // `SessionRecord` plus transient per-turn tracking (`_currentRunId`,
  // `_activeTools`, `_toolInputBuffers`, `_activeSubagents`, `_tokenUsage`).
  // Doesn't touch storage. Returned Record/Map projections are fresh on
  // every call — do not mutate them.
  //
  // Persistent thread-level aggregates (task lists, modified-file ledgers,
  // OM progress) live in `session.state`, not here — see the spec doc-comment
  // in §4.2 for the split rationale.
  // -------------------------------------------------------------------------

  /**
   * §4.2e — upload a raw-bytes file attachment scoped to this session. A flat
   * convenience over the kind-based {@link Harness.attachments}`.upload`: the caller
   * supplies a simple `{ name, mimeType, data }` form (always `kind: 'file'`) and the
   * session's own `sessionId`/`resourceId` are supplied automatically. Returns the
   * stable attachment id. `onProgress`, when provided, is invoked once on success with
   * `(byteLength, byteLength)` — the in-process upload completes atomically, so there is
   * no incremental progress to report.
   */
  async uploadAttachment(opts: {
    name: string;
    mimeType: string;
    data: Uint8Array;
    onProgress?: (loaded: number, total: number) => void;
  }): Promise<{ attachmentId: string }> {
    this._assertLive('uploadAttachment()');
    const ref = await this._harness.attachments.upload({
      sessionId: this.id,
      resourceId: this.resourceId,
      kind: 'file',
      filename: opts.name,
      contentType: opts.mimeType,
      data: opts.data,
    });
    opts.onProgress?.(opts.data.byteLength, opts.data.byteLength);
    return { attachmentId: ref.attachmentId };
  }

  /**
   * §4.2e — delete a session-scoped attachment by id. A flat convenience over
   * {@link Harness.attachments}`.delete` that supplies this session's identity.
   */
  async deleteAttachment(opts: { attachmentId: string }): Promise<void> {
    this._assertLive('deleteAttachment()');
    await this._harness.attachments.delete({
      sessionId: this.id,
      resourceId: this.resourceId,
      attachmentId: opts.attachmentId,
    });
  }

  /**
   * §4.2d — set a single application-owned thread setting. Writes ONLY to
   * `HarnessThread.metadata.app[key]` (the one public thread-metadata extension point);
   * it never touches a raw top-level thread-metadata key, and the harness never reads
   * `metadata.app` for any runtime decision. `key` must match the storage-safe grammar
   * `^[A-Za-z_][A-Za-z0-9_]{0,127}$` and must not be a prototype-pollution name
   * (`__proto__` / `prototype` / `constructor`) or a reserved Harness/Mastra namespace
   * (`__mastra*` / `mastra__*`); `value` must be canonical JSON. Invalid input throws
   * {@link HarnessValidationError} before any storage access. Concurrent writes are
   * read-merge-write (last-writer-wins). Like the other thread-metadata operations
   * (e.g. thread rename), this requires the session's thread to already exist in storage —
   * a brand-new thread is persisted on first activity; otherwise `HarnessThreadNotFoundError`.
   */
  async setThreadSetting(opts: { key: string; value: JsonValue }): Promise<void> {
    this._assertLive('setThreadSetting()');
    const { key } = opts;
    if (!/^[A-Za-z_][A-Za-z0-9_]{0,127}$/.test(key)) {
      throw new HarnessValidationError('setThreadSetting().key', 'must match ^[A-Za-z_][A-Za-z0-9_]{0,127}$');
    }
    if (
      key === '__proto__' ||
      key === 'prototype' ||
      key === 'constructor' ||
      key.startsWith('__mastra') ||
      key.startsWith('mastra__')
    ) {
      throw new HarnessValidationError(
        'setThreadSetting().key',
        `key "${key}" is reserved (prototype-pollution or Harness/Mastra namespace)`,
      );
    }
    // Validate + normalize to canonical JSON before touching storage (drops undefined props).
    const normalized = assertJsonValue(opts.value, 'setThreadSetting().value');
    const settings = await this._harness._threadOps.getSettings({
      resourceId: this.resourceId,
      threadId: this.threadId,
    });
    const rawApp = (settings as { app?: unknown }).app;
    const currentApp =
      rawApp !== null && typeof rawApp === 'object' && !Array.isArray(rawApp)
        ? (rawApp as Record<string, JsonValue>)
        : {};
    await this._harness._threadOps.setSettings({
      resourceId: this.resourceId,
      threadId: this.threadId,
      patch: { app: { ...currentApp, [key]: normalized } },
    });
  }

  /**
   * §4.2d — in-process convenience subscription to the wire-safe
   * {@link HarnessDisplayStateSnapshotV1}. Emits the current snapshot immediately, then a new
   * snapshot whenever a session event changes it (recomputed per event and de-duplicated against
   * the last emitted snapshot). With `windowMs > 0` it throttles with trailing coalescing: the
   * first change in a window emits immediately (leading edge), further changes coalesce into one
   * pending snapshot flushed at the window's end. Listener exceptions are isolated. The returned
   * unsubscribe clears any pending timer and stops all further emits. No terminal "closed"
   * snapshot is delivered — `getDisplayState()` is only available while the session is live.
   */
  subscribeDisplayState(
    listener: (state: HarnessDisplayStateSnapshotV1) => void,
    opts?: { windowMs?: number },
  ): () => void {
    this._assertLive('subscribeDisplayState()');
    const windowMs = opts?.windowMs !== undefined && opts.windowMs > 0 ? opts.windowMs : 0;
    let stopped = false;
    let lastEmitted: string | undefined;
    let pending: { snap: HarnessDisplayStateSnapshotV1; serialized: string } | undefined;
    let windowTimer: ReturnType<typeof setTimeout> | undefined;
    let unsubscribeRaw: HarnessEventUnsubscribe | undefined;

    const stop = (): void => {
      if (stopped) return;
      stopped = true;
      if (windowTimer !== undefined) {
        clearTimeout(windowTimer);
        windowTimer = undefined;
      }
      pending = undefined;
      unsubscribeRaw?.();
    };

    const emit = (snap: HarnessDisplayStateSnapshotV1, serialized: string): void => {
      lastEmitted = serialized;
      try {
        listener(snap);
      } catch {
        // Isolate listener exceptions — they must not disrupt the producer or other listeners.
      }
    };

    const compute = (): { snap: HarnessDisplayStateSnapshotV1; serialized: string } | undefined => {
      let snap: HarnessDisplayStateSnapshotV1;
      try {
        snap = toHarnessDisplayStateSnapshotV1(this.getDisplayState());
      } catch {
        // Session is closing/closed/deleted — no further snapshots are available.
        stop();
        return undefined;
      }
      return { snap, serialized: JSON.stringify(snap) };
    };

    const flushWindow = (): void => {
      windowTimer = undefined;
      if (stopped) return;
      if (pending !== undefined && pending.serialized !== lastEmitted) {
        emit(pending.snap, pending.serialized);
      }
      pending = undefined;
    };

    const onEvent = (): void => {
      if (stopped) return;
      const computed = compute();
      if (computed === undefined || computed.serialized === lastEmitted) return; // dedupe
      if (windowMs === 0) {
        emit(computed.snap, computed.serialized);
        return;
      }
      if (windowTimer === undefined) {
        emit(computed.snap, computed.serialized); // leading edge
        windowTimer = setTimeout(flushWindow, windowMs);
      } else {
        pending = computed; // coalesce latest within the open window
      }
    };

    // Immediate initial emit so a fresh subscriber has current state, BEFORE wiring the raw
    // event stream (so we never double-emit the same snapshot).
    const initial = compute();
    if (initial !== undefined) emit(initial.snap, initial.serialized);
    if (stopped) return stop;
    unsubscribeRaw = this.subscribe(onEvent);
    return stop;
  }

  getDisplayState(): SessionDisplayState {
    this._assertLive('getDisplayState()');
    const rec = this._record;
    const snapshot: SessionDisplayState = {
      // Identity
      sessionId: this.id,
      threadId: this.threadId,
      resourceId: this.resourceId,
      lifecycleState: this._state,
      modeId: rec.modeId,
      modelId: rec.modelId,
      createdAt: this.createdAt,
      lastActivityAt: rec.lastActivityAt,

      // Run
      isRunning: this.isRunning(),

      // Activity — fresh projections so callers can't mutate internal maps
      activeTools: Object.fromEntries(this._activeTools.entries()),
      toolInputBuffers: Object.fromEntries(this._toolInputBuffers.entries()),
      activeSubagents: Object.fromEntries(this._activeSubagents.entries()),

      // Tokens — copy so the caller can't mutate the running aggregate
      tokenUsage: { ...this._tokenUsage },

      // Pending interrupt — UX payload only; recovery metadata stays internal.
      pending: pendingResumeForDisplay(rec.pendingResume),

      // Queue
      queueDepth: rec.pendingQueue.length,
    };
    if (this.parentSessionId !== undefined) snapshot.parentSessionId = this.parentSessionId;
    if (this._currentRunId !== undefined) snapshot.currentRunId = this._currentRunId;
    if (this._currentMessageId !== undefined) snapshot.currentMessageId = this._currentMessageId;
    if (this._currentTraceId !== undefined) snapshot.currentTraceId = this._currentTraceId;
    if (this._currentQueuedItemId !== undefined) snapshot.currentQueuedItemId = this._currentQueuedItemId;
    if (rec.goal !== undefined) snapshot.goal = rec.goal;
    const currentRun = this._buildCurrentRunProjection();
    if (currentRun !== undefined) snapshot.currentRun = currentRun;
    return snapshot;
  }

  /**
   * §5.1b SessionRunProjection — a bounded, LIVE read-model view of the in-flight
   * (or suspended) run for the session list/detail UI. Returns `undefined` when no
   * run is active and nothing is pending. F11b fidelity:
   *   - `modeId`/`modelId`/`agentId` reflect the run's EFFECTIVE identity (a
   *     per-turn `opts.mode`/`opts.model` override is captured by `message()`;
   *     a suspended run takes the captured `pendingResume.modeId`).
   *   - `status` is `running` for a live turn, and `waiting`/`resuming` for a
   *     suspended turn (sourced from `pendingResume`; `resumedAt` ⇒ `resuming`).
   *   - `operation.kind` is `queue` for a queued item, else `signal` (carrying
   *     the owned-signal `signalId` when known); the finer kinds
   *     (`sync-generate`/`use-skill`/`inbox-response`) still fold into `signal`.
   *   - `startedAt` is the run-start (`agent_start`) for a live turn, or the
   *     pending interaction's `requestedAt` for a suspended turn.
   * Remaining minor nuance: a live turn reports `running` even at the very
   * `starting` instant (no separate starting phase is tracked).
   */
  private _buildCurrentRunProjection(): SessionRunProjection | undefined {
    const rec = this._record;
    // A pending interaction owns the projection ONLY when no UNRELATED live run is
    // executing: either there is no live run, or the live run IS the resume of
    // this pending (the resume continues the pending's own runId). A genuinely new
    // turn started while suspended (default `message()`/`signal()` are not
    // busy-blocked by a pending) has a different `_currentRunId` and must show as
    // `running`, not be masked by the stale pending.
    const pending = rec.pendingResume;
    if (pending !== undefined && (this._currentRunId === undefined || this._currentRunId === pending.runId)) {
      const modeId = pending.modeId ?? pending.runtimeDependencies?.modeId ?? rec.modeId;
      const operation: SessionRunProjection['operation'] =
        pending.queuedItemId !== undefined
          ? { kind: 'queue', queuedItemId: pending.queuedItemId }
          : pending.originSignalId !== undefined
            ? { kind: 'signal', signalId: pending.originSignalId }
            : { kind: 'signal' };
      return {
        runId: pending.runId,
        status: pending.resumedAt !== undefined ? 'resuming' : 'waiting',
        operation,
        modeId,
        modelId: pending.runtimeDependencies?.modelId ?? rec.modelId,
        agentId: this._modeAgentId(modeId),
        startedAt: pending.requestedAt,
        updatedAt: pending.resumedAt ?? pending.requestedAt,
      };
    }
    // Live in-flight turn (no pending): report the run's EFFECTIVE identity (a
    // per-turn `mode`/`model` override, not just the session default — §5.1b
    // effective identity), captured by `_beginTurn`.
    if (this._currentRunId !== undefined) {
      const modeId = this._currentRunModeId ?? rec.modeId;
      const queuedItemId = this._currentQueuedItemId;
      const projection: SessionRunProjection = {
        runId: this._currentRunId,
        status: 'running',
        operation:
          queuedItemId !== undefined
            ? { kind: 'queue', queuedItemId }
            : {
                kind: 'signal',
                ...(this._currentRunSignalId !== undefined ? { signalId: this._currentRunSignalId } : {}),
              },
        modeId,
        modelId: this._currentRunModelId ?? rec.modelId,
        agentId: this._modeAgentId(modeId),
        startedAt: this._currentRunStartedAt ?? rec.lastActivityAt,
        updatedAt: rec.lastActivityAt,
      };
      if (this._currentTraceId !== undefined) projection.traceId = this._currentTraceId;
      return projection;
    }
    return undefined;
  }

  /** Resolve a mode's backing agentId for the run projection, tolerating a mode
   * that has since been removed from config (fall back to the session default). */
  private _modeAgentId(modeId: string): string {
    try {
      return this._harness._getMode(modeId).agentId;
    } catch {
      return this._harness._getMode(this._record.modeId).agentId;
    }
  }

  // -------------------------------------------------------------------------
  // listMessages — §4.2, §4.4.
  //
  // Read-only history readback for this session's thread, returned
  // oldest-first. Delegates to the memory storage domain on the bound Mastra
  // instance and maps each row into the public `HarnessMessage` partition
  // (spec §11.1) via the shared converter.
  //
  // Returns `[]` when memory storage is not configured (e.g. ad-hoc threads,
  // tests with no storage wired). Throws if the session is no longer live.
  // -------------------------------------------------------------------------

  async listMessages(opts?: ListMessagesOptions): Promise<HarnessMessage[]> {
    this._assertLive('listMessages()');
    const memory = await this._harness._internalTryGetMemoryStorage();
    if (!memory) return [];

    const limit = opts?.limit;
    if (limit !== undefined && (!Number.isFinite(limit) || limit < 0 || !Number.isInteger(limit))) {
      throw new HarnessValidationError('limit', `\`limit\` must be a non-negative integer; received ${String(limit)}`);
    }
    if (limit === 0) return [];

    // When `limit` is set, fetch the most recent N (DESC) and reverse to
    // restore chronological order. Otherwise fetch the full thread history
    // in natural order — mirrors the legacy harness's two-path behaviour.
    if (limit !== undefined) {
      const result = await memory.listMessages({
        threadId: this.threadId,
        resourceId: this.resourceId,
        perPage: limit,
        page: 0,
        orderBy: { field: 'createdAt', direction: 'DESC' },
      });
      return result.messages
        .slice()
        .reverse()
        .map(msg => convertStoredMessageToHarnessMessage(msg as unknown as StoredMessageRow));
    }

    const result = await memory.listMessages({ threadId: this.threadId, resourceId: this.resourceId, perPage: false });
    return result.messages.map(msg => convertStoredMessageToHarnessMessage(msg as unknown as StoredMessageRow));
  }

  // -------------------------------------------------------------------------
  // Goals — §4.7.
  //
  // A goal is a standing objective attached to the session that survives
  // across turns. While the goal is `active`, the harness invokes a separate
  // judge model after every assistant turn (`_recordTurnCompletion` hook)
  // and dispatches its verdict (`done` / `continue` / `waiting`). On
  // `continue`, the harness self-enqueues a continuation turn via the
  // session's own `pendingQueue` so user follow-ups preempt it cleanly.
  //
  // Goals are session-scoped (not thread-scoped) and are forbidden on
  // subagent sessions — subagents are bounded units of work that already
  // terminate at task completion.
  // -------------------------------------------------------------------------

  /**
   * Attach a goal to this session. Replaces any existing goal (emits
   * `goal_cleared` for the prior goal first, then `goal_set`). Resets the
   * turn counter and persists to `SessionRecord.goal`.
   *
   * When `kickoff` is `true` (default), an initial continuation turn is
   * enqueued so the agent starts working without an explicit `message()`.
   */
  async setGoal(opts: GoalOptions): Promise<GoalState> {
    this._assertLive('setGoal()');
    if (this.parentSessionId !== undefined || this._record.origin === 'subagent-tool') {
      throw new HarnessValidationError('setGoal', 'goals cannot be set on subagent sessions (parent owns the loop)');
    }
    if (typeof opts.objective !== 'string' || opts.objective.length === 0) {
      throw new HarnessValidationError('setGoal.objective', 'must be a non-empty string');
    }
    if (opts.maxTurns !== undefined && (!Number.isInteger(opts.maxTurns) || opts.maxTurns < 1)) {
      throw new HarnessValidationError('setGoal.maxTurns', 'must be a positive integer');
    }

    const defaults = this._harness._internalGoalDefaults;
    const judgeModelId = opts.judgeModel ?? defaults.defaultJudgeModel;
    if (typeof judgeModelId !== 'string' || judgeModelId.length === 0) {
      throw new HarnessValidationError(
        'setGoal.judgeModel',
        'no judge model provided and `goals.defaultJudgeModel` is not configured',
      );
    }

    const priorId = this._record.goal?.id;
    const goal: GoalState = {
      id: `goal-${randomUUID()}`,
      objective: opts.objective,
      status: 'active',
      turnsUsed: 0,
      maxTurns: opts.maxTurns ?? defaults.defaultMaxTurns,
      judgeModelId,
      createdAt: Date.now(),
    };

    await this._flushUpdate(prev => ({ ...prev, goal }));
    if (priorId !== undefined) {
      this._emit({ type: 'goal_cleared', goalId: priorId });
    }
    this._emit({ type: 'goal_set', goal });

    if (opts.kickoff !== false) {
      await this._enqueueGoalContinuation(goal, buildKickoffContinuation(opts.objective));
    }

    return goal;
  }

  /** Return the active goal, if any. */
  getGoal(): GoalState | undefined {
    this._assertLive('getGoal()');
    return this._record.goal;
  }

  /** Pause auto-continuations without losing the goal. Emits `goal_paused`. */
  async pauseGoal(): Promise<GoalState | undefined> {
    this._assertLive('pauseGoal()');
    const goal = this._record.goal;
    if (!goal || goal.status === 'paused') return goal;
    const updated: GoalState = { ...goal, status: 'paused' };
    await this._flushUpdate(prev => ({ ...prev, goal: updated }));
    this._emit({ type: 'goal_paused', goalId: goal.id, reason: 'requested' });
    return updated;
  }

  /**
   * Resume an inactive goal. Re-emits `goal_resumed` and enqueues a fresh
   * continuation turn so the agent picks up where it left off.
   */
  async resumeGoal(): Promise<GoalState | undefined> {
    this._assertLive('resumeGoal()');
    const goal = this._record.goal;
    if (!goal) return undefined;
    if (goal.status === 'active') return goal;
    const updated: GoalState = { ...goal, status: 'active' };
    await this._flushUpdate(prev => ({ ...prev, goal: updated }));
    this._emit({ type: 'goal_resumed', goalId: goal.id });
    await this._enqueueGoalContinuation(updated, buildResumeContinuation(updated.objective));
    return updated;
  }

  /** Drop the goal entirely. Emits `goal_cleared`. */
  async clearGoal(): Promise<void> {
    this._assertLive('clearGoal()');
    const goal = this._record.goal;
    if (!goal) return;
    await this._flushUpdate(prev => {
      const next = { ...prev };
      delete next.goal;
      return next;
    });
    this._emit({ type: 'goal_cleared', goalId: goal.id });
  }

  /**
   * Re-point judge model and/or budget on the in-flight goal. Parity with
   * TUI's `GoalManager.updateJudgeDefaults`. Both fields are optional; pass
   * only what you want to change. Does not reset `turnsUsed`, does not emit
   * `goal_paused`/`goal_resumed`. No-op when no goal is set.
   *
   * Returns the updated goal, or `undefined` if there's nothing to update.
   */
  async updateJudgeDefaults(opts: { judgeModelId?: string; maxTurns?: number }): Promise<GoalState | undefined> {
    this._assertLive('updateJudgeDefaults()');
    const goal = this._record.goal;
    if (!goal) return undefined;
    if (opts.judgeModelId === undefined && opts.maxTurns === undefined) return goal;
    if (opts.maxTurns !== undefined && (!Number.isFinite(opts.maxTurns) || opts.maxTurns <= 0)) {
      throw new HarnessValidationError('maxTurns', 'maxTurns must be a positive number');
    }
    const updated: GoalState = {
      ...goal,
      ...(opts.judgeModelId !== undefined ? { judgeModelId: opts.judgeModelId } : {}),
      ...(opts.maxTurns !== undefined ? { maxTurns: opts.maxTurns } : {}),
    };
    await this._flushUpdate(prev => ({ ...prev, goal: updated }));
    return updated;
  }

  /**
   * @internal — enqueue a goal-driven continuation turn. Caller is responsible
   * for building the final prompt content (kickoff / resume / judge-continue
   * each use a distinct template — see `buildKickoffContinuation` /
   * `buildResumeContinuation` / `buildJudgeContinuation`). Marked with
   * `source: 'goal'` so the judge loop knows to skip re-judging on the
   * resulting turn (otherwise the loop would never terminate).
   */
  private async _enqueueGoalContinuation(goal: GoalState, content: string): Promise<void> {
    if (this._currentCancelRequest() !== undefined) return;
    const cap = this._harness._internalMaxQueueDepth;
    const item: QueuedItem = {
      id: `q-${randomUUID()}`,
      enqueuedAt: Date.now(),
      content,
      attachments: [],
      mode: this._record.modeId,
      source: 'goal',
      goalId: goal.id,
    };
    const droppedItems: QueueBackpressureDrop[] = [];
    let admitted = false;
    await this._flushUpdate(prev => {
      if (prev.cancelRequest !== undefined) return prev;
      return this._applyQueueBackpressureForAppend(prev, {
        item,
        receipt: undefined,
        maxQueueDepth: cap,
        source: 'goal',
        goalId: goal.id,
        droppedItems,
        onRejected: () => {
          admitted = false;
        },
        onAdmitted: () => {
          admitted = true;
        },
      });
    });
    this._failBackpressureDroppedItems(droppedItems);
    if (!admitted) {
      // §10.2: queue-full/backpressure is not a public event — the goal item is
      // simply not admitted (rejected at the caller for direct queue()).
      return;
    }
    void this._maybeDrainQueue();
  }

  /**
   * @internal — invoked from `_recordTurnCompletion` after every assistant
   * turn settles. Implements the judge loop (§4.7).
   *
   * Triple stale-goal gate: we capture the goal id before fetching context,
   * before calling the judge, and before enqueueing the continuation. If
   * any check fails the verdict is discarded silently (no event, no state
   * change) — the user has already moved on.
   */
  private async _runGoalJudge(turn: FullOutput<unknown>, wasGoalDriven: boolean): Promise<void> {
    // Skip re-judging on goal-driven continuation turns to avoid a tight
    // loop where every continuation triggers another judge call. The
    // judge only runs after user-driven turns; continuations are auto-
    // generated from the prior judge call.
    if (wasGoalDriven) return;

    const goal = this._record.goal;
    if (!goal || goal.status !== 'active') return;

    const evaluatedGoalId = goal.id;

    // Suspended turns don't count toward the judge loop — wait for resume.
    if (turn.finishReason === 'suspended') return;

    // Gate 1 — re-read goal after the async context fetch. If it's been
    // cleared / paused / replaced, drop this judge cycle silently.
    const context = await this._getJudgeContext(turn);
    if (this._record.goal?.id !== evaluatedGoalId || this._record.goal.status !== 'active') return;

    // No-assistant-message fallback: parity with TUI's evaluateAfterTurn.
    // The judge has nothing to score, but the agent still made some attempt
    // (typically a tool call without a closing assistant message). Push a
    // gentle nudge unless we've hit the budget.
    if (!context.lastAssistantContent) {
      if (goal.turnsUsed >= goal.maxTurns) {
        await this._flushUpdate(prev =>
          prev.goal && prev.goal.id === evaluatedGoalId
            ? { ...prev, goal: { ...prev.goal, status: 'paused' as const } }
            : prev,
        );
        this._emit({ type: 'goal_paused', goalId: evaluatedGoalId, reason: 'budget_exhausted' });
        return;
      }
      await this._enqueueGoalContinuation(
        goal,
        buildJudgeContinuation({
          turn: goal.turnsUsed,
          max: goal.maxTurns,
          objective: goal.objective,
          judgeReason: 'No response yet, keep working.',
        }),
      );
      return;
    }

    let decision: GoalJudgeDecision;
    try {
      decision = await this._callJudge(goal, turn);
    } catch {
      // Gate 2a — goal might have changed during the judge call.
      if (this._record.goal?.id !== evaluatedGoalId) return;
      await this._flushUpdate(prev =>
        prev.goal && prev.goal.id === evaluatedGoalId
          ? { ...prev, goal: { ...prev.goal, status: 'paused' as const } }
          : prev,
      );
      this._emit({ type: 'goal_paused', goalId: evaluatedGoalId, reason: 'judge_failed' });
      return;
    }

    // Gate 2b — goal might have changed during the judge call.
    if (this._record.goal?.id !== evaluatedGoalId) return;

    const turnsUsed = decision.decision === 'waiting' ? goal.turnsUsed : goal.turnsUsed + 1;
    const updated: GoalState = { ...goal, turnsUsed, lastDecision: decision };

    await this._flushUpdate(prev =>
      prev.goal && prev.goal.id === evaluatedGoalId ? { ...prev, goal: updated } : prev,
    );

    this._emit({
      type: 'goal_judged',
      goalId: evaluatedGoalId,
      decision,
      turnsUsed,
      maxTurns: updated.maxTurns,
    });

    if (decision.decision === 'done') {
      await this._flushUpdate(prev =>
        prev.goal && prev.goal.id === evaluatedGoalId
          ? { ...prev, goal: { ...prev.goal, status: 'done' as const } }
          : prev,
      );
      this._emit({ type: 'goal_done', goalId: evaluatedGoalId, reason: decision.reason, turnsUsed });
      return;
    }

    if (decision.decision === 'waiting') {
      // §10.2: the judge parked the goal at an external checkpoint. Surface it
      // as `goal_waiting` (turnsUsed is unchanged — see the computation above)
      // so subscribers can distinguish a checkpoint pause from a budget/judge
      // failure (`goal_paused`) or completion (`goal_done`).
      this._emit({ type: 'goal_waiting', goalId: evaluatedGoalId, reason: decision.reason, turnsUsed });
      return;
    }

    // decision.decision === 'continue'
    if (turnsUsed >= updated.maxTurns) {
      await this._flushUpdate(prev =>
        prev.goal && prev.goal.id === evaluatedGoalId
          ? { ...prev, goal: { ...prev.goal, status: 'paused' as const } }
          : prev,
      );
      this._emit({ type: 'goal_paused', goalId: evaluatedGoalId, reason: 'budget_exhausted' });
      return;
    }

    // Gate 3 — final stale check before enqueueing.
    if (this._record.goal?.id !== evaluatedGoalId || this._record.goal.status !== 'active') return;
    await this._enqueueGoalContinuation(
      updated,
      buildJudgeContinuation({
        turn: turnsUsed,
        max: updated.maxTurns,
        objective: updated.objective,
        judgeReason: decision.reason,
      }),
    );
  }

  /**
   * @internal — execute the judge model. Returns a `GoalJudgeDecision`.
   *
   * Test-injection hook: when `__testJudge` is set on this session, it
   * runs in place of the real judge call so unit tests can drive verdicts
   * deterministically without standing up a live model.
   */
  private async _callJudge(goal: GoalState, turn: FullOutput<unknown>): Promise<GoalJudgeDecision> {
    const hook = this.__testJudge;
    if (hook) {
      const verdict = await hook(goal);
      return { ...verdict, judgedAt: Date.now() };
    }
    // Real judge path. Mirrors the TUI's GoalManager.callJudge:
    //   - dedicated `goal-judge` Agent with JUDGE_SYSTEM_PROMPT baked in
    //   - input processor: ProviderHistoryCompat (history-shape parity
    //     across providers, esp. Anthropic)
    //   - error processors: StreamErrorRetryProcessor, PrefillErrorHandler,
    //     ProviderHistoryCompat (retry flaky judge streams cleanly)
    //   - dedicated memory thread `${sessionId}-${goalId}` so the judge
    //     sees continuity across iterations (its own prior verdicts)
    //   - structured output via the judge schema
    //   - context: goal + last user content + assistantStepsSinceLastUser
    //     + truncated last assistant content (4000-char cap)
    const context = await this._getJudgeContext(turn);
    const judgeAgent = this._createJudgeAgent(goal);
    const memory = await judgeAgent.getMemory({ requestContext: new RequestContext() });
    const judgeThreadId = `${this._record.id}-${goal.id}`;

    if (memory) {
      const existing = await memory.getThreadById({ threadId: judgeThreadId });
      if (!existing) {
        await memory.createThread({
          threadId: judgeThreadId,
          resourceId: this._record.resourceId,
          title: `Goal judge: ${goal.objective.slice(0, 80)}`,
          metadata: {
            goalJudge: true,
            parentSessionId: this._record.id,
            goalId: goal.id,
          },
        });
      }
    }

    const truncatedAssistant = truncateForJudge(context.lastAssistantContent ?? 'No response yet, keep working.');
    const recentUser = context.lastUserContent
      ? `\n\nLatest user message:\n${truncateForJudge(context.lastUserContent)}\n\nAssistant steps since that user message: ${context.assistantStepsSinceLastUser}`
      : '';
    const prompt = `Goal: ${goal.objective}${recentUser}\n\nLatest assistant message:\n${truncatedAssistant}`;

    const stream = await judgeAgent.stream(prompt, {
      ...(memory ? { memory: { thread: judgeThreadId, resource: this._record.resourceId } } : {}),
      structuredOutput: { schema: GoalJudgeSchema },
    } as never);

    await (stream as { consumeStream: () => Promise<void> }).consumeStream();
    const full = (await (stream as { getFullOutput: () => Promise<unknown> }).getFullOutput()) as {
      object?: unknown;
    };
    const obj = full.object as { decision: 'done' | 'continue' | 'waiting'; reason: string } | undefined;
    if (!obj || typeof obj !== 'object') {
      throw new Error('judge returned no structured output');
    }
    return { decision: obj.decision, reason: obj.reason, judgedAt: Date.now() };
  }

  /** @internal — test-only hook used by `session.goal.test.ts`. */
  __testJudge?: (goal: GoalState) => Promise<Omit<GoalJudgeDecision, 'judgedAt'>>;

  /**
   * Build the conversation context the judge sees: the latest user content,
   * how many assistant steps have happened since that user message, and the
   * latest assistant content. Mirrors `GoalManager.getRecentConversationContext`.
   */
  private async _getJudgeContext(turn?: FullOutput<unknown>): Promise<{
    lastUserContent: string | null;
    assistantStepsSinceLastUser: number;
    lastAssistantContent: string | null;
  }> {
    let messages: HarnessMessage[] = [];
    try {
      messages = await this.listMessages();
    } catch {
      // listMessages can fail in ad-hoc-thread setups; we'll fall back to
      // the in-memory turn text below.
    }

    let lastUserIndex = -1;
    let lastAssistantContent: string | null = null;

    for (let i = messages.length - 1; i >= 0; i--) {
      const msg = messages[i] as { role?: string; content?: unknown } | undefined;
      if (!msg) continue;
      if (!lastAssistantContent && msg.role === 'assistant') {
        lastAssistantContent = this._extractTextContent(msg.content);
      }
      if (msg.role === 'user') {
        lastUserIndex = i;
        break;
      }
    }

    // Storage may not have the assistant turn yet (depends on the agent
    // wiring). Fall back to the in-memory `turn.text` we just produced.
    if (!lastAssistantContent && turn) {
      const text = (turn as { text?: string }).text;
      if (typeof text === 'string' && text.length > 0) {
        lastAssistantContent = text;
      }
    }

    const lastUserContent =
      lastUserIndex >= 0 ? this._extractTextContent((messages[lastUserIndex] as { content?: unknown }).content) : null;
    const assistantStepsSinceLastUser =
      lastUserIndex >= 0
        ? messages.slice(lastUserIndex + 1).filter(m => (m as { role?: string }).role === 'assistant').length
        : 0;

    return {
      lastUserContent,
      assistantStepsSinceLastUser,
      lastAssistantContent,
    };
  }

  private _extractTextContent(content: unknown): string {
    if (typeof content === 'string') return content;
    if (Array.isArray(content)) {
      return content
        .filter(part => (part as { type?: string })?.type === 'text')
        .map(part => (part as { text?: string }).text ?? '')
        .join('\n');
    }
    return String(content ?? '');
  }

  /**
   * Construct the dedicated judge Agent. The processor chain matches the
   * TUI's GoalManager so the harness-native judge has the same robustness
   * against provider history quirks and transient stream errors.
   *
   * The judge agent is bound to the same Mastra instance as the parent
   * session so it inherits memory/storage wiring.
   */
  private _createJudgeAgent(goal: GoalState): Agent {
    const model = new ModelRouterLanguageModel(goal.judgeModelId as never);
    return new Agent({
      id: 'goal-judge',
      name: 'Goal Judge',
      instructions: JUDGE_SYSTEM_PROMPT,
      model,
      mastra: this._harness.mastra,
      inputProcessors: [new ProviderHistoryCompat()],
      errorProcessors: [new StreamErrorRetryProcessor(), new PrefillErrorHandler(), new ProviderHistoryCompat()],
    });
  }

  // -------------------------------------------------------------------------
  // Suspend / resume — §4.2.
  //
  // When `message()` (or a queued turn) finishes with `finishReason
  // === 'suspended'`, the harness persists a `PendingResume` record holding
  // the agent's `runId` + `toolCallId` + UX-facing payload. Callers respond
  // through one of four typed entry points; all funnel into `_resume(...)`,
  // which is the single place that calls `agent.resumeStream(...)`.
  //
  // `pendingResume.resumedAt` is set under the lease before the agent call so
  // a crash between "marked resumed" and "cleared pending" replays as a no-op
  // on rehydration (idempotent at-least-once).
  // -------------------------------------------------------------------------

  /** Resume a pending tool-approval. `approved: false` rejects the call. */
  async respondToToolApproval(
    opts: { approved: boolean; reason?: string } & InboxReceiptResponseOptions,
  ): Promise<InboxResponseResult>;
  async respondToToolApproval(
    opts: { approved: boolean; reason?: string } & LegacyInboxResponseOptions,
  ): Promise<AgentResult>;
  async respondToToolApproval(
    opts: { approved: boolean; reason?: string } & InboxResponseOptions,
  ): Promise<AgentResult | InboxResponseResult>;
  async respondToToolApproval(
    opts: { approved: boolean; reason?: string } & InboxResponseOptions,
  ): Promise<AgentResult | InboxResponseResult> {
    return this._resume(
      'tool-approval',
      compactJsonObject({
        approved: opts.approved,
        reason: opts.reason,
      }),
      opts,
    );
  }

  /** Resume a pending tool-suspension. `resumeData` is forwarded to the tool. */
  async respondToToolSuspension(
    opts: { resumeData: unknown } & InboxReceiptResponseOptions,
  ): Promise<InboxResponseResult>;
  async respondToToolSuspension(opts: { resumeData: unknown } & LegacyInboxResponseOptions): Promise<AgentResult>;
  async respondToToolSuspension(
    opts: { resumeData: unknown } & InboxResponseOptions,
  ): Promise<AgentResult | InboxResponseResult>;
  async respondToToolSuspension(
    opts: { resumeData: unknown } & InboxResponseOptions,
  ): Promise<AgentResult | InboxResponseResult> {
    return this._resume('tool-suspension', opts.resumeData, opts);
  }

  /** Resume a pending `ask_user` question. */
  async respondToQuestion(opts: { answer: unknown } & InboxReceiptResponseOptions): Promise<InboxResponseResult>;
  async respondToQuestion(opts: { answer: unknown } & LegacyInboxResponseOptions): Promise<AgentResult>;
  async respondToQuestion(opts: { answer: unknown } & InboxResponseOptions): Promise<AgentResult | InboxResponseResult>;
  async respondToQuestion(
    opts: { answer: unknown } & InboxResponseOptions,
  ): Promise<AgentResult | InboxResponseResult> {
    return this._resume('question', { answer: opts.answer }, opts);
  }

  async respondToSandboxAccess(
    opts: { approved: boolean; reason?: string } & InboxReceiptResponseOptions,
  ): Promise<InboxResponseResult>;
  async respondToSandboxAccess(
    opts: { approved: boolean; reason?: string } & LegacyInboxResponseOptions,
  ): Promise<AgentResult>;
  async respondToSandboxAccess(
    opts: { approved: boolean; reason?: string } & InboxResponseOptions,
  ): Promise<AgentResult | InboxResponseResult>;
  async respondToSandboxAccess(
    opts: { approved: boolean; reason?: string } & InboxResponseOptions,
  ): Promise<AgentResult | InboxResponseResult> {
    return this._resume(
      'sandbox-access',
      compactJsonObject({
        approved: opts.approved,
        reason: opts.reason,
      }),
      opts,
    );
  }

  /**
   * Resume a pending `submit_plan` approval.
   *
   * On `approved: true` the harness flips the active mode to:
   *   - `opts.transitionToMode` when supplied (overrides mode-declared default), OR
   *   - the submitting mode's declared `transitionsTo` when set, OR
   *   - no-op (stays in the submitting mode).
   *
   * `revision` is free-form reviewer feedback forwarded to the tool as
   * `resumeData.revision` (see `submitPlan` resume schema). It is independent
   * of approval — the reviewer can approve with a revision note or reject
   * with revision guidance.
   */
  async respondToPlanApproval(
    opts: {
      approved: boolean;
      revision?: string;
      transitionToMode?: string;
    } & InboxReceiptResponseOptions,
  ): Promise<InboxResponseResult>;
  async respondToPlanApproval(
    opts: {
      approved: boolean;
      revision?: string;
      transitionToMode?: string;
    } & LegacyInboxResponseOptions,
  ): Promise<AgentResult>;
  async respondToPlanApproval(
    opts: {
      approved: boolean;
      revision?: string;
      transitionToMode?: string;
    } & InboxResponseOptions,
  ): Promise<AgentResult | InboxResponseResult>;
  async respondToPlanApproval(
    opts: {
      approved: boolean;
      revision?: string;
      transitionToMode?: string;
    } & InboxResponseOptions,
  ): Promise<AgentResult | InboxResponseResult> {
    if (opts.transitionToMode !== undefined) {
      // Validate eagerly so callers see a clean error rather than a CAS-time
      // throw from inside the resume flow.
      this._harness._getMode(opts.transitionToMode);
    }
    return this._resume(
      'plan-approval',
      compactJsonObject({
        approved: opts.approved,
        revision: opts.revision,
        transitionToMode: opts.transitionToMode,
      }),
      opts,
    );
  }

  // -------------------------------------------------------------------------
  // Permissions (§4.2e).
  //
  // Session-scoped grants (`SessionRecord.sessionGrants`) and policy rules
  // (`SessionRecord.permissionRules`) compose with the tool's static
  // approval flag, the harness `defaultPermissionPolicy`, and any
  // resolver-supplied category to decide allow/ask/deny on each tool call.
  //
  // Both surfaces are persisted under the session's write lease so a crash
  // mid-grant either lands entirely or not at all.
  // -------------------------------------------------------------------------

  /**
   * Session permissions namespace (§4.2e). All mutators write
   * `SessionRecord.permissionRules` / `SessionRecord.sessionGrants` under
   * the session lease and resolve only after the durable transition
   * commits. Validation, closed-session, ownership, or storage failures
   * reject before any event or display projection is emitted.
   */
  readonly permissions = Object.freeze({
    grantCategory: (opts: { category: ToolCategory }): Promise<void> => this._permGrantCategory(opts),
    grantTool: (opts: { toolName: string }): Promise<void> => this._permGrantTool(opts),
    revokeCategory: (opts: { category: ToolCategory }): Promise<void> => this._permRevokeCategory(opts),
    revokeTool: (opts: { toolName: string }): Promise<void> => this._permRevokeTool(opts),
    getGrants: (): Readonly<SessionGrants> => this._permGetGrants(),
    getRules: (): Readonly<PermissionRules> => this._permGetRules(),
    setPolicy: (
      opts:
        | { category: ToolCategory; toolName?: never; policy: PermissionPolicy }
        | { toolName: string; category?: never; policy: PermissionPolicy },
    ): Promise<void> => this._permSetPolicy(opts),
  });

  /**
   * Grant every tool in a category for the lifetime of this session
   * ("don't ask again for `read` tools"). No-op if already granted.
   * Emits `permission_granted` on a transition.
   */
  private async _permGrantCategory(opts: { category: ToolCategory }): Promise<void> {
    this._assertLive('permissions.grantCategory()');
    assertToolCategory('permissions.grantCategory', opts.category);
    if (this._record.sessionGrants.categories.includes(opts.category)) return;
    await this._flushUpdate(prev => ({
      ...prev,
      sessionGrants: {
        ...prev.sessionGrants,
        categories: [...prev.sessionGrants.categories, opts.category],
      },
    }));
    this._emitter.emit({ type: 'permission_granted', category: opts.category });
  }

  /**
   * Grant a specific tool for the lifetime of this session. No-op if
   * already granted. Emits `permission_granted` on a transition.
   */
  private async _permGrantTool(opts: { toolName: string }): Promise<void> {
    this._assertLive('permissions.grantTool()');
    assertToolName('permissions.grantTool', opts.toolName);
    if (this._record.sessionGrants.tools.includes(opts.toolName)) return;
    await this._flushUpdate(prev => ({
      ...prev,
      sessionGrants: {
        ...prev.sessionGrants,
        tools: [...prev.sessionGrants.tools, opts.toolName],
      },
    }));
    this._emitter.emit({ type: 'permission_granted', toolName: opts.toolName });
  }

  /**
   * Revoke a previously granted category. No-op if not granted. Emits
   * `permission_revoked` on a transition.
   */
  private async _permRevokeCategory(opts: { category: ToolCategory }): Promise<void> {
    this._assertLive('permissions.revokeCategory()');
    assertToolCategory('permissions.revokeCategory', opts.category);
    const idx = this._record.sessionGrants.categories.indexOf(opts.category);
    if (idx === -1) return;
    await this._flushUpdate(prev => ({
      ...prev,
      sessionGrants: {
        ...prev.sessionGrants,
        categories: prev.sessionGrants.categories.filter(c => c !== opts.category),
      },
    }));
    this._emitter.emit({ type: 'permission_revoked', category: opts.category });
  }

  /**
   * Revoke a previously granted tool. No-op if not granted. Emits
   * `permission_revoked` on a transition.
   */
  private async _permRevokeTool(opts: { toolName: string }): Promise<void> {
    this._assertLive('permissions.revokeTool()');
    assertToolName('permissions.revokeTool', opts.toolName);
    const idx = this._record.sessionGrants.tools.indexOf(opts.toolName);
    if (idx === -1) return;
    await this._flushUpdate(prev => ({
      ...prev,
      sessionGrants: {
        ...prev.sessionGrants,
        tools: prev.sessionGrants.tools.filter(t => t !== opts.toolName),
      },
    }));
    this._emitter.emit({ type: 'permission_revoked', toolName: opts.toolName });
  }

  /** Read-only snapshot of the session's current grants. */
  private _permGetGrants(): Readonly<SessionGrants> {
    this._assertLive('permissions.getGrants()');
    const { categories, tools } = this._record.sessionGrants;
    return Object.freeze({ categories: [...categories], tools: [...tools] });
  }

  /** Read-only snapshot of the session's current per-category / per-tool rules. */
  private _permGetRules(): Readonly<PermissionRules> {
    this._assertLive('permissions.getRules()');
    const { categories, tools } = this._record.permissionRules;
    return Object.freeze({ categories: { ...categories }, tools: { ...tools } });
  }

  /**
   * Set a permission rule. Exactly one of `category` / `toolName` must be
   * set — the wire shape and the storage shape both keep these
   * dimensions separate so subscribers can route without inspecting the
   * payload. Emits `permission_policy_changed` on a transition.
   */
  private async _permSetPolicy(
    opts:
      | { category: ToolCategory; toolName?: never; policy: PermissionPolicy }
      | { toolName: string; category?: never; policy: PermissionPolicy },
  ): Promise<void> {
    this._assertLive('permissions.setPolicy()');
    if ((opts.category === undefined) === (opts.toolName === undefined)) {
      throw new HarnessValidationError('permissions.setPolicy', 'must set exactly one of "category" or "toolName"');
    }
    assertPolicy('permissions.setPolicy', opts.policy);
    if (opts.category !== undefined) {
      assertToolCategory('permissions.setPolicy', opts.category);
      const oldPolicy = this._record.permissionRules.categories[opts.category];
      if (oldPolicy === opts.policy) return;
      await this._flushUpdate(prev => ({
        ...prev,
        permissionRules: {
          ...prev.permissionRules,
          categories: { ...prev.permissionRules.categories, [opts.category!]: opts.policy },
        },
      }));
      this._emitter.emit({
        type: 'permission_policy_changed',
        category: opts.category,
        oldPolicy,
        newPolicy: opts.policy,
      });
      return;
    }
    assertToolName('permissions.setPolicy', opts.toolName!);
    const oldPolicy = this._record.permissionRules.tools[opts.toolName!];
    if (oldPolicy === opts.policy) return;
    await this._flushUpdate(prev => ({
      ...prev,
      permissionRules: {
        ...prev.permissionRules,
        tools: { ...prev.permissionRules.tools, [opts.toolName!]: opts.policy },
      },
    }));
    this._emitter.emit({
      type: 'permission_policy_changed',
      toolName: opts.toolName,
      oldPolicy,
      newPolicy: opts.policy,
    });
  }

  private async _resume(
    expectedKind: PendingResume['kind'],
    resumeData: unknown,
    responseOptions: InboxResponseOptions = {},
  ): Promise<AgentResult | InboxResponseResult> {
    this._assertLive(`respond[${expectedKind}]`);
    const responseId = getOwnRecordValue(responseOptions as Record<string, unknown>, 'responseId');
    if (responseId !== undefined && typeof responseId !== 'string') {
      throw new HarnessValidationError(`respond[${expectedKind}].responseId`, 'responseId must be a string');
    }
    const responseMode: ResumeResponseMode = responseId !== undefined ? 'inbox-receipt' : 'agent-result';
    if (responseId !== undefined && responseId.length === 0) {
      throw new HarnessValidationError(`respond[${expectedKind}].responseId`, 'responseId must be a non-empty string');
    }
    const requestedItemId = getOwnRecordValue(responseOptions as Record<string, unknown>, 'itemId');
    if (requestedItemId !== undefined && typeof requestedItemId !== 'string') {
      throw new HarnessValidationError(`respond[${expectedKind}].itemId`, 'itemId must be a string');
    }

    const storedReceipt =
      responseId !== undefined ? getOwnRecordValue(this._record.inboxResponseReceipts, responseId) : undefined;
    if (storedReceipt !== undefined) {
      const duplicate = this._resolveStoredInboxResponse(expectedKind, resumeData, responseOptions);
      if (storedReceipt.status === 'applied') {
        return duplicate!;
      }
      if (this._record.pendingResume === undefined) {
        const recoveredReceipt = await this._applyInboxReceiptFromCompletedQueue(storedReceipt);
        if (recoveredReceipt) return this._inboxReceiptResult(recoveredReceipt, true);
        return duplicate!;
      }
    }

    const cancelRequest = this._currentCancelRequest();
    if (cancelRequest !== undefined) {
      throw new HarnessSessionCancelledError(this.id, cancelRequest.reason);
    }

    const pending = this._record.pendingResume;
    if (!pending) {
      throw new HarnessValidationError(`respond[${expectedKind}]`, 'no pending resume on this session');
    }
    if (pending.kind !== expectedKind) {
      throw new HarnessValidationError(
        `respond[${expectedKind}]`,
        `pending resume is "${pending.kind}", not "${expectedKind}"`,
      );
    }

    const itemId = pending.itemId ?? pending.toolCallId;
    if (requestedItemId !== undefined && requestedItemId !== itemId) {
      throw new HarnessInboxItemNotFoundError(this.id, requestedItemId, expectedKind === 'sandbox-access' ? undefined : expectedKind);
    }
    const responseHash =
      responseId !== undefined
        ? this._computeInboxResponseHash({
            kind: expectedKind,
            itemId,
            runId: pending.runId,
            pendingRequestedAt: pending.requestedAt,
            response: resumeData,
          })
        : undefined;
    const persistedResponse =
      responseId !== undefined ? assertJsonValue(resumeData, `respond[${expectedKind}].response`) : undefined;
    const existingReceipt =
      responseId !== undefined ? getOwnRecordValue(this._record.inboxResponseReceipts, responseId) : undefined;
    if (existingReceipt !== undefined) {
      this._assertMatchingInboxReceipt(existingReceipt, {
        kind: expectedKind,
        itemId,
        responseId: responseId!,
        responseHash: responseHash!,
      });
      this._throwStoredInboxResponseFailure(existingReceipt);
      if (
        responseMode === 'inbox-receipt' &&
        (pending.resumedAt === undefined || existingReceipt.status === 'applied')
      ) {
        return this._inboxReceiptResult(existingReceipt, true);
      }
      if (existingReceipt.status === 'applied' && existingReceipt.result !== undefined) {
        return existingReceipt.result as AgentResult;
      }
    }

    // Idempotency: a crash between "marked resumed" and "cleared pending"
    // surfaces here on the next call. We do not replay the agent — the prior
    // resumeStream() either landed (and cleared pending in a later flush we
    // lost) or is being completed by a sibling caller. Either way, the safe
    // move is to surface the suspended state to the caller and let them
    // re-fetch via getDisplayState / listMessages.
    if (pending.resumedAt !== undefined) {
      if (existingReceipt !== undefined && responseMode === 'inbox-receipt') {
        const recovery = await this._maybeRecoverStaleQueuedResume();
        if (recovery.status === 'completed') {
          await this._markInboxResponseApplied(existingReceipt.responseId, recovery.result);
          const receipt =
            getOwnRecordValue(this._record.inboxResponseReceipts, existingReceipt.responseId) ?? existingReceipt;
          return this._inboxReceiptResult(receipt, true);
        }
        if (recovery.status === 'stale') {
          const stale = new QueueResumeRecoveryStaleError();
          await this._markInboxResponseFailed(existingReceipt.responseId, stale);
          throw stale;
        }
        if (
          this._currentTurnAbortController === undefined &&
          this._queuedItemIdForPendingResume(pending) === undefined &&
          Date.now() >= pending.resumedAt + QUEUE_ACCEPTED_RECOVERY_STALE_MS
        ) {
          const stale = new QueueResumeRecoveryStaleError();
          await this._markInboxResponseFailedAndClearPending(existingReceipt.responseId, pending, stale);
          throw stale;
        }
        return this._inboxReceiptResult(existingReceipt, true);
      }
      const recovery = await this._maybeRecoverStaleQueuedResume();
      if (recovery.status === 'completed') {
        if (responseMode === 'inbox-receipt') {
          throw new HarnessValidationError(
            `respond[${expectedKind}]`,
            'pending resume already responded; no matching inbox response receipt exists',
          );
        }
        return recovery.result;
      }
      if (recovery.status === 'stale') {
        const stale = new QueueResumeRecoveryStaleError();
        if (responseId !== undefined) {
          await this._markInboxResponseFailed(responseId, stale);
        }
        throw stale;
      }
      if (
        this._currentTurnAbortController === undefined &&
        this._queuedItemIdForPendingResume(pending) === undefined &&
        Date.now() >= pending.resumedAt + QUEUE_ACCEPTED_RECOVERY_STALE_MS
      ) {
        const stale = new QueueResumeRecoveryStaleError();
        await this._markInboxResponseFailedAndClearPending(responseId, pending, stale);
        throw stale;
      }
      throw new HarnessValidationError(
        `respond[${expectedKind}]`,
        'pending resume already responded; awaiting agent confirmation',
      );
    }

    const pendingQueuedItemId = this._queuedItemIdForPendingResume(pending);
    if (pendingQueuedItemId !== undefined) {
      this._ensureQueuedItemContext(pendingQueuedItemId);
    }

    // For plan-approval, resolve the active-mode flip before finalizing the
    // resumed turn. Queued terminal resumes persist this flip with the
    // completed receipt so crash recovery cannot observe "completed plan
    // approval, old mode".
    //
    // Resolution order on approval:
    //   1. Caller-supplied `transitionToMode` overrides everything.
    //   2. Falls back to the submitting mode's declared `transitionsTo`
    //      (captured into `pending.transitionModeId` at suspend time).
    //   3. Otherwise no flip.
    let modeFlipTarget: string | undefined;
    if (expectedKind === 'plan-approval') {
      const data = resumeData as { approved: boolean; transitionToMode?: string };
      if (data.approved) {
        const candidate = data.transitionToMode ?? pending.transitionModeId;
        if (candidate && candidate !== this._record.modeId) {
          // Validate the target mode exists before we hand off to the agent.
          // (Caller-supplied `transitionToMode` is also validated up-front in
          // `respondToPlanApproval`; this catches the pending-record path.)
          this._harness._getMode(candidate);
          modeFlipTarget = candidate;
        }
      }
    }

    const previousModeId = this._record.modeId;
    const resumeModeId = this._modeIdForPendingResume(pending);
    const resumeRuntimeDependencies = this._runtimeDependenciesForPendingResume(pending);
    let agent: Agent;
    try {
      agent = this._harness._resolveAgentForRuntimeDependencies(
        resumeRuntimeDependencies,
        `pending ${expectedKind} resume`,
      ).agent;
    } catch (err) {
      if (responseId !== undefined) {
        await this._recordInboxResponsePreDispatchFailure(
          {
            responseId,
            responseHash: responseHash!,
            itemId,
            queuedItemId: pendingQueuedItemId,
            kind: expectedKind,
            pending,
            response: persistedResponse,
          },
          err,
        );
      }
      throw err;
    }

    // Mark resumed under the lease BEFORE calling the agent (idempotency
    // marker per §5.4 / §5.7). On crash here, the next caller observes
    // resumedAt set and rejects rather than double-resuming.
    const resumedAt = Date.now();
    let duplicateReceiptAfterAdmission: InboxResponseReceipt | undefined;
    let pendingAlreadyResumedAfterAdmission = false;
    let cancelledBeforeResume: { reason?: string } | undefined;
    await this._flushUpdate(prev => {
      if (prev.cancelRequest !== undefined && prev.pendingResume?.resumedAt === undefined) {
        cancelledBeforeResume = prev.cancelRequest.reason !== undefined ? { reason: prev.cancelRequest.reason } : {};
        return prev;
      }

      const currentReceipt =
        responseId !== undefined ? getOwnRecordValue(prev.inboxResponseReceipts, responseId) : undefined;
      if (currentReceipt !== undefined) {
        this._assertMatchingInboxReceipt(currentReceipt, {
          kind: expectedKind,
          itemId,
          responseId: currentReceipt.responseId,
          responseHash: responseHash!,
        });
        duplicateReceiptAfterAdmission = currentReceipt;
        return prev;
      }

      const currentPending = prev.pendingResume;
      if (
        currentPending === undefined ||
        currentPending.resumedAt !== undefined ||
        currentPending.kind !== expectedKind ||
        currentPending.runId !== pending.runId ||
        currentPending.toolCallId !== pending.toolCallId ||
        (currentPending.itemId ?? currentPending.toolCallId) !== itemId
      ) {
        pendingAlreadyResumedAfterAdmission = true;
        return prev;
      }

      const next: SessionRecord = {
        ...prev,
        pendingResume: { ...currentPending, resumedAt },
      };
      if (responseId === undefined) return next;

      next.inboxResponseReceipts = {
        ...(prev.inboxResponseReceipts ?? {}),
        [responseId]: {
          responseId,
          responseHash: responseHash!,
          resumeAttemptId: responseId,
          itemId,
          ...(pendingQueuedItemId !== undefined ? { queuedItemId: pendingQueuedItemId } : {}),
          kind: expectedKind,
          runId: pending.runId,
          toolCallId: pending.toolCallId,
          pendingRequestedAt: pending.requestedAt,
          response: persistedResponse,
          status: 'accepted',
          acceptedAt: resumedAt,
          updatedAt: resumedAt,
        } satisfies InboxResponseReceipt,
      };
      return next;
    });
    if (cancelledBeforeResume !== undefined) {
      throw new HarnessSessionCancelledError(this.id, cancelledBeforeResume.reason);
    }
    if (duplicateReceiptAfterAdmission !== undefined) {
      this._throwStoredInboxResponseFailure(duplicateReceiptAfterAdmission);
      return this._inboxReceiptResult(duplicateReceiptAfterAdmission, true);
    }
    if (pendingAlreadyResumedAfterAdmission) {
      const recovery = await this._maybeRecoverStaleQueuedResume();
      if (recovery.status === 'completed') {
        if (responseMode === 'inbox-receipt') {
          throw new HarnessValidationError(
            `respond[${expectedKind}]`,
            'pending resume already responded; no matching inbox response receipt exists',
          );
        }
        return recovery.result;
      }
      if (recovery.status === 'stale') {
        const stale = new QueueResumeRecoveryStaleError();
        if (responseId !== undefined) {
          await this._markInboxResponseFailed(responseId, stale);
        }
        throw stale;
      }
      throw new HarnessValidationError(
        `respond[${expectedKind}]`,
        'pending resume already responded; awaiting agent confirmation',
      );
    }

    // §10.2 defines no sandbox_access_resolved event — sandbox/path-access
    // prompts surface as question_pending and resolve via the inbox response
    // transition + display snapshot (no dedicated resolved event).

    // Resumed runs run under a session-owned AbortController too, so
    // `session.abort()` can cancel an in-flight resume (e.g. ESC after the
    // user approved a tool that's now grinding through a long workflow).
    const turnAbortController = this._beginTurn(undefined);
    const activeTurnWaiter = this._createActiveTurnWaiter();
    void activeTurnWaiter.promise.catch(() => {});
    const finishResumedTurn = () => {
      activeTurnWaiter.cleanup();
      this._endTurn(turnAbortController);
    };
    const assertResumedTurnNotDeleted = () => {
      if (this._state === 'deleted') {
        throw new HarnessSessionDeletedError(this.id, this._record.resourceId, this._record.threadId);
      }
    };
    try {
      this._assertOpenForTurn(`respond[${expectedKind}]`);
    } catch (err) {
      let thrown = err;
      if (responseId !== undefined) {
        try {
          await this._markInboxResponseFailed(responseId, err);
        } catch (responseErr) {
          thrown = responseErr;
        }
      }
      finishResumedTurn();
      throw thrown;
    }
    let full: FullOutput<unknown>;
    try {
      assertResumedTurnNotDeleted();
      const resumeStream = agent.resumeStream(resumeData, {
        runId: pending.runId,
        toolCallId: pending.toolCallId,
        abortSignal: turnAbortController.signal,
      });
      void resumeStream.catch(() => {});
      const out = await Promise.race([resumeStream, activeTurnWaiter.promise]);
      // §10.4 — suspension events interleave with text/tool events on the live
      // subscriber stream and are followed by a `tool_end` after resume. The
      // resume run REUSES the suspended run's `runId` (`pending.runId`), which is
      // already in the long-lived thread subscription's `seenRunIds`
      // (thread-stream-runtime.ts), so that subscription dedups the re-registered
      // resume run and never re-drains it. Drain the resume run's own
      // `fullStream` through the same `_emitForChunk` path so the approved tool's
      // `tool_end` and any post-approval `text_delta` surface LIVE to subscribers
      // before the terminal `agent_end`. This local drain is the SOLE consumer of
      // the resumed segment's chunks (no double-emit). Completion delivery stays
      // on the independent `getFullOutput()` path below — draining is a separate
      // evented reader over the shared chunk buffer and does not gate settlement.
      const fullOutput = out.getFullOutput() as Promise<FullOutput<unknown>>;
      void fullOutput.catch(() => {});
      const resumeDrain = this._drainResumeStream(out as MastraModelOutput<unknown>);
      void resumeDrain.catch(() => {});
      full = await Promise.race([fullOutput, activeTurnWaiter.promise]);
      // The drain feeds `tool_end`/`text_delta` for the resumed segment; await it
      // (best-effort) so those live events are emitted before the terminal
      // `agent_end` below. A drain error must not fail the resume — settlement
      // already came from `getFullOutput()`.
      await Promise.race([resumeDrain.catch(() => {}), activeTurnWaiter.promise]);
      const resumedQueuedItemId = this._queuedItemIdForPendingResume(pending);
      if (full.finishReason !== 'suspended' && resumedQueuedItemId !== undefined) {
        await Promise.race([
          this._markQueuedTurnCompleted(resumedQueuedItemId, full, { modeId: modeFlipTarget }),
          activeTurnWaiter.promise,
        ]);
      }
    } catch (err) {
      let thrown = err;
      if (responseId !== undefined) {
        try {
          await Promise.race([this._markInboxResponseFailed(responseId, err), activeTurnWaiter.promise]);
        } catch (responseErr) {
          thrown = responseErr;
        }
      }
      // §4.2f — a failed resume of an owned `signal()` turn terminalizes that
      // signal's still-`pending` durable evidence as `signal_failed`, mirroring
      // the owned-turn failure path; otherwise it would stay pending forever.
      if (pending.originSignalId !== undefined) {
        await this._settleSignalResult(pending.originSignalId, {
          status: 'failed',
          runId: pending.runId,
          error: projectHarnessPublicError(err),
        });
      }
      finishResumedTurn();
      throw thrown;
    }

    // Clear pending + apply any remaining mode flip in a single CAS write. A
    // queued terminal resume has already persisted its completed receipt before
    // this point, so crash recovery never sees "pending cleared, queue still
    // accepted".
    const completingQueuedItemId = full.finishReason !== 'suspended' ? pendingQueuedItemId : undefined;
    try {
      const suspendedPayload =
        full.finishReason === 'suspended'
          ? (full.suspendPayload as
              | { toolCallId: string; toolName: string; args?: unknown; suspendPayload?: unknown; approvalReasons?: string[] }
              | undefined)
          : undefined;
      const suspendedPending =
        suspendedPayload !== undefined
          ? this._pendingResumeFromSuspendedOutput(
              full,
              suspendedPayload,
              pendingQueuedItemId,
              resumeModeId,
              resumeRuntimeDependencies.modelId,
              pending.originSignalId,
            )
          : undefined;
      const suspendedTokenUsageDelta =
        full.finishReason === 'suspended' ? this._tokenUsageDeltaFromFullOutput(full) : undefined;
      if (full.finishReason === 'suspended') this._captureTurnRunId(full);
      let alreadyAccounted = false;
      if (completingQueuedItemId !== undefined) {
        if (modeFlipTarget && modeFlipTarget !== previousModeId) {
          await Promise.race([
            this._flushUpdate(prev => ({ ...prev, modeId: modeFlipTarget })),
            activeTurnWaiter.promise,
          ]);
        }
        const queuedItem = this._record.pendingQueue.find(item => item.id === completingQueuedItemId);
        if (queuedItem) {
          let tokenUsageDelta: TokenUsage | undefined;
          try {
            this._captureTurnRunId(full);
            tokenUsageDelta = this._tokenUsageDeltaFromFullOutput(full);
            alreadyAccounted = true;
            await Promise.race([
              this._markQueuedPostRunFinalized(completingQueuedItemId, { tokenUsageDelta }),
              activeTurnWaiter.promise,
            ]);
          } catch (err) {
            if (err instanceof HarnessSessionDeletedError) throw err;
            throw new QueuePostRunFinalizationPendingError(Date.now() + QUEUE_POST_RUN_FINALIZATION_RETRY_MS, err);
          }
        }
        if (!alreadyAccounted) this._recordTurnCompletion(full, { persist: false });
      } else if (full.finishReason !== 'suspended') {
        this._recordTurnCompletion(full, { persist: false });
      }
      const queueCompletedAt = Date.now();
      const responseAppliedAt = queueCompletedAt;
      await Promise.race([
        this._flushUpdate(
          prev => {
            const next: SessionRecord = { ...prev };
            if (full.finishReason === 'suspended' && suspendedPending !== undefined) {
              next.pendingResume = suspendedPending;
            } else {
              delete next.pendingResume;
            }
            const receipt =
              responseId !== undefined ? getOwnRecordValue(prev.inboxResponseReceipts, responseId) : undefined;
            if (receipt) {
              next.inboxResponseReceipts = {
                ...(prev.inboxResponseReceipts ?? {}),
                [receipt.responseId]: {
                  ...receipt,
                  status: 'applied',
                  result: full,
                  appliedAt: receipt.appliedAt ?? responseAppliedAt,
                  updatedAt: responseAppliedAt,
                },
              };
            }
            if (modeFlipTarget) next.modeId = modeFlipTarget;
            if (completingQueuedItemId !== undefined) {
              next.pendingQueue = (prev.pendingQueue ?? []).filter(x => x.id !== completingQueuedItemId);
              const receipt = prev.queueAdmissionReceipts?.[completingQueuedItemId];
              if (receipt) {
                next.queueAdmissionReceipts = {
                  ...(prev.queueAdmissionReceipts ?? {}),
                  [completingQueuedItemId]: {
                    ...receipt,
                    status: 'completed',
                    result: full,
                    completedAt: receipt.completedAt ?? queueCompletedAt,
                    updatedAt: queueCompletedAt,
                  },
                };
              }
            }
            return next;
          },
          {
            tokenUsageDelta:
              full.finishReason === 'suspended' && suspendedTokenUsageDelta !== undefined
                ? suspendedTokenUsageDelta
                : undefined,
          },
        ),
        activeTurnWaiter.promise,
      ]);

      // §10.2 defines no suspension_resolved event — resolution is observed via
      // the inbox response transition + display snapshot. A mode flip on a plan
      // approval still emits mode_changed; a re-suspension emits the matching
      // §10.2 pending event.
      if (modeFlipTarget && modeFlipTarget !== previousModeId) {
        this._emitter.emit({
          type: 'mode_changed',
          modeId: modeFlipTarget,
          previousModeId,
        });
      }

      if (suspendedPending !== undefined) {
        this._emitPendingEvent(suspendedPending);
      }

      // If the resumed run did NOT suspend again, the turn is complete from
      // the harness's perspective. Surface that to subscribers via agent_end.
      if (full.finishReason !== 'suspended') {
        this._emitAgentEnd({ runId: full.runId, finishReason: this._agentEndReasonForFullOutput(full), full });

        // §4.2f — an owned `signal()` turn that suspended left its durable
        // per-`signalId` evidence `pending`. The terminal (non-suspended)
        // resume IS that signal's answer (1:1), so settle the evidence and
        // project `signal_completed`/`signal_failed` now; otherwise a suspended
        // owned signal would stay pending forever. Queued turns are settled via
        // their queue receipt above and never carry `originSignalId`.
        if (pending.originSignalId !== undefined) {
          await this._settleSignalResult(pending.originSignalId, {
            status: 'completed',
            runId: pending.runId,
            result: full as AgentResult,
          });
        }

        // If this was the terminal completion of a queued turn, settle the
        // resolver, remove the head item, clear current, then kick the drain
        // for the next item.
        const wasGoalDriven = (this._currentQueuedItemSource ?? 'user') === 'goal';
        await Promise.race([this._runGoalJudge(full, wasGoalDriven), activeTurnWaiter.promise]);
        if (completingQueuedItemId !== undefined) {
          this._currentQueuedItemId = undefined;
          this._currentQueuedItemSource = undefined;
          const resolver = this._queueResolvers.get(completingQueuedItemId);
          if (resolver) {
            this._queueResolvers.delete(completingQueuedItemId);
            resolver.resolve(full as AgentResult);
          }
          this._notifyMaybeIdle();
          void this._maybeDrainQueue();
        }
      }
    } catch (err) {
      if (err instanceof QueuePostRunFinalizationPendingError && completingQueuedItemId !== undefined) {
        this._deferQueuedTurnRetry(err);
      }
      throw err;
    } finally {
      finishResumedTurn();
    }
    if (responseMode === 'inbox-receipt') {
      const receipt =
        responseId !== undefined ? getOwnRecordValue(this._record.inboxResponseReceipts, responseId) : undefined;
      if (receipt) return this._inboxReceiptResult(receipt, false);
    }
    return full as AgentResult;
  }

  private _resolveStoredInboxResponse(
    expectedKind: PendingResume['kind'],
    resumeData: unknown,
    responseOptions: InboxResponseOptions,
  ): InboxResponseResult | undefined {
    const responseId = getOwnRecordValue(responseOptions as Record<string, unknown>, 'responseId');
    if (typeof responseId !== 'string') return undefined;
    const receipt = getOwnRecordValue(this._record.inboxResponseReceipts, responseId);
    if (receipt === undefined) return undefined;
    if (receipt.kind !== expectedKind) {
      throw new HarnessInboxResponseConflictError(this.id, receipt.itemId, responseId);
    }
    const requestedItemId = getOwnRecordValue(responseOptions as Record<string, unknown>, 'itemId');
    if (requestedItemId !== undefined && typeof requestedItemId !== 'string') {
      throw new HarnessValidationError(`respond[${expectedKind}].itemId`, 'itemId must be a string');
    }
    if (requestedItemId !== undefined && receipt.itemId !== requestedItemId) {
      throw new HarnessInboxItemNotFoundError(this.id, requestedItemId, expectedKind === 'sandbox-access' ? undefined : expectedKind);
    }
    const attemptedHash = this._computeInboxResponseHash({
      kind: expectedKind,
      itemId: receipt.itemId,
      runId: receipt.runId,
      pendingRequestedAt: receipt.pendingRequestedAt,
      response: resumeData,
    });
    if (attemptedHash !== receipt.responseHash) {
      throw new HarnessInboxResponseConflictError(this.id, receipt.itemId, responseId);
    }
    this._throwStoredInboxResponseFailure(receipt);
    return this._inboxReceiptResult(receipt, true);
  }

  private _assertMatchingInboxReceipt(
    receipt: InboxResponseReceipt,
    input: { kind: PendingResume['kind']; itemId: string; responseId: string; responseHash: string },
  ): void {
    if (receipt.kind !== input.kind || receipt.itemId !== input.itemId || receipt.responseHash !== input.responseHash) {
      throw new HarnessInboxResponseConflictError(this.id, input.itemId, input.responseId);
    }
  }

  private _inboxReceiptResult(receipt: InboxResponseReceipt, duplicate: boolean): InboxResponseResult {
    return {
      itemId: receipt.itemId,
      kind: receipt.kind,
      status: receipt.status === 'applied' ? 'applied' : 'accepted',
      responseId: receipt.responseId,
      duplicate,
    };
  }

  private _throwStoredInboxResponseFailure(receipt: InboxResponseReceipt): void {
    if (receipt.status !== 'failed' && receipt.status !== 'dead') return;
    throw publicErrorProjectionToError(
      receipt.error ?? { code: 'harness.inbox_response_failed', message: 'inbox response failed' },
    );
  }

  private async _applyInboxReceiptFromCompletedQueue(
    receipt: InboxResponseReceipt,
  ): Promise<InboxResponseReceipt | undefined> {
    if (receipt.queuedItemId === undefined) return undefined;
    const completed = this._record.queueAdmissionReceipts?.[receipt.queuedItemId];
    if (completed?.status !== 'completed' || completed.runId !== receipt.runId || completed.result === undefined) {
      return undefined;
    }
    await this._markInboxResponseApplied(receipt.responseId, completed.result as AgentResult);
    return getOwnRecordValue(this._record.inboxResponseReceipts, receipt.responseId) ?? receipt;
  }

  private async _markInboxResponseApplied(responseId: string, result: AgentResult): Promise<void> {
    const appliedAt = Date.now();
    await this._flushUpdate(prev => {
      const receipt = getOwnRecordValue(prev.inboxResponseReceipts, responseId);
      if (!receipt || receipt.status === 'applied') return prev;
      return {
        ...prev,
        inboxResponseReceipts: {
          ...(prev.inboxResponseReceipts ?? {}),
          [responseId]: {
            ...receipt,
            status: 'applied',
            result,
            appliedAt: receipt.appliedAt ?? appliedAt,
            updatedAt: appliedAt,
          },
        },
      };
    });
  }

  private async _recordInboxResponsePreDispatchFailure(
    input: {
      responseId: string;
      responseHash: string;
      itemId: string;
      queuedItemId?: string;
      kind: PendingResume['kind'];
      pending: PendingResume;
      response: unknown;
    },
    err: unknown,
  ): Promise<void> {
    const failedAt = Date.now();
    await this._flushUpdate(prev => {
      const currentReceipt = getOwnRecordValue(prev.inboxResponseReceipts, input.responseId);
      if (currentReceipt !== undefined) {
        this._assertMatchingInboxReceipt(currentReceipt, {
          kind: input.kind,
          itemId: input.itemId,
          responseId: input.responseId,
          responseHash: input.responseHash,
        });
        return prev;
      }
      return {
        ...prev,
        inboxResponseReceipts: {
          ...(prev.inboxResponseReceipts ?? {}),
          [input.responseId]: {
            responseId: input.responseId,
            responseHash: input.responseHash,
            resumeAttemptId: input.responseId,
            itemId: input.itemId,
            ...(input.queuedItemId !== undefined ? { queuedItemId: input.queuedItemId } : {}),
            kind: input.kind,
            runId: input.pending.runId,
            toolCallId: input.pending.toolCallId,
            pendingRequestedAt: input.pending.requestedAt,
            response: input.response,
            status: 'failed',
            error: projectHarnessPublicError(err),
            retryable: false,
            acceptedAt: failedAt,
            failedAt,
            updatedAt: failedAt,
          } satisfies InboxResponseReceipt,
        },
      };
    });
  }

  private async _markInboxResponseFailed(responseId: string, err: unknown): Promise<void> {
    const failedAt = Date.now();
    await this._flushUpdate(prev => {
      const receipt = getOwnRecordValue(prev.inboxResponseReceipts, responseId);
      if (!receipt || receipt.status === 'applied' || receipt.status === 'failed' || receipt.status === 'dead') {
        return prev;
      }
      return {
        ...prev,
        inboxResponseReceipts: {
          ...(prev.inboxResponseReceipts ?? {}),
          [responseId]: {
            ...receipt,
            status: 'failed',
            error: projectHarnessPublicError(err),
            retryable: false,
            failedAt: receipt.failedAt ?? failedAt,
            updatedAt: failedAt,
          },
        },
      };
    });
  }

  private async _markInboxResponseFailedAndClearPending(
    responseId: string | undefined,
    pending: PendingResume,
    err: unknown,
  ): Promise<void> {
    const failedAt = Date.now();
    await this._flushUpdate(prev => {
      const receipt = responseId !== undefined ? getOwnRecordValue(prev.inboxResponseReceipts, responseId) : undefined;
      const current = prev.pendingResume;
      const currentItemId = current ? (current.itemId ?? current.toolCallId) : undefined;
      const pendingItemId = pending.itemId ?? pending.toolCallId;
      const canClearPending =
        current !== undefined &&
        current.runId === pending.runId &&
        current.toolCallId === pending.toolCallId &&
        currentItemId === pendingItemId &&
        current.resumedAt === pending.resumedAt &&
        current.queuedItemId === undefined;

      if (
        (!receipt || receipt.status === 'applied' || receipt.status === 'failed' || receipt.status === 'dead') &&
        !canClearPending
      ) {
        return prev;
      }

      const next: SessionRecord = { ...prev };
      if (receipt && receipt.status !== 'applied' && receipt.status !== 'failed' && receipt.status !== 'dead') {
        next.inboxResponseReceipts = {
          ...(prev.inboxResponseReceipts ?? {}),
          [receipt.responseId]: {
            ...receipt,
            status: 'failed',
            error: projectHarnessPublicError(err),
            retryable: false,
            failedAt: receipt.failedAt ?? failedAt,
            updatedAt: failedAt,
          },
        };
      }
      if (canClearPending) {
        delete next.pendingResume;
      }
      return next;
    });
  }

  private _computeInboxResponseHash(input: {
    kind: PendingResume['kind'];
    itemId: string;
    runId: string;
    pendingRequestedAt: number;
    response: unknown;
  }): string {
    return sha256CanonicalJson(input);
  }

  private _queuedItemIdForPendingResume(pending: PendingResume): string | undefined {
    if (pending.queuedItemId !== undefined) return pending.queuedItemId;
    if (this._currentQueuedItemId !== undefined) return this._currentQueuedItemId;
    return (this._record.pendingQueue ?? []).find(item => {
      const receipt = this._record.queueAdmissionReceipts?.[item.id];
      return (receipt?.status === 'accepted' || receipt?.status === 'completed') && receipt.runId === pending.runId;
    })?.id;
  }

  private _modeIdForPendingResume(pending: PendingResume): string {
    const queuedItemId = this._queuedItemIdForPendingResume(pending);
    const queuedItem = queuedItemId ? this._record.pendingQueue.find(item => item.id === queuedItemId) : undefined;
    const receipt = queuedItemId ? this._record.queueAdmissionReceipts?.[queuedItemId] : undefined;
    return pending.modeId ?? receipt?.modeId ?? queuedItem?.mode ?? this._record.modeId;
  }

  private _modelIdForQueuedItem(queuedItemId: string | undefined): string {
    const queuedItem = queuedItemId ? this._record.pendingQueue.find(item => item.id === queuedItemId) : undefined;
    const receipt = queuedItemId ? this._record.queueAdmissionReceipts?.[queuedItemId] : undefined;
    return queuedItem?.model ?? receipt?.runtimeDependencies?.modelId ?? this._record.modelId;
  }

  private _runtimeDependenciesForPendingResume(pending: PendingResume): HarnessRuntimeDependencyRefs {
    const queuedItemId = this._queuedItemIdForPendingResume(pending);
    const queuedItem = queuedItemId ? this._record.pendingQueue.find(item => item.id === queuedItemId) : undefined;
    const receipt = queuedItemId ? this._record.queueAdmissionReceipts?.[queuedItemId] : undefined;
    const modeId = this._modeIdForPendingResume(pending);
    const modelId = queuedItem?.model ?? this._record.modelId;
    return pending.runtimeDependencies ?? receipt?.runtimeDependencies ?? { modeId, ...(modelId ? { modelId } : {}) };
  }

  private async _maybeRecoverStaleQueuedResume(): Promise<QueueResumeRecoveryResult> {
    if (this._currentTurnAbortController !== undefined) return { status: 'none' };
    const pending = this._record.pendingResume;
    if (pending?.resumedAt === undefined) return { status: 'none' };
    const queuedItemId = this._queuedItemIdForPendingResume(pending);
    if (queuedItemId === undefined) return { status: 'none' };
    // §10.2: no queue_item_replayed event on crash-recovery — the recovered
    // turn flows through normal drain/agent_start; settlement evidence stays durable.
    this._ensureQueuedItemContext(queuedItemId);

    const currentReceipt = this._record.queueAdmissionReceipts?.[queuedItemId];
    if (currentReceipt?.status === 'completed') {
      const queuedItem = this._record.pendingQueue.find(item => item.id === queuedItemId);
      const shouldRunPostRunSideEffects = queuedItem !== undefined && currentReceipt.postRunFinalizedAt === undefined;
      let alreadyAccounted = false;
      if (shouldRunPostRunSideEffects) {
        let tokenUsageDelta: TokenUsage | undefined;
        try {
          const full = currentReceipt.result as FullOutput<unknown>;
          this._captureTurnRunId(full);
          tokenUsageDelta = this._tokenUsageDeltaFromFullOutput(full);
          alreadyAccounted = true;
          await this._markQueuedPostRunFinalized(queuedItemId, { tokenUsageDelta });
        } catch (err) {
          this._deferQueuedTurnRetry(
            new QueuePostRunFinalizationPendingError(Date.now() + QUEUE_POST_RUN_FINALIZATION_RETRY_MS, err),
          );
          return { status: 'none' };
        }
      }
      await this._flushUpdate(prev => {
        const current = prev.pendingResume;
        if (
          !current ||
          current.runId !== pending.runId ||
          current.toolCallId !== pending.toolCallId ||
          current.resumedAt !== pending.resumedAt
        ) {
          return prev;
        }
        const next: SessionRecord = {
          ...prev,
          pendingQueue: (prev.pendingQueue ?? []).filter(item => item.id !== queuedItemId),
        };
        delete next.pendingResume;
        return next;
      });
      if (shouldRunPostRunSideEffects && queuedItem) {
        await this._finalizeQueuedRunCompletion(
          queuedItem,
          currentReceipt.result as FullOutput<unknown>,
          pending.modeId ?? currentReceipt.modeId ?? queuedItem.mode ?? this._record.modeId,
          undefined,
          { skipTokenAccounting: alreadyAccounted },
        );
      }
      this._currentQueuedItemId = undefined;
      this._currentQueuedItemSource = undefined;
      const resolver = this._queueResolvers.get(queuedItemId);
      if (resolver) {
        this._queueResolvers.delete(queuedItemId);
        resolver.resolve(currentReceipt.result as AgentResult);
      }
      this._notifyMaybeIdle();
      void this._maybeDrainQueue();
      return { status: 'completed', result: currentReceipt.result as AgentResult };
    }

    const retryAt = pending.resumedAt + QUEUE_ACCEPTED_RECOVERY_STALE_MS;
    if (Date.now() < retryAt) {
      if (this._queuedResumeRecoveryTimer === undefined) {
        const delayMs = Math.max(0, retryAt - Date.now());
        this._queuedResumeRecoveryTimer = setTimeout(() => {
          this._queuedResumeRecoveryTimer = undefined;
          void this._maybeDrainQueue();
        }, delayMs);
        this._queuedResumeRecoveryTimer.unref?.();
      }
      return { status: 'none' };
    }

    if (this._queuedResumeRecoveryTimer !== undefined) {
      clearTimeout(this._queuedResumeRecoveryTimer);
      this._queuedResumeRecoveryTimer = undefined;
    }

    const err = new QueueResumeRecoveryStaleError();
    const now = Date.now();
    await this._flushUpdate(prev => {
      const current = prev.pendingResume;
      if (
        !current ||
        current.runId !== pending.runId ||
        current.toolCallId !== pending.toolCallId ||
        current.resumedAt !== pending.resumedAt
      ) {
        return prev;
      }
      const receipt = prev.queueAdmissionReceipts?.[queuedItemId];
      const next: SessionRecord = {
        ...prev,
        pendingQueue: (prev.pendingQueue ?? []).filter(item => item.id !== queuedItemId),
      };
      delete next.pendingResume;
      if (receipt) {
        if (receipt.status === 'completed') {
          return next;
        }
        next.queueAdmissionReceipts = {
          ...(prev.queueAdmissionReceipts ?? {}),
          [queuedItemId]: {
            ...receipt,
            status: 'failed',
            error: projectHarnessPublicError(err),
            failedAt: receipt.failedAt ?? now,
            updatedAt: now,
          },
        };
      }
      return next;
    });

    const current = this._record.pendingResume;
    if (
      current?.runId === pending.runId &&
      current.toolCallId === pending.toolCallId &&
      current.resumedAt === pending.resumedAt
    ) {
      return { status: 'none' };
    }

    this._currentQueuedItemId = undefined;
    this._currentQueuedItemSource = undefined;
    const resolver = this._queueResolvers.get(queuedItemId);
    if (resolver) {
      this._queueResolvers.delete(queuedItemId);
      resolver.reject(err);
    }
    this._notifyMaybeIdle();
    return { status: 'stale' };
  }

  // -------------------------------------------------------------------------
  // queue() — wait-for-idle FIFO turn queue (§4.2 / §6).
  //
  // Append-then-drain. The capacity check + durable append are atomic per
  // session: two concurrent `queue()` calls on the same Session instance
  // race on the in-process `_flushUpdate` lock, so neither can both observe
  // available space and commit past the cap. Cross-instance contention is
  // covered by the lease + version CAS on the underlying record.
  //
  // Drain semantics:
  //   1. `queue()` admits → flush record → register resolver → kick drain.
  //   2. Drain pulls head item, emits `queue_item_started`, runs the turn
  //      by dispatching a deterministic `agent.sendSignal()` turn (so
  //      `agent_start`, `message_*`, `tool_*`, `suspension_*`, `agent_end`
  //      all flow with `queuedItemId` stamped automatically by
  //      `_emitTurnEvent`).
  //   3. If the turn suspends, the head item stays in `pendingQueue` and
  //      `_currentQueuedItemId` stays set. The next `respondTo*` call calls
  //      into `_resume`; on terminal completion the resume path settles the
  //      resolver + removes the head + kicks drain again.
  //   4. If the turn completes without suspending, the queue receipt,
  //      signal-result evidence, resolver, and head item settle together.
  //
  // Promise resolution: the eventual `AgentResult` once the turn fully ends
  // (including any suspend → resume cycles). Rejection surfaces admission
  // conflicts, queued-run failures, stale accepted recovery, or expired
  // duplicate-result evidence.
  // -------------------------------------------------------------------------

  /**
   * Append a turn to the durable queue. Resolves with the eventual
   * `AgentResult` once the turn fully completes — including any
   * suspend → resume cycles.
   *
   * Rejects synchronously with:
   *   - `HarnessConfigError` if the session is not live, or `mode` is unknown.
   *   - `HarnessValidationError` if `content` is empty.
   *   - `HarnessQueueFullError` if `pendingQueue.length` is already at
   *     `sessions.maxQueueDepth`.
   *   - `HarnessSessionCancelledError` if the session has been cancelled.
   */
  async queue(opts: QueueOptions): Promise<AgentResult> {
    const admission = await this._admitQueue(opts, 'queue()');
    if (admission.duplicate) {
      return this._withActiveDeletedWaiter(activeDeleted =>
        this._raceActiveTurnWaiter(this._returnDuplicateQueueResult(admission.evidence, activeDeleted), activeDeleted),
      );
    }

    const queued = createDeferred<AgentResult>();
    const promise = queued.promise;
    const latestReceipt = this._record.queueAdmissionReceipts?.[admission.queuedItemId];
    const terminalAdmissionError = this._queueReceiptTerminalFailureErrorFromReceipt(latestReceipt);
    if (terminalAdmissionError) {
      queued.reject(
        latestReceipt?.status === 'failed' && latestReceipt.error?.code === 'harness.queue_full_dropped'
          ? new HarnessQueueFullDroppedError(admission.queuedItemId)
          : terminalAdmissionError,
      );
      void promise.catch(() => {});
      return promise;
    }
    if (latestReceipt?.status === 'completed') {
      queued.resolve(latestReceipt.result as AgentResult);
      return promise;
    }

    this._queueResolvers.set(admission.queuedItemId, { promise, resolve: queued.resolve, reject: queued.reject });
    // Kick the drain — fire-and-forget. Drain handles its own errors and
    // settles the resolver via `_completeQueuedTurn` / `_failQueuedTurn`.
    void this._maybeDrainQueue();
    void promise.catch(() => {});
    return promise;
  }

  /**
   * Admit a queued turn without awaiting its eventual AgentResult. This is the
   * remote-route counterpart to `queue(...)`; SDK promises settle from
   * session events or result lookup routes.
   */
  async admitQueue(opts: QueueOptions): Promise<QueueAdmissionResult> {
    if (opts.admissionId === undefined || opts.admissionId.length === 0) {
      throw new HarnessValidationError('admitQueue().admissionId', 'admissionId must be a non-empty string');
    }
    const admission = await this._admitQueue(opts, 'admitQueue()');
    if (!admission.duplicate) {
      this._liveAdmittedQueuedItemIds.add(admission.queuedItemId);
    }
    void this._maybeDrainQueue();
    return { accepted: true, queuedItemId: admission.queuedItemId, duplicate: admission.duplicate };
  }

  /**
   * @internal §14.2 channel ingress admission via the queue path. Like
   * `admitQueue` but threads a persisted `requestContext` (the trusted
   * `requestContext.channel` projection built by the bridge) into the queued item
   * so the admitted turn — and its crash recovery — carry channel provenance.
   * `admissionId` (the inbox item id) makes a provider/worker retry idempotent.
   */
  async _admitChannelQueueTurn(opts: {
    content: string;
    admissionId: string;
    requestContext: PersistedRequestContextInput;
    mode?: string;
    model?: string;
    // Already-normalized persisted attachments from the durable inbox row. The
    // channel bridge resolves provider files to Harness-owned refs before the row
    // exists (§14.2 step 3); a queued/retried turn never depends on live bytes.
    attachments?: PersistedAttachment[];
    // §14.2 step 7/8: when the bridge has already computed and persisted the
    // admissionHash, it is replayed here so the queue boundary rejects a payload
    // that does not match the persisted admission (recovery integrity).
    expectedAdmissionHash?: string;
  }): Promise<QueueAdmissionResult> {
    if (opts.admissionId.length === 0) {
      throw new HarnessValidationError('admitChannelQueueTurn().admissionId', 'admissionId must be a non-empty string');
    }
    const queueOpts: QueueOptions = {
      content: opts.content,
      admissionId: opts.admissionId,
      ...(opts.mode !== undefined ? { mode: opts.mode } : {}),
      ...(opts.model !== undefined ? { model: opts.model } : {}),
    };
    const admission = await this._admitQueue(queueOpts, 'admitQueue()', {
      persistedRequestContext: opts.requestContext,
      ...(opts.attachments !== undefined ? { persistedAttachments: opts.attachments } : {}),
      ...(opts.expectedAdmissionHash !== undefined ? { expectedAdmissionHash: opts.expectedAdmissionHash } : {}),
    });
    if (!admission.duplicate) {
      this._liveAdmittedQueuedItemIds.add(admission.queuedItemId);
    }
    void this._maybeDrainQueue();
    return { accepted: true, queuedItemId: admission.queuedItemId, duplicate: admission.duplicate };
  }

  /**
   * @internal §14.2 step 7: the queue admissionHash for a channel turn, computed
   * over the exact persisted admission payload (content + persisted attachments +
   * policy-selected mode/model + trusted requestContext). The bridge records this
   * on the inbox row BEFORE runtime admission so recovery replays the SAME payload
   * and validates it via {@link _admitChannelQueueTurn}'s `expectedAdmissionHash`
   * — never re-running channel policy once the hash exists.
   */
  _channelQueueAdmissionHash(
    opts: { content: string; mode?: string; model?: string },
    attachments: PersistedAttachment[],
    requestContext: PersistedRequestContextInput,
  ): string {
    const queueOpts: QueueOptions = {
      content: opts.content,
      ...(opts.mode !== undefined ? { mode: opts.mode } : {}),
      ...(opts.model !== undefined ? { model: opts.model } : {}),
    };
    return this._computeQueueAdmissionHash(queueOpts, attachments, requestContext);
  }

  /**
   * @internal §14.2 channel signal-admission duplicate resolution. Decides
   * whether existing reservation evidence proves a run was already DISPATCHED
   * (so the caller short-circuits without re-dispatching) or not (so the caller
   * re-dispatches under the deterministic signalId — closing the pre-dispatch
   * lost-signal window from C2/C7).
   *
   * - `failed`: rethrow the projected error (matches the message/queue
   *   duplicate-of-failed contract).
   * - `completed`: the run already answered — short-circuit to its identity.
   * - `pending` WITH a recorded `runId`: a run was durably dispatched under this
   *   admissionId — short-circuit to that real run (no double interleave/wake).
   * - `pending` WITHOUT a `runId`: the reservation committed but the dispatch
   *   never recorded a run (pre-dispatch crash window). Return `undefined` so the
   *   caller (re-)dispatches; NEVER fabricate `runId === signalId` (C7).
   * - tombstone / missing `status`: expired evidence — throw.
   * - `undefined` input (no evidence): `undefined` (caller proceeds to reserve).
   */
  /**
   * @internal §14.2 idempotency resolver for a replayed channel-signal admission.
   *
   * Channel-signal operation-admission evidence is intentionally a pre-dispatch
   * BARRIER, not the durable answer. `_admitChannelSignalTurn` reserves `pending`
   * evidence keyed by the deterministic signalId WITH admissionId/admissionHash,
   * then records the dispatched `runId` on that same `pending` row. The run's
   * completed/failed terminal settles through `_settleSignalResult`, which writes
   * evidence WITHOUT admissionId/admissionHash (it is the shared terminal for all
   * signals, channel or not, and carries no admission identity). Because
   * `sameMessageEvidenceIdentity` (storage) compares admissionId+admissionHash,
   * that terminal write is an identity mismatch the best-effort path swallows —
   * so this evidence row stays `pending` (with a runId) for the lifetime of the
   * operation. That is by design and matches §14.2 ("answer read-only from a
   * stored row that is already terminal or accepted/queued"): the DURABLE answer
   * for an idempotent channel duplicate lives on the channel INBOX row plus the
   * run terminal, not on this admission record. The `failed` branch below is
   * therefore defensive — it never fires for channel signals because the failure
   * terminal never reaches this row — and the `pending + runId` branch is the
   * live short-circuit that re-resolves a replay to its original run without
   * re-dispatching.
   */
  private _channelSignalDispatchedDuplicate(
    evidence: AgentSignalResultEvidence | OperationAdmissionTombstone | undefined,
  ): { runId: string; signalId: string; willInterleave: boolean } | undefined {
    if (evidence === undefined) return undefined;
    if (!('status' in evidence)) {
      throw new HarnessValidationError(
        'admitChannelSignalTurn().admissionId',
        'duplicate channel signal admission evidence has expired',
      );
    }
    if (evidence.status === 'failed') {
      throw publicErrorProjectionToError(evidence.error);
    }
    // A `pending` reservation with no durable `runId` was never delivered to a
    // run — signal re-dispatch, do not short-circuit to a fabricated identity.
    if (evidence.status === 'pending' && evidence.runId === undefined) return undefined;
    return {
      runId: evidence.runId!,
      signalId: evidence.signalId,
      willInterleave: false,
    };
  }

  /**
   * @internal §14.2 channel ingress admission via the SIGNAL path. The
   * interactive counterpart to {@link _admitChannelQueueTurn}: an inbound that
   * the channel policy selected `delivery: 'signal'` for interleaves into an
   * active run (§21 shared terminal) or wakes a fresh idle run, rather than
   * appending a sequential queue boundary. The trusted `requestContext.channel`
   * projection and the already-persisted inbox attachments are threaded via the
   * internal signal hook. `signal()` has no per-turn `model` override, so a
   * policy `model` is rejected by {@link admitChannelInbound} before reaching
   * here. Returns the run/signal ids the bridge persists on the inbox row.
   */
  async _admitChannelSignalTurn(opts: {
    content: string;
    admissionId: string;
    requestContext: PersistedRequestContextInput;
    expectedAdmissionHash: string;
    mode?: string;
    attachments?: PersistedAttachment[];
  }): Promise<{ runId: string; signalId: string; willInterleave: boolean }> {
    if (opts.admissionId.length === 0) {
      throw new HarnessValidationError('admitChannelSignalTurn().admissionId', 'admissionId must be a non-empty string');
    }
    if (opts.expectedAdmissionHash.length === 0) {
      throw new HarnessValidationError(
        'admitChannelSignalTurn().expectedAdmissionHash',
        'expectedAdmissionHash must be a non-empty string',
      );
    }
    // §14.2 idempotency barrier (signal analogue of the queue path's
    // `admissionId`-keyed `queueAdmissionReceipts` de-dup). The deterministic
    // per-`admissionId` signal id ties the dispatched run to a durable
    // reservation written BEFORE dispatch. On the admitted→accepted recovery
    // crash window the worker replays this method against the still-pending
    // row, but the existing reservation short-circuits to the ORIGINAL run
    // instead of firing a second interleave/wake (double model turn / spend).
    const signalId = this._channelSignalAdmissionSignalId(opts.admissionId);
    const existing = await this._resolveMessageAdmissionDuplicate({
      admissionId: opts.admissionId,
      admissionHash: opts.expectedAdmissionHash,
    });
    const dispatchedDuplicate = this._channelSignalDispatchedDuplicate(existing);
    if (dispatchedDuplicate !== undefined) return dispatchedDuplicate;
    // Reserve the admissionId durably BEFORE dispatch. The conflict-detecting
    // write rejects a payload-mismatched replay and resolves a concurrent winner
    // to its evidence (returned and short-circuited here, never double-dispatched).
    const reserved = await this._writeMessageResultEvidence({
      status: 'pending',
      signalId,
      admissionId: opts.admissionId,
      admissionHash: opts.expectedAdmissionHash,
    });
    if (!reserved.created) {
      const reservedDuplicate = this._channelSignalDispatchedDuplicate(reserved.evidence);
      if (reservedDuplicate !== undefined) return reservedDuplicate;
      // The reservation already exists but carries NO durable runId: a crash
      // between reserving and recording the dispatched run (the pre-dispatch
      // lost-signal window, C2/C7). The signal was never delivered to a run, so
      // fall through and (re-)dispatch under the SAME deterministic signalId — a
      // real run is created exactly once on recovery instead of fabricating a
      // runId for a run that never started. This is safe because channel-ingress
      // admission for a given admissionId is SERIALIZED by the durable inbox-row
      // claim (admitChannelInbound's initial claim / recoverChannelInboxOnce's
      // reclaim), so `_admitChannelSignalTurn` never runs concurrently for the
      // same admissionId — a pending-without-runId reservation is always the
      // single claimant's own crashed prior attempt, never a live concurrent run.
    }
    const handle = await this.signal(
      {
        content: opts.content,
        ...(opts.mode !== undefined ? { mode: opts.mode } : {}),
      },
      {
        persistedRequestContext: opts.requestContext,
        signalId,
        ...(opts.attachments !== undefined ? { attachments: opts.attachments } : {}),
      },
    );
    // Record the dispatched run on the durable reservation BEFORE returning so the
    // recovery worker can distinguish "dispatched" (pending + runId) from
    // "never dispatched" (pending + no runId) and only re-dispatch the latter.
    // Awaited (not best-effort/background): the reservation's runId is the
    // recovery discriminator for the lost-signal window, not a convenience field.
    // The conflicting-write path resolves a concurrent winner to its evidence;
    // the same-signal runId update never conflicts (runId is not part of the
    // admission identity), so a duplicate here returns the already-recorded run.
    const recordedRun = await this._writeMessageResultEvidence({
      status: 'pending',
      signalId,
      runId: handle.runId,
      admissionId: opts.admissionId,
      admissionHash: opts.expectedAdmissionHash,
    });
    if (!recordedRun.created) {
      const recordedDuplicate = this._channelSignalDispatchedDuplicate(recordedRun.evidence);
      if (recordedDuplicate !== undefined) return recordedDuplicate;
    }
    // The shared run terminal settles `handle.result` in the background (§4.2f /
    // §21); the bridge persists `runId`/`signalId` synchronously and does not
    // await the answer here. Swallow so an unawaited rejection is not unhandled.
    void handle.result.catch(() => {});
    return { runId: handle.runId, signalId: handle.id, willInterleave: handle.willInterleave };
  }

  /**
   * @internal §13.7/§14.2 step 3: resolve inbound channel provider files
   * (AttachmentRefs the bridge has already uploaded) into Harness-owned persisted
   * attachment refs scoped to this resolved session. Thin wrapper over the same
   * `_resolveAttachmentRefs` the queue/message admission paths use — validates
   * ownership + loads metadata, so the durable inbox row, admissionHash, and the
   * admitted turn all carry the SAME normalized attachments.
   */
  async _resolveChannelInboundAttachments(refs: AttachmentRef[]): Promise<PersistedAttachment[]> {
    if (refs.length === 0) return [];
    return this._resolveAttachmentRefs('channel ingress attachments', refs);
  }

  /**
   * @internal §14.2 step 7: the SIGNAL admissionHash for a channel turn, computed
   * over the exact persisted admission payload (content + persisted attachments +
   * policy-selected mode + trusted requestContext). Discriminated `kind: 'signal'`
   * (distinct from the queue path's `kind: 'queue'`) so a queue-vs-signal payload
   * never collides. Persisted on the inbox row BEFORE runtime admission so
   * recovery replays the SAME payload and never re-runs policy. Hashes the same
   * `PersistedAttachment` projection as {@link _computeQueueAdmissionHash}. Note:
   * signal delivery has no `model` field, so model is intentionally absent.
   */
  _channelSignalAdmissionHash(
    opts: { content: string; mode?: string },
    attachments: PersistedAttachment[],
    requestContext: PersistedRequestContextInput,
  ): string {
    return sha256CanonicalJson({
      kind: 'signal',
      content: opts.content,
      ...(opts.mode !== undefined ? { mode: opts.mode } : {}),
      attachments: attachments.map(attachment => ({
        kind: attachment.kind,
        name: attachment.name,
        mimeType: attachment.mimeType,
        ...(attachment.kind === 'ref'
          ? {
              attachmentId: attachment.attachmentId,
              resourceId: this.resourceId,
              ownerSessionId: attachment.ownerSessionId,
              bytes: attachment.bytes,
              sha256: attachment.sha256,
              source: attachment.source,
              attachmentKind: attachment.attachmentKind ?? 'file',
              ...(attachment.primitiveType ? { primitiveType: attachment.primitiveType } : {}),
              ...(attachment.elementType ? { elementType: attachment.elementType } : {}),
              ...(attachment.renderer ? { renderer: attachment.renderer } : {}),
              ...(attachment.schemaId ? { schemaId: attachment.schemaId } : {}),
              ...(attachment.metadata ? { metadata: cloneAttachmentMetadata(attachment.metadata) } : {}),
              ...(attachment.object ? { object: attachment.object } : {}),
            }
          : { url: attachment.url }),
      })),
      requestContext: clonePersistedRequestContext(requestContext),
    });
  }

  private async _admitQueue(
    opts: QueueOptions,
    methodName: 'queue()' | 'admitQueue()',
    internal?: {
      persistedAttachments?: PersistedAttachment[];
      persistedRequestContext?: PersistedRequestContextInput;
      expectedAdmissionHash?: string;
    },
  ): Promise<{
    queuedItemId: string;
    evidence: QueueAdmissionReceipt | OperationAdmissionTombstone;
    duplicate: boolean;
  }> {
    this._assertLive(methodName);
    this._assertOpenForTurn(methodName);
    if (typeof opts.content !== 'string' || opts.content.length === 0) {
      throw new HarnessValidationError(`${methodName}.content`, 'must be a non-empty string');
    }
    if (opts.mode !== undefined) {
      // Validates and throws on unknown id.
      this._harness._getMode(opts.mode);
    }
    if (opts.admissionId !== undefined && opts.admissionId.length === 0) {
      throw new HarnessValidationError(`${methodName}.admissionId`, 'admissionId must be a non-empty string');
    }
    this._validateQueueSchedulingOptions(opts, methodName);

    // §4.4c: validate caller request context before attachment resolution,
    // hashing, and persistence. A direct queue()/admitQueue() caller supplies
    // `app`; the channel-bridge path supplies a trusted `channel` via `internal`.
    // They are top-level siblings — combine into one persisted DTO, never deep
    // merge. (No path supplies both today; channel ingress has no SDK caller.)
    const callerRequestContext = validateCallerRequestContext(opts.requestContext, methodName);
    const callerPersistedRequestContext = callerRequestContextToPersisted(callerRequestContext);
    const effectivePersistedRequestContext: PersistedRequestContextInput | undefined =
      internal?.persistedRequestContext !== undefined || callerPersistedRequestContext !== undefined
        ? { ...(internal?.persistedRequestContext ?? {}), ...(callerPersistedRequestContext ?? {}) }
        : undefined;

    const attachments =
      internal?.persistedAttachments ??
      (await this._resolveAttachmentRefs(`${methodName}.attachments`, opts.attachments ?? []));
    if (internal?.persistedAttachments) {
      this._validatePersistedAttachments(`${methodName}.attachments`, attachments);
    }
    const effectiveModeId = opts.mode ?? this._record.modeId;
    const admissionId = opts.admissionId ?? `queue-${randomUUID()}`;
    const admissionHash = this._computeQueueAdmissionHash(opts, attachments, effectivePersistedRequestContext);
    if (internal?.expectedAdmissionHash !== undefined && internal.expectedAdmissionHash !== admissionHash) {
      throw new HarnessAdmissionConflictError(this.id, admissionId, internal.expectedAdmissionHash, admissionHash);
    }
    const duplicate = opts.admissionId
      ? await this._resolveQueueAdmissionDuplicate({ admissionId, admissionHash })
      : undefined;
    if (duplicate) {
      this._assertOpenForTurn(methodName);
      const queuedItemId = duplicate.queuedItemId;
      if (queuedItemId === undefined) {
        throw new HarnessValidationError(`${methodName}.admissionId`, 'duplicate queue result evidence has expired');
      }
      return { queuedItemId, evidence: duplicate, duplicate: true };
    }

    const cap = this._harness._internalMaxQueueDepth;
    const queueBackpressure = this._harness._internalQueueBackpressure;
    if (queueBackpressure === 'reject' && (this._record.pendingQueue?.length ?? 0) >= cap) {
      throw new HarnessQueueFullError(this.id, cap, this._record.pendingQueue?.length ?? 0);
    }
    const queuedItemId = this._queueAdmissionQueuedItemId(admissionId);
    const item: QueuedItem = {
      id: queuedItemId,
      admissionId,
      admissionHash,
      enqueuedAt: Date.now(),
      content: opts.content,
      attachments,
      ...(effectivePersistedRequestContext
        ? { requestContext: clonePersistedRequestContext(effectivePersistedRequestContext) }
        : {}),
      ...(opts.model !== undefined ? { model: opts.model } : {}),
      mode: effectiveModeId,
      ...(opts.yolo !== undefined ? { yolo: opts.yolo } : {}),
      ...(opts.priority !== undefined ? { priority: opts.priority } : {}),
      ...(opts.deadline !== undefined ? { deadline: opts.deadline } : {}),
      ...(opts.notBefore !== undefined ? { notBefore: opts.notBefore } : {}),
    };
    const attachmentReferences = attachments
      .filter((attachment): attachment is Extract<PersistedAttachment, { kind: 'ref' }> => attachment.kind === 'ref')
      .map(attachment => ({
        harnessName: this._record.harnessName,
        sessionId: attachment.ownerSessionId,
        attachmentId: attachment.attachmentId,
        source: 'queued_item' as const,
        sourceId: item.id,
      }));
    let admittedReceipt: QueueAdmissionReceipt | undefined;
    const droppedItems: QueueBackpressureDrop[] = [];
    const receipt: QueueAdmissionReceipt = {
      admissionId,
      admissionHash,
      queuedItemId: item.id,
      modeId: effectiveModeId,
      runtimeDependencies: this._harness._runtimeDependenciesForMode(
        effectiveModeId,
        item.model ?? this._record.modelId,
      ),
      status: 'queued',
      attempts: 0,
      enqueuedAt: item.enqueuedAt,
      updatedAt: item.enqueuedAt,
    };

    try {
      // Atomic check + append: re-check capacity inside the updater so a
      // concurrent in-process `queue()` cannot push us past the cap. Exact
      // admission retries are resolved here too so they do not append or
      // consume queue capacity even when racing the original admission.
      await this._flushUpdate(
        prev => {
          for (const existing of Object.values(prev.queueAdmissionReceipts ?? {})) {
            if (existing.admissionId !== admissionId) continue;
            if (existing.admissionHash !== admissionHash) {
              throw new HarnessAdmissionConflictError(this.id, admissionId, existing.admissionHash, admissionHash);
            }
            admittedReceipt = existing;
            return prev;
          }
          if (prev.closingAt !== undefined || this.isClosing) {
            throw harnessSessionClosingError(this);
          }
          if (prev.cancelRequest !== undefined) {
            throw new HarnessSessionCancelledError(this.id, prev.cancelRequest.reason);
          }
          if (queueBackpressure === 'reject' && (prev.pendingQueue?.length ?? 0) >= cap) {
            throw new HarnessQueueFullError(this.id, cap, prev.pendingQueue?.length ?? 0);
          }
          return this._applyQueueBackpressureForAppend(prev, {
            item,
            receipt,
            maxQueueDepth: cap,
            source: 'queue',
            droppedItems,
          });
        },
        { attachmentReferences },
      );
    } catch (err) {
      if (isStorageAttachmentUnavailableError(err)) {
        throw new HarnessAttachmentUnavailableError(err.sessionId, 'not_found', err.attachmentId);
      }
      throw err;
    }

    if (admittedReceipt) {
      this._assertOpenForTurn(methodName);
      return { queuedItemId: admittedReceipt.queuedItemId, evidence: admittedReceipt, duplicate: true };
    }

    this._failBackpressureDroppedItems(droppedItems);
    return { queuedItemId: item.id, evidence: receipt, duplicate: false };
  }

  /**
   * @internal
   * Worker-only admission for durable wakeups. Wakeup rows already carry
   * persisted attachment/request-context records, so this path must not
   * reinterpret them as caller-provided attachment refs or route request
   * context through public queue options.
   */
  async _admitWakeupQueue(item: {
    content: string;
    admissionId: string;
    admissionHash?: string;
    mode?: string;
    model?: string;
    yolo?: boolean;
    attachments: PersistedAttachment[];
    requestContext?: PersistedRequestContextInput;
  }): Promise<QueueAdmissionResult> {
    const admission = await this._admitQueue(
      {
        content: item.content,
        admissionId: item.admissionId,
        ...(item.mode !== undefined ? { mode: item.mode } : {}),
        ...(item.model !== undefined ? { model: item.model } : {}),
        ...(item.yolo === true ? { yolo: true } : {}),
      },
      'admitQueue()',
      {
        persistedAttachments: item.attachments.map(clonePersistedAttachment),
        ...(item.requestContext ? { persistedRequestContext: clonePersistedRequestContext(item.requestContext) } : {}),
        ...(item.admissionHash ? { expectedAdmissionHash: item.admissionHash } : {}),
      },
    );
    if (!admission.duplicate) {
      this._liveAdmittedQueuedItemIds.add(admission.queuedItemId);
    }
    void this._maybeDrainQueue();
    return { accepted: true, queuedItemId: admission.queuedItemId, duplicate: admission.duplicate };
  }

  private async _resolveQueueAdmissionDuplicate({
    admissionId,
    admissionHash,
  }: {
    admissionId: string;
    admissionHash: string;
  }): Promise<QueueAdmissionReceipt | OperationAdmissionTombstone | undefined> {
    const resolved = await this._storage.resolveOperationAdmissionEvidence({
      harnessName: this._record.harnessName,
      sessionId: this.id,
      resourceId: this.resourceId,
      threadId: this.threadId,
      kind: 'queue',
      admissionId,
      attemptedAdmissionHash: admissionHash,
    });
    if (resolved.status === 'none') return undefined;
    if (resolved.status === 'conflict') {
      throw new HarnessAdmissionConflictError(this.id, admissionId, resolved.storedAdmissionHash ?? '', admissionHash);
    }
    return resolved.evidence as QueueAdmissionReceipt | OperationAdmissionTombstone | undefined;
  }

  private async _returnDuplicateQueueResult(
    evidence: QueueAdmissionReceipt | OperationAdmissionTombstone,
    activeDeleted?: Promise<never>,
  ): Promise<AgentResult> {
    if ('kind' in evidence) {
      throw new HarnessValidationError('queue().admissionId', 'duplicate queue result evidence has expired');
    }
    if (evidence.status === 'completed' && evidence.postRunFinalizedAt !== undefined) {
      return evidence.result as AgentResult;
    }
    if (evidence.status === 'failed' || evidence.status === 'admission_failed') {
      throw publicErrorProjectionToError(
        evidence.error ?? { code: 'harness.queue_failed', message: 'queued turn failed' },
      );
    }
    if (evidence.status === 'dead') {
      throw publicErrorProjectionToError(
        evidence.error ?? { code: 'harness.queue_exhausted', message: 'queued turn exhausted retry attempts' },
      );
    }
    const resolver = this._queueResolvers.get(evidence.queuedItemId);
    if (resolver) return this._raceActiveTurnWaiter(resolver.promise, activeDeleted);
    void this._maybeDrainQueue();
    return this._awaitDurableQueueResult(evidence, activeDeleted);
  }

  private async _awaitDurableQueueResult(
    receipt: QueueAdmissionReceipt,
    activeDeleted?: Promise<never>,
  ): Promise<AgentResult> {
    const deadline = Date.now() + MESSAGE_ADMISSION_DURABLE_WAIT_TIMEOUT_MS;
    while (true) {
      this._assertNotDeleted();
      const latest = await this._raceActiveTurnWaiter(
        this._storage.loadQueueResultEvidence({
          harnessName: this._record.harnessName,
          sessionId: this.id,
          resourceId: this.resourceId,
          queuedItemId: receipt.queuedItemId,
        }),
        activeDeleted,
      );
      this._assertNotDeleted();
      if (!latest) {
        throw new HarnessValidationError('queue().admissionId', 'duplicate queue result evidence has expired');
      }
      if ('kind' in latest) {
        throw new HarnessValidationError('queue().admissionId', 'duplicate queue result evidence has expired');
      }
      if (latest.status === 'completed' && latest.postRunFinalizedAt !== undefined) return latest.result as AgentResult;
      if (latest.status === 'failed' || latest.status === 'admission_failed') {
        throw publicErrorProjectionToError(
          latest.error ?? { code: 'harness.queue_failed', message: 'queued turn failed' },
        );
      }
      if (latest.status === 'dead') {
        throw publicErrorProjectionToError(
          latest.error ?? { code: 'harness.queue_exhausted', message: 'queued turn exhausted retry attempts' },
        );
      }
      const waitMs = Math.min(MESSAGE_ADMISSION_DURABLE_WAIT_INTERVAL_MS, Math.max(0, deadline - Date.now()));
      if (waitMs === 0) {
        throw new HarnessValidationError('queue().admissionId', 'duplicate queue result evidence has expired');
      }
      await this._raceActiveTurnWaiter(delay(waitMs), activeDeleted);
    }
  }

  private _queueAdmissionQueuedItemId(admissionId: string): string {
    const digest = sha256CanonicalJson({
      kind: 'queue-admission',
      harnessName: this._record.harnessName,
      sessionId: this.id,
      resourceId: this.resourceId,
      threadId: this.threadId,
      admissionId,
    });
    return `q-${digest.slice(0, 32)}`;
  }

  private _queueSignalIdentity(item: QueuedItem): MessageAdmissionIdentity {
    const digest = sha256CanonicalJson({
      kind: 'queue-signal',
      harnessName: this._record.harnessName,
      sessionId: this.id,
      resourceId: this.resourceId,
      threadId: this.threadId,
      queuedItemId: item.id,
      admissionId: item.admissionId,
    });
    return {
      signalId: `harness-queue-${digest.slice(0, 32)}`,
      runId: `harness-queue-${digest.slice(32, 64)}`,
    };
  }

  private _computeQueueAdmissionHash(
    opts: QueueOptions,
    attachments: PersistedAttachment[],
    requestContext?: PersistedRequestContextInput,
  ): string {
    return sha256CanonicalJson({
      kind: 'queue',
      content: opts.content,
      ...(opts.mode !== undefined ? { mode: opts.mode } : {}),
      ...(opts.model !== undefined ? { model: opts.model } : {}),
      ...(opts.yolo === true ? { yolo: true } : {}),
      ...(opts.priority !== undefined && opts.priority !== 0 ? { priority: opts.priority } : {}),
      ...(opts.deadline !== undefined ? { deadline: opts.deadline } : {}),
      ...(opts.notBefore !== undefined ? { notBefore: opts.notBefore } : {}),
      attachments: attachments.map(attachment => ({
        kind: attachment.kind,
        name: attachment.name,
        mimeType: attachment.mimeType,
        ...(attachment.kind === 'ref'
          ? {
              attachmentId: attachment.attachmentId,
              resourceId: this.resourceId,
              ownerSessionId: attachment.ownerSessionId,
              bytes: attachment.bytes,
              sha256: attachment.sha256,
              source: attachment.source,
              attachmentKind: attachment.attachmentKind ?? 'file',
              ...(attachment.primitiveType ? { primitiveType: attachment.primitiveType } : {}),
              ...(attachment.elementType ? { elementType: attachment.elementType } : {}),
              ...(attachment.renderer ? { renderer: attachment.renderer } : {}),
              ...(attachment.schemaId ? { schemaId: attachment.schemaId } : {}),
              ...(attachment.metadata ? { metadata: cloneAttachmentMetadata(attachment.metadata) } : {}),
              ...(attachment.object ? { object: attachment.object } : {}),
            }
          : { url: attachment.url }),
      })),
      ...(requestContext ? { requestContext: clonePersistedRequestContext(requestContext) } : {}),
    });
  }

  private _validateQueueSchedulingOptions(opts: QueueOptions, methodName: 'queue()' | 'admitQueue()'): void {
    for (const field of ['priority', 'deadline', 'notBefore'] as const) {
      const value = opts[field];
      if (value !== undefined && (!Number.isFinite(value) || Object.is(value, -0))) {
        throw new HarnessValidationError(`${methodName}.${field}`, 'must be a finite JSON number other than -0');
      }
    }
    if (opts.notBefore !== undefined && opts.deadline !== undefined && opts.notBefore > opts.deadline) {
      throw new HarnessValidationError(
        `${methodName}.notBefore`,
        '`notBefore` must be less than or equal to `deadline`',
      );
    }
  }

  private _applyQueueBackpressureForAppend(
    prev: SessionRecord,
    opts: {
      item: QueuedItem;
      receipt?: QueueAdmissionReceipt;
      maxQueueDepth: number;
      source: 'queue' | 'goal';
      goalId?: string;
      droppedItems: QueueBackpressureDrop[];
      onRejected?: () => void;
      onAdmitted?: () => void;
    },
  ): SessionRecord {
    const policy = this._harness._internalQueueBackpressure;
    const activeId = this._currentQueuedItemId ?? prev.pendingResume?.queuedItemId;
    const queue = [...(prev.pendingQueue ?? [])];
    const existingReceipts = prev.queueAdmissionReceipts ?? {};
    let nextReceipts: Record<string, QueueAdmissionReceipt> | undefined;
    const now = opts.item.enqueuedAt;

    while (queue.length >= opts.maxQueueDepth) {
      if (policy === 'reject') {
        opts.onRejected?.();
        return prev;
      }

      let dropIdx = -1;
      let dropEnqueuedAt = Number.POSITIVE_INFINITY;
      for (let i = 0; i < queue.length; i += 1) {
        const candidate = queue[i]!;
        if (activeId !== undefined && candidate.id === activeId) continue;
        const receipt = existingReceipts[candidate.id];
        if (receipt !== undefined && receipt.status !== 'queued') continue;
        if (candidate.enqueuedAt < dropEnqueuedAt) {
          dropIdx = i;
          dropEnqueuedAt = candidate.enqueuedAt;
        }
      }
      if (dropIdx < 0) {
        if (opts.source === 'goal') {
          opts.onRejected?.();
          return prev;
        }
        throw new HarnessQueueFullError(this.id, opts.maxQueueDepth, queue.length);
      }

      const [dropped] = queue.splice(dropIdx, 1);
      if (!dropped) continue;
      opts.droppedItems.push({
        queuedItemId: dropped.id,
        ...(dropped.admissionId !== undefined ? { admissionId: dropped.admissionId } : {}),
        replacementQueuedItemId: opts.item.id,
        ...(opts.item.admissionId !== undefined ? { replacementAdmissionId: opts.item.admissionId } : {}),
        maxQueueDepth: opts.maxQueueDepth,
        source: opts.source,
        ...(opts.goalId !== undefined ? { goalId: opts.goalId } : {}),
      });

      const receipt = existingReceipts[dropped.id];
      if (receipt && !this._isTerminalQueueReceipt(receipt)) {
        nextReceipts ??= { ...existingReceipts };
        const dropError = new HarnessQueueFullDroppedError(dropped.id);
        nextReceipts[dropped.id] = {
          ...receipt,
          status: 'failed',
          error: projectHarnessPublicError(dropError),
          failedAt: receipt.failedAt ?? now,
          updatedAt: now,
        };
      }
    }

    opts.onAdmitted?.();
    const next: SessionRecord = {
      ...prev,
      pendingQueue: [...queue, opts.item],
    };
    if (opts.receipt !== undefined) {
      next.queueAdmissionReceipts = {
        ...(nextReceipts ?? existingReceipts),
        [opts.item.id]: opts.receipt,
      };
    } else if (nextReceipts !== undefined) {
      next.queueAdmissionReceipts = nextReceipts;
    }
    return next;
  }

  // §10.2: backpressure drops emit no public event — the observable effect is
  // the rejected drop-oldest operation promises below. (The reject-policy path
  // throws HarnessQueueFullError at the caller and admits nothing.)
  private _failBackpressureDroppedItems(droppedItems: QueueBackpressureDrop[]): void {
    for (const dropped of droppedItems) {
      const resolver = this._queueResolvers.get(dropped.queuedItemId);
      if (resolver) {
        this._queueResolvers.delete(dropped.queuedItemId);
        resolver.reject(new HarnessQueueFullDroppedError(dropped.queuedItemId));
      }
    }
  }

  private _isTerminalQueueReceipt(receipt: QueueAdmissionReceipt): boolean {
    return receipt.status === 'completed' || receipt.status === 'failed' || receipt.status === 'dead';
  }

  private async _updateQueueAdmissionReceipt(
    queuedItemId: string,
    update: (receipt: QueueAdmissionReceipt, now: number) => QueueAdmissionReceipt,
  ): Promise<void> {
    await this._flushUpdate(prev => {
      const current = prev.queueAdmissionReceipts?.[queuedItemId];
      if (!current) return prev;
      const now = Date.now();
      return {
        ...prev,
        queueAdmissionReceipts: {
          ...(prev.queueAdmissionReceipts ?? {}),
          [queuedItemId]: update(current, now),
        },
      };
    });
  }

  private async _resolveAttachmentRefs(field: string, refs: AttachmentRef[]): Promise<PersistedAttachment[]> {
    const attachments: PersistedAttachment[] = [];
    for (let i = 0; i < refs.length; i += 1) {
      const ref = refs[i]!;
      const ownerSessionId = ref.ownerSessionId ?? this.id;
      if (ownerSessionId !== this.id) {
        throw new HarnessValidationError(`${field}[${i}].ownerSessionId`, 'attachment must belong to this session');
      }
      const record = await this._storage.getAttachmentRecord({
        harnessName: this._record.harnessName,
        sessionId: this.id,
        attachmentId: ref.attachmentId,
      });
      if (!record) {
        throw new HarnessAttachmentUnavailableError(this.id, 'not_found', ref.attachmentId);
      }
      if (ref.bytes !== undefined && ref.bytes !== record.bytes) {
        throw new HarnessValidationError(`${field}[${i}].bytes`, 'attachment byte count does not match storage');
      }
      if (ref.sha256 !== undefined && ref.sha256 !== record.sha256) {
        throw new HarnessValidationError(`${field}[${i}].sha256`, 'attachment digest does not match storage');
      }
      attachments.push({
        kind: 'ref',
        name: record.name,
        mimeType: record.mimeType,
        ownerSessionId: record.ownerSessionId,
        attachmentId: record.attachmentId,
        bytes: record.bytes,
        sha256: record.sha256,
        source: record.source,
        attachmentKind: record.kind ?? 'file',
        ...(record.primitiveType ? { primitiveType: record.primitiveType } : {}),
        ...(record.elementType ? { elementType: record.elementType } : {}),
        ...(record.renderer ? { renderer: { ...record.renderer } } : {}),
        ...(record.schemaId ? { schemaId: record.schemaId } : {}),
        ...(record.metadata ? { metadata: cloneAttachmentMetadata(record.metadata) } : {}),
        ...(record.object ? { object: { ...record.object } } : {}),
      });
    }
    return attachments;
  }

  private _validatePersistedAttachments(field: string, attachments: PersistedAttachment[]): void {
    for (let i = 0; i < attachments.length; i += 1) {
      const attachment = attachments[i]!;
      if (attachment.kind !== 'ref') continue;
      if (attachment.ownerSessionId !== this.id) {
        throw new HarnessValidationError(`${field}[${i}].ownerSessionId`, 'attachment must belong to this session');
      }
    }
  }

  /**
   * Scheduler step. Inside a single `_flushUpdate` CAS:
   *   - drop any items whose `deadline` has passed; emit
   *     `queue_item_expired` per drop, mark each item's receipt
   *     `failed`, and reject the resolver after the commit;
   *   - select the next item to run by `(priority desc, enqueuedAt
   *     asc)` from those that have no `notBefore` block; rotate it
   *     to position 0 so the existing drain + recovery logic can keep
   *     its `pendingQueue[0]` invariant.
   *
   * No-op when the queue is empty or no item is currently eligible.
   */
  private async _scheduleNextQueueHead(): Promise<boolean> {
    const now = Date.now();
    const expired: { queuedItemId: string; admissionId?: string; deadline: number }[] = [];
    let didCommit = false;
    let hasRunnableHead = false;

    await this._flushUpdate(prev => {
      const queue = prev.pendingQueue ?? [];
      if (queue.length === 0) return prev;

      const survivors: QueuedItem[] = [];
      const existingReceipts = prev.queueAdmissionReceipts ?? {};
      const nextReceipts: Record<string, QueueAdmissionReceipt> = { ...existingReceipts };
      let receiptsChanged = false;

      for (const item of queue) {
        const receipt = existingReceipts[item.id];
        if (receipt?.status === 'completed') {
          survivors.push(item);
          continue;
        }
        if (item.deadline !== undefined && item.deadline <= now) {
          expired.push({ queuedItemId: item.id, admissionId: item.admissionId, deadline: item.deadline });
          if (receipt && receipt.status !== 'failed' && receipt.status !== 'dead') {
            const expiryError = new HarnessQueueItemExpiredError(this.id, item.id, item.deadline);
            nextReceipts[item.id] = {
              ...receipt,
              status: 'failed',
              error: projectHarnessPublicError(expiryError),
              failedAt: receipt.failedAt ?? now,
              updatedAt: now,
            };
            receiptsChanged = true;
          }
          continue;
        }
        survivors.push(item);
      }

      // Pick the next runnable item: highest priority, FIFO tie-break.
      // The currently-running item (if any) stays first — it's already
      // executing and the drain only consults position 0 mid-loop when
      // no turn is in flight.
      let head: QueuedItem | undefined;
      let headIdx = -1;
      for (let i = 0; i < survivors.length; i++) {
        const candidate = survivors[i]!;
        if (candidate.notBefore !== undefined && candidate.notBefore > now) continue;
        const cp = candidate.priority ?? 0;
        const hp = head?.priority ?? 0;
        if (head === undefined || cp > hp || (cp === hp && candidate.enqueuedAt < head.enqueuedAt)) {
          head = candidate;
          headIdx = i;
        }
      }
      hasRunnableHead = head !== undefined;

      const rotated = head !== undefined && headIdx > 0;
      const didExpire = expired.length > 0;
      // No-op short-circuit: nothing expired, no rotation needed, no
      // receipts to update. `survivors` is always a fresh array, so an
      // identity check vs `queue` would never short-circuit — we have
      // to derive "nothing changed" from the flags instead.
      if (!didExpire && !rotated && !receiptsChanged) return prev;

      const nextQueue = rotated
        ? // Rotate selected item to position 0 so the existing
          // recovery + drain code can keep its `pendingQueue[0]`
          // assumption while still running the highest-priority
          // work first.
          [head!, ...survivors.slice(0, headIdx), ...survivors.slice(headIdx + 1)]
        : survivors;

      didCommit = true;
      const next: SessionRecord = { ...prev, pendingQueue: nextQueue };
      if (receiptsChanged) {
        next.queueAdmissionReceipts = nextReceipts;
      }
      return next;
    });

    if (!didCommit) return hasRunnableHead;

    for (const dropped of expired) {
      // §10.2: no queue_item_expired event — the observable effect is the
      // rejected operation promise (HarnessQueueItemExpiredError) below.
      const resolver = this._queueResolvers.get(dropped.queuedItemId);
      if (resolver) {
        this._queueResolvers.delete(dropped.queuedItemId);
        resolver.reject(new HarnessQueueItemExpiredError(this.id, dropped.queuedItemId, dropped.deadline));
      }
    }
    return hasRunnableHead;
  }

  /**
   * Drain pending queue items head-of-line. No-op while another drain is
   * running, the session is suspended (`pendingResume` set), or the queue
   * is empty. Each item runs as a fresh turn; if the turn suspends, drain
   * exits early and resumes from `_resume()` once the user responds.
   */
  private async _maybeDrainQueue(): Promise<void> {
    if (this._draining) return;
    if (!this._canDrainQueue()) return;
    // A live suspension means a previous queued turn is awaiting a
    // `respondTo*` call — drain stays parked until that resolves.
    if (this._record.pendingResume !== undefined) {
      const recovery = await this._maybeRecoverStaleQueuedResume();
      if (recovery.status === 'none') return;
    }
    if (this._currentQueuedItemId !== undefined) return;
    // A manual `message()` turn is in flight — wait for it to settle.
    // `_recordTurnCompletion` will re-kick the drain on its way out.
    if (this._currentTurnAbortController !== undefined) return;

    this._draining = true;
    try {
      while (this._canDrainQueue() && (this._record.pendingQueue?.length ?? 0) > 0) {
        // Bail if a previous iteration left the session suspended.
        if (this._record.pendingResume !== undefined) return;

        // Scheduler step: expire any items past their deadline, then
        // rotate the highest-priority item to the head. Done in a
        // single CAS so the rest of the drain logic (recovery,
        // post-run finalize) can keep its `pendingQueue[0]` assumption.
        const hasRunnableHead = await this._scheduleNextQueueHead();
        if (!hasRunnableHead) {
          this._scheduleQueueWakeupForPendingQueue();
          return;
        }
        this._clearQueueWakeTimer();
        const head = this._record.pendingQueue?.[0];
        if (!head) return;
        this._currentQueuedItemId = head.id;
        this._currentQueuedItemSource = head.source ?? 'user';
        // §10.2: no queue_item_started / queue_item_replayed events — the closed
        // union has no queue-lifecycle family; the drained turn's `agent_start`
        // marks the boundary. We still clear the live-admission marker (only
        // when no live resolver is tracking the item, mirroring the prior
        // short-circuit) so `_liveAdmittedQueuedItemIds` does not leak.
        if (!this._queueResolvers.has(head.id)) {
          this._liveAdmittedQueuedItemIds.delete(head.id);
        }

        let suspended = false;
        try {
          const full = await this._runQueuedTurn(head);
          suspended = full.finishReason === 'suspended';
          if (!suspended) {
            await this._completeQueuedTurn(head.id, full as AgentResult);
          }
        } catch (err) {
          if (err instanceof QueueRecoveryPendingError) {
            this._parkQueuedTurn(head.id, err);
            return;
          }
          if (err instanceof QueuePostRunFinalizationPendingError) {
            this._deferQueuedTurnRetry(err);
            return;
          }
          // Permanent failure during the turn — reject the resolver and
          // remove the item so we don't replay it forever.
          await this._failQueuedTurn(head.id, err);
        }

        if (suspended) {
          // Stop draining; `_resume()` will re-kick when the user responds.
          return;
        }
      }
    } finally {
      this._draining = false;
      this._notifyMaybeIdle();
    }
  }

  /**
   * Run a single queued item as a turn. Mirrors `message()`'s default path
   * but pulls overrides off the queued item rather than per-call options.
   * Returns the `FullOutput` so the drain loop can decide whether the head
   * stays in place (suspended) or is removed (complete / error).
   */
  private async _runQueuedTurn(item: QueuedItem): Promise<FullOutput<unknown>> {
    const currentReceipt = this._record.queueAdmissionReceipts?.[item.id];
    const effectiveModeId = currentReceipt?.modeId ?? item.mode ?? this._record.modeId;
    if (currentReceipt?.status === 'completed') {
      const full = currentReceipt.result as FullOutput<unknown>;
      if (currentReceipt.postRunFinalizedAt === undefined) {
        await this._finalizeCompletedQueuedTurn(item, full, effectiveModeId);
      }
      return full;
    }

    this._assertOpenForTurn('queue drain');
    await this._validateQueuedAttachmentRefs(item);
    const identity = this._queueSignalIdentity(item);
    let shouldMarkAdmitting = true;
    if (currentReceipt) {
      if (currentReceipt.status === 'failed' || currentReceipt.status === 'admission_failed') {
        throw publicErrorProjectionToError(
          currentReceipt.error ?? { code: 'harness.queue_failed', message: 'queued turn failed' },
        );
      }
      if (currentReceipt.status === 'dead') {
        throw publicErrorProjectionToError(
          currentReceipt.error ?? {
            code: 'harness.queue_exhausted',
            message: 'queued turn exhausted retry attempts',
          },
        );
      }
    }
    if (
      currentReceipt &&
      (currentReceipt.status === 'admitting' || currentReceipt.status === 'accepted') &&
      currentReceipt.runId &&
      currentReceipt.signalId
    ) {
      const recoveredTerminal = await this._withActiveDeletedWaiter(activeDeleted =>
        this._recoverQueuedTerminalEvidence(item, currentReceipt, effectiveModeId, activeDeleted),
      );
      if (recoveredTerminal) return recoveredTerminal;
    }
    const runtimeDependencies = this._runtimeDependenciesForQueuedTurn(item, currentReceipt, effectiveModeId);
    const { mode, agent } = this._harness._resolveAgentForRuntimeDependencies(
      runtimeDependencies,
      `queued item "${item.id}" recovery`,
    );
    if (
      currentReceipt &&
      (currentReceipt.status === 'admitting' || currentReceipt.status === 'accepted') &&
      currentReceipt.runId &&
      currentReceipt.signalId
    ) {
      const recovered = await this._recoverQueuedDispatch(item, currentReceipt, agent, effectiveModeId);
      if (recovered) return recovered;
    }
    if (shouldMarkAdmitting) {
      await this._updateQueueAdmissionReceipt(item.id, (receipt, now) => ({
        ...receipt,
        status: 'admitting',
        runId: identity.runId,
        signalId: identity.signalId,
        modeId: receipt.modeId ?? effectiveModeId,
        attempts: receipt.attempts + 1,
        admittingAt: receipt.admittingAt ?? now,
        updatedAt: now,
      }));
    }

    const toolsets = this._buildToolsets(mode);
    // Queued turns run under a session-owned AbortController so
    // `session.abort()` can cancel an in-flight queued run too.
    this._assertOpenForTurn('queue drain');
    const turnAbortController = this._beginTurn(undefined, {
      modeId: effectiveModeId,
      modelId: item.model ?? this._record.modelId,
    });
    const activeTurnWaiter = this._createActiveTurnWaiter();
    void activeTurnWaiter.promise.catch(() => {});
    const finishQueuedTurn = () => {
      activeTurnWaiter.cleanup();
      this._endTurn(turnAbortController);
    };
    const assertQueuedTurnNotDeleted = () => {
      if (this._state === 'deleted') {
        throw new HarnessSessionDeletedError(this.id, this._record.resourceId, this._record.threadId);
      }
    };
    let agentStarted = false;
    let agentEndEmitted = false;
    let fallbackRunId = identity.runId;

    try {
      const requestContext = await Promise.race([
        this._buildRequestContext({
          modeId: effectiveModeId,
          modelId: this._modelIdForQueuedItem(item.id),
          abortSignal: turnAbortController.signal,
          persistedRequestContext: item.requestContext,
        }),
        activeTurnWaiter.promise,
      ]);
      assertQueuedTurnNotDeleted();
      const baseExecOptions: AgentExecutionOptionsBase<unknown> = {
        memory: { thread: this.threadId, resource: this.resourceId },
        abortSignal: turnAbortController.signal,
        requestContext,
        ...(toolsets ? { toolsets } : {}),
        ...(mode.instructions ? { instructions: mode.instructions } : {}),
      };

      await Promise.race([this._ensureThreadSubscription(agent), activeTurnWaiter.promise]);
      assertQueuedTurnNotDeleted();
      await Promise.race([
        this._writeQueueSignalResultEvidence({
          status: 'pending',
          signalId: identity.signalId,
          runId: identity.runId,
        }),
        activeTurnWaiter.promise,
      ]);
      assertQueuedTurnNotDeleted();
      // §13.7/§14.2: forward persisted attachment bytes as model file-parts so a
      // queued (channel or direct) turn's attachments actually reach the agent,
      // not just the dedup identity. No attachments → bare `item.content`.
      const queuedContents = await Promise.race([
        this._buildSignalContentsWithAttachments(item.content, item.attachments),
        activeTurnWaiter.promise,
      ]);
      assertQueuedTurnNotDeleted();
      const signal = agent.sendSignal(
        { id: identity.signalId, type: 'user-message', contents: queuedContents as never },
        {
          runId: identity.runId,
          resourceId: this.resourceId,
          threadId: this.threadId,
          ifIdle: { behavior: 'wake', streamOptions: baseExecOptions as never },
        },
      );
      const signalIdentity =
        signal.runId === identity.runId && signal.signal.id === identity.signalId
          ? identity
          : { runId: signal.runId, signalId: signal.signal.id };
      fallbackRunId = signalIdentity.runId;
      // §10.2 agent_start carries the runId — emit it only AFTER sendSignal so
      // the run is registered and the runId matches the one chunks/completion
      // use (signalIdentity.runId may differ from the pre-dispatch identity).
      this._emitAgentStart(signalIdentity.runId);
      agentStarted = true;
      const completion = this._awaitQueuedRunCompletion(
        item,
        signalIdentity.runId,
        signalIdentity.signalId,
        effectiveModeId,
        activeTurnWaiter.promise,
        { onAgentEnd: () => (agentEndEmitted = true) },
      );
      void completion.catch(() => {});
      if (signalIdentity !== identity) {
        await Promise.race([
          this._writeQueueSignalResultEvidence({
            status: 'pending',
            signalId: signalIdentity.signalId,
            runId: signalIdentity.runId,
          }),
          activeTurnWaiter.promise,
        ]);
      }
      await Promise.race([
        this._updateQueueAdmissionReceipt(item.id, (receipt, now) => ({
          ...receipt,
          status: 'accepted',
          runId: signalIdentity.runId,
          signalId: signalIdentity.signalId,
          modeId: receipt.modeId ?? effectiveModeId,
          acceptedAt: receipt.acceptedAt ?? now,
          updatedAt: now,
        })).catch(() => {}),
        activeTurnWaiter.promise,
      ]);
      const full = await Promise.race([completion, activeTurnWaiter.promise]);
      agentEndEmitted = true;
      return full;
    } catch (err) {
      if (agentStarted && !agentEndEmitted && !(err instanceof QueuePostRunFinalizationPendingError)) {
        this._emitTurnEvent({
          type: 'agent_end',
          finishReason: turnAbortController.signal.aborted ? 'aborted' : 'error',
          runId: fallbackRunId,
          usage: this._runUsage(),
          queuedItemId: item.id,
        });
      }
      throw err;
    } finally {
      finishQueuedTurn();
    }
  }

  private _runtimeDependenciesForQueuedTurn(
    item: QueuedItem,
    receipt: QueueAdmissionReceipt | undefined,
    modeId: string,
  ): HarnessRuntimeDependencyRefs {
    const modelId = item.model ?? this._record.modelId;
    return receipt?.runtimeDependencies ?? { modeId, ...(modelId ? { modelId } : {}) };
  }

  private async _recoverQueuedDispatch(
    item: QueuedItem,
    receipt: QueueAdmissionReceipt,
    agent: Agent,
    modeId: string,
  ): Promise<FullOutput<unknown> | undefined> {
    if (!receipt.runId || !receipt.signalId) return undefined;

    return this._withActiveDeletedWaiter(async activeDeleted => {
      await this._raceActiveTurnWaiter(this._ensureThreadSubscription(agent), activeDeleted);
      if (this._hasLiveMessageRun(agent, receipt.runId!)) {
        return this._awaitQueuedRunCompletion(item, receipt.runId!, receipt.signalId!, modeId, activeDeleted);
      }

      const terminalEvidence = await this._recoverQueuedTerminalEvidence(item, receipt, modeId, activeDeleted);
      if (terminalEvidence) return terminalEvidence;

      const recovery = await this._raceActiveTurnWaiter(this._inspectQueueReceiptMemory(receipt), activeDeleted);
      if (recovery.status === 'pending') {
        const dispatchAt = receipt.acceptedAt ?? receipt.admittingAt ?? receipt.updatedAt;
        const retryAt = dispatchAt + QUEUE_ACCEPTED_RECOVERY_STALE_MS;
        if (Date.now() >= retryAt) throw new QueueRecoveryStaleError();
        throw new QueueRecoveryPendingError(retryAt);
      }

      return undefined;
    });
  }

  private async _recoverQueuedTerminalEvidence(
    item: QueuedItem,
    receipt: QueueAdmissionReceipt,
    modeId: string,
    activeTurnWaiter?: Promise<never>,
  ): Promise<FullOutput<unknown> | undefined> {
    if (!receipt.runId || !receipt.signalId) return undefined;

    const evidence = await this._raceActiveTurnWaiter(this._loadQueueSignalResultEvidence(receipt), activeTurnWaiter);
    if (evidence.status === 'completed') {
      const full = evidence.result as FullOutput<unknown>;
      await this._raceActiveTurnWaiter(this._markQueuedTurnCompleted(item.id, full), activeTurnWaiter);
      if (receipt.postRunFinalizedAt === undefined) {
        await this._finalizeCompletedQueuedTurn(item, full, modeId, activeTurnWaiter);
      }
      return full;
    }
    if (evidence.status === 'failed') {
      throw publicErrorProjectionToError(
        evidence.error ?? { code: 'harness.queue_failed', message: 'queued turn failed' },
      );
    }

    return undefined;
  }

  private _queueReceiptTerminalFailureError(queuedItemId: string): Error | undefined {
    return this._queueReceiptTerminalFailureErrorFromReceipt(this._record.queueAdmissionReceipts?.[queuedItemId]);
  }

  private _queueReceiptTerminalFailureErrorFromReceipt(receipt: QueueAdmissionReceipt | undefined): Error | undefined {
    if (
      !receipt ||
      (receipt.status !== 'failed' && receipt.status !== 'dead' && receipt.status !== 'admission_failed')
    ) {
      return undefined;
    }
    return publicErrorProjectionToError(
      receipt.error ?? {
        code: receipt.status === 'dead' ? 'harness.queue_exhausted' : 'harness.queue_failed',
        message: receipt.status === 'dead' ? 'queued turn exhausted retry attempts' : 'queued turn failed',
      },
    );
  }

  private async _awaitQueuedRunCompletion(
    item: QueuedItem,
    runId: string,
    signalId: string,
    modeId: string,
    activeTurnWaiter?: Promise<never>,
    opts: { onAgentEnd?: () => void } = {},
  ): Promise<FullOutput<unknown>> {
    let full: FullOutput<unknown>;
    try {
      full = await this._raceActiveTurnWaiter(this._awaitRunCompletion(runId), activeTurnWaiter);
    } catch (err) {
      if (this._shouldWriteTurnFailureEvidence(err)) {
        await this._raceActiveTurnWaiter(
          this._writeQueueSignalResultEvidence({
            status: 'failed',
            signalId,
            runId,
            error: projectHarnessPublicError(err),
          }).catch(() => {}),
          activeTurnWaiter,
        );
      }
      throw err;
    }

    const terminalFailure = this._queueReceiptTerminalFailureError(item.id);
    if (terminalFailure) throw terminalFailure;

    if (full.finishReason !== 'suspended') {
      await this._raceActiveTurnWaiter(this._markQueuedTurnCompleted(item.id, full), activeTurnWaiter);
      await this._finalizeCompletedQueuedTurn(item, full, modeId, activeTurnWaiter, opts);
      await this._raceActiveTurnWaiter(
        this._writeQueueSignalResultEvidence({
          status: 'completed',
          signalId,
          runId,
          result: full,
        }).catch(() => {}),
        activeTurnWaiter,
      );
    } else {
      await this._finalizeQueuedRunCompletion(item, full, modeId, activeTurnWaiter, opts);
    }
    return full;
  }

  private async _finalizeCompletedQueuedTurn(
    item: QueuedItem,
    full: FullOutput<unknown>,
    modeId: string,
    activeTurnWaiter?: Promise<never>,
    opts: { onAgentEnd?: () => void } = {},
  ): Promise<void> {
    let alreadyAccounted = false;
    if (this._record.queueAdmissionReceipts?.[item.id]?.postRunFinalizedAt === undefined) {
      // Account for the turn's tokens BEFORE writing the no-replay marker so
      // the marker's CAS save piggybacks the live `_tokenUsage` via the
      // `_flushUpdate` overlay. Without this ordering, a
      // crash between the marker save and a later scheduled token persist
      // would resume with `postRunFinalizedAt` set and never re-account.
      let tokenUsageDelta: TokenUsage | undefined;
      try {
        this._captureTurnRunId(full);
        tokenUsageDelta = this._tokenUsageDeltaFromFullOutput(full);
        alreadyAccounted = true;
        await this._raceActiveTurnWaiter(
          this._markQueuedPostRunFinalized(item.id, { tokenUsageDelta }),
          activeTurnWaiter,
        );
      } catch (err) {
        if (err instanceof HarnessSessionDeletedError) throw err;
        throw new QueuePostRunFinalizationPendingError(Date.now() + QUEUE_POST_RUN_FINALIZATION_RETRY_MS, err);
      }
    }
    await this._finalizeQueuedRunCompletion(item, full, modeId, activeTurnWaiter, {
      skipTokenAccounting: alreadyAccounted,
      onAgentEnd: opts.onAgentEnd,
    });
  }

  private async _settleCompletedQueuedItemAfterCancellation(
    item: QueuedItem,
    full: FullOutput<unknown>,
    modeId: string,
  ): Promise<void> {
    try {
      await this._finalizeCompletedQueuedTurn(item, full, modeId);
      await this._completeQueuedTurn(item.id, full as AgentResult);
    } catch (err) {
      if (err instanceof HarnessSessionDeletedError) throw err;
      const receipt = this._record.queueAdmissionReceipts?.[item.id];
      if (receipt?.postRunFinalizedAt !== undefined) {
        try {
          await this._completeQueuedTurn(item.id, full as AgentResult);
          return;
        } catch (completionErr) {
          if (completionErr instanceof HarnessSessionDeletedError) throw completionErr;
          this._deferCompletedQueuedItemFinalizationAfterCancellation(
            item,
            full,
            modeId,
            new QueuePostRunFinalizationPendingError(Date.now() + QUEUE_POST_RUN_FINALIZATION_RETRY_MS, completionErr),
          );
          return;
        }
      }
      this._deferCompletedQueuedItemFinalizationAfterCancellation(
        item,
        full,
        modeId,
        err instanceof QueuePostRunFinalizationPendingError
          ? err
          : new QueuePostRunFinalizationPendingError(Date.now() + QUEUE_POST_RUN_FINALIZATION_RETRY_MS, err),
      );
    }
  }

  private _deferCompletedQueuedItemFinalizationAfterCancellation(
    item: QueuedItem,
    full: FullOutput<unknown>,
    modeId: string,
    err: QueuePostRunFinalizationPendingError,
  ): void {
    this._currentQueuedItemId = undefined;
    this._currentQueuedItemSource = undefined;
    this._notifyMaybeIdle();
    const delayMs = Math.max(0, err.retryAt - Date.now());
    const timer = setTimeout(() => {
      void this._settleCompletedQueuedItemAfterCancellation(item, full, modeId).catch(() => {
        this._notifyMaybeIdle();
      });
    }, delayMs);
    timer.unref?.();
  }

  private async _markQueuedTurnCompleted(
    queuedItemId: string,
    full: FullOutput<unknown>,
    opts?: { modeId?: string },
  ): Promise<void> {
    let terminalFailure: Error | undefined;
    await this._flushUpdate(prev => {
      const receipt = prev.queueAdmissionReceipts?.[queuedItemId];
      if (!receipt && opts?.modeId === undefined) return prev;
      const receiptTerminalFailure = this._queueReceiptTerminalFailureErrorFromReceipt(receipt);
      if (receiptTerminalFailure) {
        terminalFailure = receiptTerminalFailure;
        return prev;
      }
      const now = Date.now();
      const next: SessionRecord = { ...prev };
      if (opts?.modeId !== undefined) next.modeId = opts.modeId;
      if (receipt) {
        next.queueAdmissionReceipts = {
          ...(prev.queueAdmissionReceipts ?? {}),
          [queuedItemId]:
            receipt.status === 'completed'
              ? receipt
              : {
                  ...receipt,
                  status: 'completed',
                  result: full,
                  completedAt: receipt.completedAt ?? now,
                  updatedAt: now,
                },
        };
      }
      return next;
    });
    if (terminalFailure) throw terminalFailure;
  }

  private async _finalizeQueuedRunCompletion(
    item: QueuedItem,
    full: FullOutput<unknown>,
    modeId?: string,
    activeTurnWaiter?: Promise<never>,
    opts: { skipTokenAccounting?: boolean; onAgentEnd?: () => void } = {},
  ): Promise<FullOutput<unknown>> {
    const tokenUsageDelta = opts.skipTokenAccounting
      ? undefined
      : full.finishReason === 'suspended'
        ? this._tokenUsageDeltaFromFullOutput(full)
        : this._recordTurnCompletion(full, { persist: false });
    await this._raceActiveTurnWaiter(
      this._maybeCaptureSuspend(full, item.id, modeId ?? item.mode ?? this._record.modeId, undefined, {
        tokenUsageDelta: full.finishReason === 'suspended' ? tokenUsageDelta : undefined,
      }),
      activeTurnWaiter,
    );
    if (tokenUsageDelta !== undefined && full.finishReason !== 'suspended') {
      await this._raceActiveTurnWaiter(this._persistTokenUsageOrLatch(), activeTurnWaiter);
    }
    this._emitAgentEnd({ runId: full.runId, finishReason: this._agentEndReasonForFullOutput(full), full });
    opts.onAgentEnd?.();
    await this._raceActiveTurnWaiter(this._runGoalJudge(full, (item.source ?? 'user') === 'goal'), activeTurnWaiter);
    return full;
  }

  private async _markQueuedPostRunFinalized(
    queuedItemId: string,
    opts: { tokenUsageDelta?: TokenUsage } = {},
  ): Promise<void> {
    let tokenUsageDeltaToPersist: TokenUsage | undefined;
    await this._flushUpdate(
      prev => {
        const current = prev.queueAdmissionReceipts?.[queuedItemId];
        if (!current) return prev;
        const now = Date.now();
        const nextReceipt =
          current.postRunFinalizedAt !== undefined
            ? current
            : {
                ...current,
                postRunFinalizedAt: now,
                updatedAt: now,
              };
        if (current.postRunFinalizedAt === undefined) tokenUsageDeltaToPersist = opts.tokenUsageDelta;
        return {
          ...prev,
          queueAdmissionReceipts: {
            ...(prev.queueAdmissionReceipts ?? {}),
            [queuedItemId]: nextReceipt,
          },
        };
      },
      { tokenUsageDelta: () => tokenUsageDeltaToPersist },
    );
  }

  private async _loadQueueSignalResultEvidence(
    receipt: QueueAdmissionReceipt,
  ): Promise<AgentSignalResultStatus | { status: 'not_found' }> {
    if (!receipt.signalId) return { status: 'not_found' };
    const evidence = await this._storage.loadMessageResultEvidence({
      harnessName: this._record.harnessName,
      sessionId: this.id,
      resourceId: this.resourceId,
      threadId: this.threadId,
      signalId: receipt.signalId,
    });
    if (!evidence || 'kind' in evidence) return { status: 'not_found' };
    return evidence;
  }

  private async _writeQueueSignalResultEvidence(status: AgentSignalResultStatus): Promise<void> {
    const now = Date.now();
    this._operationEvidenceSignalIds.add(status.signalId);
    await this._storage.writeMessageResultEvidence({
      ...status,
      harnessName: this._record.harnessName,
      sessionId: this.id,
      resourceId: this.resourceId,
      threadId: this.threadId,
      createdAt: now,
      updatedAt: now,
    });
    await this._cleanupOperationEvidenceIfDeleted(status);
  }

  private async _inspectQueueReceiptMemory(
    receipt: QueueAdmissionReceipt,
  ): Promise<{ status: 'not_found' } | { status: 'pending' }> {
    if (!receipt.signalId) return { status: 'not_found' };

    const memory = await this._harness._internalTryGetMemoryStorage();
    if (!memory) return { status: 'not_found' };

    const result = await memory.listMessages({ threadId: this.threadId, resourceId: this.resourceId, perPage: false });
    const messages = result.messages as StoredMessageRow[];
    return messages.some(message => message.id === receipt.signalId) ? { status: 'pending' } : { status: 'not_found' };
  }

  private async _validateQueuedAttachmentRefs(item: QueuedItem): Promise<void> {
    for (const attachment of item.attachments) {
      if (attachment.kind !== 'ref') continue;
      const loaded = await this._storage.loadAttachment({
        harnessName: this._record.harnessName,
        sessionId: attachment.ownerSessionId,
        attachmentId: attachment.attachmentId,
      });
      if (!loaded) {
        throw new HarnessAttachmentUnavailableError(attachment.ownerSessionId, 'not_found', attachment.attachmentId);
      }
      if (loaded.sha256 !== attachment.sha256) {
        throw new HarnessAttachmentUnavailableError(
          attachment.ownerSessionId,
          'digest_mismatch',
          attachment.attachmentId,
        );
      }
      if (loaded.bytes !== attachment.bytes) {
        // §4.5a has no separate `bytes_mismatch`; a recorded-size mismatch is a
        // digest/bytes mismatch (the persisted ref no longer resolves to the
        // recorded bytes).
        throw new HarnessAttachmentUnavailableError(
          attachment.ownerSessionId,
          'digest_mismatch',
          attachment.attachmentId,
        );
      }
    }
  }

  /**
   * §14.2 / §13.7: build the agent signal contents for a turn, attaching any
   * persisted attachment bytes as model file/image parts. With no attachments
   * the contents stay the bare `content` string (byte-identical to the prior
   * text-only dispatch — no behavior change for the common case). When
   * attachments are present we load each ref's bytes (already validated by
   * {@link _validateQueuedAttachmentRefs}) and build a structured user message
   * mirroring the non-v1 harness file-part pattern, so the agent turn actually
   * receives the attachment, not just the dedup identity.
   */
  private async _buildSignalContentsWithAttachments(
    content: string,
    attachments: PersistedAttachment[] | undefined,
  ): Promise<AgentSignalContents> {
    if (attachments === undefined || attachments.length === 0) {
      return content;
    }
    const parts: Array<
      | { type: 'text'; text: string }
      | { type: 'file'; data: string; mediaType: string; filename?: string }
    > = [{ type: 'text', text: content }];
    for (const attachment of attachments) {
      if (attachment.kind === 'url') {
        // A URL-only attachment carries no bytes to inline; surface it as a file
        // reference so the model still sees the link + filename.
        parts.push({ type: 'file', data: attachment.url, mediaType: attachment.mimeType, filename: attachment.name });
        continue;
      }
      const loaded = await this._storage.loadAttachment({
        harnessName: this._record.harnessName,
        sessionId: attachment.ownerSessionId,
        attachmentId: attachment.attachmentId,
      });
      if (!loaded) {
        throw new HarnessAttachmentUnavailableError(attachment.ownerSessionId, 'not_found', attachment.attachmentId);
      }
      const base64 = Buffer.from(loaded.data).toString('base64');
      // Inline both image and non-image attachment bytes through the AI SDK v5
      // file-part shape (a `data:<mime>;base64,<bytes>` URL). The v5 image-part
      // shape keys on `image`/`mediaType`, not `data`/`mimeType`, so a hand-built
      // `{ type: 'image', data, mimeType }` part silently drops its bytes when
      // MessageList runs `convertImageFilePart` (it reads `part.image`, which is
      // undefined). The file shape preserves bytes for image/* mime types too —
      // `convertToDataContent` decodes the data URL regardless of media type — so
      // one code path covers every inline attachment.
      parts.push({
        type: 'file',
        data: `data:${attachment.mimeType};base64,${base64}`,
        mediaType: attachment.mimeType,
        filename: attachment.name,
      });
    }
    return { role: 'user', content: parts } as AgentSignalContents;
  }

  private async _registerQuestion(
    params: RegisterQuestionParams & { runId?: string; toolCallId?: string; modeId?: string; modelId?: string },
  ): Promise<void> {
    this._assertOpenForTurn('ctx.registerQuestion');
    if (typeof params.questionId !== 'string' || params.questionId.length === 0) {
      throw new HarnessValidationError('ctx.registerQuestion.questionId', 'must be a non-empty string');
    }
    if (typeof params.question !== 'string' || params.question.length === 0) {
      throw new HarnessValidationError('ctx.registerQuestion.question', 'must be a non-empty string');
    }
    if (
      params.selectionMode !== undefined &&
      params.selectionMode !== 'single_select' &&
      params.selectionMode !== 'multi_select'
    ) {
      throw new HarnessValidationError('ctx.registerQuestion.selectionMode', 'must be single_select or multi_select');
    }
    const runId = params.runId ?? this._currentRunId;
    const toolCallId = params.toolCallId ?? params.questionId;
    if (!runId) {
      throw new HarnessValidationError('ctx.registerQuestion.runId', 'active run id is required');
    }
    const pending: PendingResume = {
      kind: 'question',
      itemId: params.questionId,
      runId,
      toolCallId,
      toolName: ASK_USER_TOOL_NAME,
      source: (this._record.subagentDepth ?? 0) > 0 ? 'subagent' : 'parent',
      requestedAt: Date.now(),
      modeId: params.modeId ?? this._record.modeId,
      runtimeDependencies: this._harness._runtimeDependenciesForMode(
        params.modeId ?? this._record.modeId,
        params.modelId ?? this._modelIdForQueuedItem(this._currentQueuedItemId),
      ),
      payload: {
        question: params.question,
        ...(params.options ? { options: params.options } : {}),
        ...(params.selectionMode ? { selectionMode: params.selectionMode } : {}),
      },
    };
    let registered = false;
    await this._flushUpdate(prev => {
      const current = prev.pendingResume;
      if (current) {
        if (current.kind === 'question' && current.runId === runId && current.toolCallId === toolCallId) {
          return prev;
        }
        throw new HarnessValidationError('ctx.registerQuestion', `pending resume is already "${current.kind}"`);
      }
      registered = true;
      return { ...prev, pendingResume: pending };
    });
    if (!registered) return;
    if (this._record.pendingResume) this._emitPendingEvent(this._record.pendingResume);
  }

  private async _registerSandboxAccess(
    params: RegisterSandboxAccessParams & { runId?: string; toolCallId?: string; modeId?: string; modelId?: string },
  ): Promise<void> {
    this._assertOpenForTurn('ctx.registerSandboxAccess');
    if (typeof params.requestId !== 'string' || params.requestId.length === 0) {
      throw new HarnessValidationError('ctx.registerSandboxAccess.requestId', 'must be a non-empty string');
    }
    const validSemanticTypes = new Set(['file', 'command', 'network', 'mcp', 'custom']);
    if (!validSemanticTypes.has(params.semanticType)) {
      throw new HarnessValidationError(
        'ctx.registerSandboxAccess.semanticType',
        "must be one of 'file' | 'command' | 'network' | 'mcp' | 'custom'",
      );
    }
    if (params.reason !== undefined && typeof params.reason !== 'string') {
      throw new HarnessValidationError('ctx.registerSandboxAccess.reason', 'must be a string when provided');
    }
    const sanitizedPayload =
      params.payload !== undefined ? assertJsonValue(params.payload, 'ctx.registerSandboxAccess.payload') : undefined;
    const runId = params.runId ?? this._currentRunId;
    const toolCallId = params.toolCallId ?? params.requestId;
    if (!runId) {
      throw new HarnessValidationError('ctx.registerSandboxAccess.runId', 'active run id is required');
    }
    const modeId = params.modeId ?? this._record.modeId;
    const pending: PendingResume = {
      kind: 'sandbox-access',
      itemId: params.requestId,
      runId,
      toolCallId,
      source: (this._record.subagentDepth ?? 0) > 0 ? 'subagent' : 'parent',
      requestedAt: Date.now(),
      modeId,
      runtimeDependencies: this._harness._runtimeDependenciesForMode(
        modeId,
        params.modelId ?? this._modelIdForQueuedItem(this._currentQueuedItemId),
      ),
      payload: {
        sandboxAccess: {
          semanticType: params.semanticType,
          ...(params.reason !== undefined ? { reason: params.reason } : {}),
          ...(sanitizedPayload !== undefined ? { payload: sanitizedPayload } : {}),
        },
      },
    };
    let registered = false;
    await this._flushUpdate(prev => {
      const current = prev.pendingResume;
      if (current) {
        const currentSandboxAccess = current.payload?.sandboxAccess;
        if (
          current.kind === 'sandbox-access' &&
          current.runId === runId &&
          current.toolCallId === toolCallId &&
          current.itemId === params.requestId &&
          currentSandboxAccess?.semanticType === params.semanticType &&
          currentSandboxAccess?.reason === params.reason &&
          jsonValuesEqual(currentSandboxAccess?.payload, sanitizedPayload)
        ) {
          return prev;
        }
        throw new HarnessValidationError('ctx.registerSandboxAccess', `pending resume is already "${current.kind}"`);
      }
      registered = true;
      return { ...prev, pendingResume: pending };
    });
    if (!registered) return;
    // §10.2: sandbox/path-access has no dedicated event — it projects to
    // question_pending via the captured pending resume.
    if (this._record.pendingResume) this._emitPendingEvent(this._record.pendingResume);
  }

  private async _registerPlanApproval(
    params: RegisterPlanApprovalParams & { runId?: string; toolCallId?: string; modeId?: string; modelId?: string },
  ): Promise<void> {
    this._assertOpenForTurn('ctx.registerPlanApproval');
    if (typeof params.planId !== 'string' || params.planId.length === 0) {
      throw new HarnessValidationError('ctx.registerPlanApproval.planId', 'must be a non-empty string');
    }
    if (params.title !== undefined && typeof params.title !== 'string') {
      throw new HarnessValidationError('ctx.registerPlanApproval.title', 'must be a string when provided');
    }
    if (typeof params.plan !== 'string' || params.plan.length === 0) {
      throw new HarnessValidationError('ctx.registerPlanApproval.plan', 'must be a non-empty string');
    }
    const runId = params.runId ?? this._currentRunId;
    const toolCallId = params.toolCallId ?? params.planId;
    if (!runId) {
      throw new HarnessValidationError('ctx.registerPlanApproval.runId', 'active run id is required');
    }
    const submittingModeId = params.modeId ?? this._record.modeId;
    const submittingMode = this._harness._getMode(submittingModeId);
    const pending: PendingResume = {
      kind: 'plan-approval',
      itemId: params.planId,
      runId,
      toolCallId,
      toolName: SUBMIT_PLAN_TOOL_NAME,
      source: (this._record.subagentDepth ?? 0) > 0 ? 'subagent' : 'parent',
      requestedAt: Date.now(),
      modeId: submittingModeId,
      runtimeDependencies: this._harness._runtimeDependenciesForMode(
        submittingModeId,
        params.modelId ?? this._modelIdForQueuedItem(this._currentQueuedItemId),
      ),
      payload: {
        ...(params.title !== undefined ? { title: params.title } : {}),
        plan: params.plan,
      },
      ...(submittingMode.transitionsTo ? { transitionModeId: submittingMode.transitionsTo } : {}),
    };
    let registered = false;
    await this._flushUpdate(prev => {
      const current = prev.pendingResume;
      if (current) {
        if (current.kind === 'plan-approval' && current.runId === runId && current.toolCallId === toolCallId) {
          return prev;
        }
        throw new HarnessValidationError('ctx.registerPlanApproval', `pending resume is already "${current.kind}"`);
      }
      registered = true;
      return { ...prev, pendingResume: pending };
    });
    if (!registered) return;
    if (this._record.pendingResume) this._emitPendingEvent(this._record.pendingResume);
  }

  /**
   * Settle a queued item's resolver with success and remove it from the
   * head of `pendingQueue`. The CAS write here is the durable record that
   * the item ran exactly once. Crash recovery uses `pendingQueue[0]`,
   * `pendingResume`, queue receipts, and signal-result evidence to decide
   * whether to replay, await, or fail a previously admitted item.
   */
  private async _completeQueuedTurn(itemId: string, result: AgentResult): Promise<void> {
    if (this.isClosed) {
      const resolver = this._queueResolvers.get(itemId);
      if (resolver) {
        this._queueResolvers.delete(itemId);
        resolver.resolve(result);
      }
      return;
    }
    const now = Date.now();
    let terminalFailure: Error | undefined;
    await this._flushUpdate(prev => {
      const receipt = prev.queueAdmissionReceipts?.[itemId];
      const receiptTerminalFailure = this._queueReceiptTerminalFailureErrorFromReceipt(receipt);
      if (receiptTerminalFailure) {
        terminalFailure = receiptTerminalFailure;
        return prev;
      }
      return {
        ...prev,
        pendingQueue: (prev.pendingQueue ?? []).filter(x => x.id !== itemId),
        ...(receipt
          ? {
              queueAdmissionReceipts: {
                ...(prev.queueAdmissionReceipts ?? {}),
                [itemId]: {
                  ...receipt,
                  status: 'completed',
                  result,
                  completedAt: receipt.completedAt ?? now,
                  updatedAt: now,
                },
              },
            }
          : {}),
      };
    });
    this._currentQueuedItemId = undefined;
    this._currentQueuedItemSource = undefined;
    // §10.2 OperationEvent — `queue_completed` is the queue settlement boundary,
    // not `agent_end`. Requires the accepted-signal identities (`runId`,
    // `signalId`); skip if the receipt never crossed the agent boundary.
    if (!terminalFailure) {
      const settledReceipt = this._record.queueAdmissionReceipts?.[itemId];
      if (settledReceipt?.runId && settledReceipt.signalId) {
        this._emit({
          type: 'queue_completed',
          runId: settledReceipt.runId,
          queuedItemId: itemId,
          signalId: settledReceipt.signalId,
          ...(settledReceipt.admissionId ? { admissionId: settledReceipt.admissionId } : {}),
          result,
        });
      }
    }
    const resolver = this._queueResolvers.get(itemId);
    if (resolver) {
      this._queueResolvers.delete(itemId);
      if (terminalFailure) {
        resolver.reject(terminalFailure);
      } else {
        resolver.resolve(result);
      }
    }
    this._notifyMaybeIdle();
    if (terminalFailure) return;
    // Kick the drain again — there may be more items waiting.
    void this._maybeDrainQueue();
  }

  /** Same as `_completeQueuedTurn` but rejects the resolver with `err`. */
  private async _failQueuedTurn(itemId: string, err: unknown): Promise<void> {
    if (this.isClosed) {
      const resolver = this._queueResolvers.get(itemId);
      if (resolver) {
        this._queueResolvers.delete(itemId);
        resolver.reject(err);
      }
      return;
    }
    const now = Date.now();
    let completedResult: AgentResult | undefined;
    await this._flushUpdate(prev => {
      const receipt = prev.queueAdmissionReceipts?.[itemId];
      if (receipt?.status === 'completed') {
        completedResult = receipt.result as AgentResult | undefined;
        return {
          ...prev,
          pendingQueue: (prev.pendingQueue ?? []).filter(x => x.id !== itemId),
        };
      }
      return {
        ...prev,
        pendingQueue: (prev.pendingQueue ?? []).filter(x => x.id !== itemId),
        ...(receipt
          ? {
              queueAdmissionReceipts: {
                ...(prev.queueAdmissionReceipts ?? {}),
                [itemId]: {
                  ...receipt,
                  status: 'failed',
                  error: projectHarnessPublicError(err),
                  failedAt: receipt.failedAt ?? now,
                  updatedAt: now,
                },
              },
            }
          : {}),
      };
    });
    this._currentQueuedItemId = undefined;
    this._currentQueuedItemSource = undefined;
    // §10.2 OperationEvent — a late failure may race a completion that already
    // won (`completedResult`): emit `queue_completed` for that, else
    // `queue_failed`. `signalId`/`runId` are optional on the failed event.
    {
      const settledReceipt = this._record.queueAdmissionReceipts?.[itemId];
      if (completedResult !== undefined) {
        if (settledReceipt?.runId && settledReceipt.signalId) {
          this._emit({
            type: 'queue_completed',
            runId: settledReceipt.runId,
            queuedItemId: itemId,
            signalId: settledReceipt.signalId,
            ...(settledReceipt.admissionId ? { admissionId: settledReceipt.admissionId } : {}),
            result: completedResult,
          });
        }
      } else {
        this._emit({
          type: 'queue_failed',
          queuedItemId: itemId,
          ...(settledReceipt?.runId ? { runId: settledReceipt.runId } : {}),
          ...(settledReceipt?.signalId ? { signalId: settledReceipt.signalId } : {}),
          ...(settledReceipt?.admissionId ? { admissionId: settledReceipt.admissionId } : {}),
          error: projectHarnessPublicError(err),
        });
      }
    }
    const resolver = this._queueResolvers.get(itemId);
    if (resolver) {
      this._queueResolvers.delete(itemId);
      if (completedResult !== undefined) {
        resolver.resolve(completedResult);
      } else {
        resolver.reject(err);
      }
    }
    this._notifyMaybeIdle();
    void this._maybeDrainQueue();
  }

  private async _failPendingQueueForClose(err: unknown): Promise<void> {
    const queuedIds = (this._record.pendingQueue ?? []).map(item => item.id);
    if (queuedIds.length === 0) return;

    const completedResults = new Map<string, AgentResult>();
    const failedIds = new Set<string>();
    const now = Date.now();
    await this._flushUpdate(prev => {
      const next: SessionRecord = {
        ...prev,
        pendingQueue: [],
      };
      const receipts = prev.queueAdmissionReceipts ?? {};
      const nextReceipts: Record<string, QueueAdmissionReceipt> = { ...receipts };
      for (const item of prev.pendingQueue ?? []) {
        const receipt = receipts[item.id];
        if (!receipt) {
          failedIds.add(item.id);
          continue;
        }
        if (receipt.status === 'completed') {
          completedResults.set(item.id, receipt.result as AgentResult);
          continue;
        }
        failedIds.add(item.id);
        nextReceipts[item.id] = {
          ...receipt,
          status: 'failed',
          error: projectHarnessPublicError(err),
          failedAt: receipt.failedAt ?? now,
          updatedAt: now,
        };
      }
      next.queueAdmissionReceipts = nextReceipts;
      return next;
    });

    this._currentQueuedItemId = undefined;
    this._currentQueuedItemSource = undefined;
    for (const itemId of queuedIds) {
      const resolver = this._queueResolvers.get(itemId);
      if (!resolver) continue;
      this._queueResolvers.delete(itemId);
      const completed = completedResults.get(itemId);
      if (completed !== undefined) {
        resolver.resolve(completed);
      } else if (failedIds.has(itemId)) {
        resolver.reject(err);
      }
    }
    this._notifyMaybeIdle();
  }

  private _parkQueuedTurn(itemId: string, err: unknown): void {
    this._currentQueuedItemId = undefined;
    this._currentQueuedItemSource = undefined;
    this._notifyMaybeIdle();
    if (err instanceof QueueRecoveryPendingError) {
      const delayMs = Math.max(0, err.retryAt - Date.now());
      const timer = setTimeout(() => void this._maybeDrainQueue(), delayMs);
      this._unrefQueueTimerIfBackgroundOnly(timer);
      return;
    }
    const resolver = this._queueResolvers.get(itemId);
    if (resolver) {
      this._queueResolvers.delete(itemId);
      resolver.reject(err);
    }
  }

  private _ensureQueuedItemContext(queuedItemId: string): void {
    if (this._currentQueuedItemId !== undefined) return;
    const queuedItem = this._record.pendingQueue.find(item => item.id === queuedItemId);
    this._currentQueuedItemId = queuedItemId;
    this._currentQueuedItemSource = queuedItem?.source ?? 'user';
  }

  private _deferQueuedTurnRetry(err: QueuePostRunFinalizationPendingError): void {
    this._currentQueuedItemId = undefined;
    this._currentQueuedItemSource = undefined;
    this._notifyMaybeIdle();
    const delayMs = Math.max(0, err.retryAt - Date.now());
    const timer = setTimeout(() => void this._maybeDrainQueue(), delayMs);
    this._unrefQueueTimerIfBackgroundOnly(timer);
  }

  private _scheduleQueueWakeupForPendingQueue(): void {
    const now = Date.now();
    let wakeAt: number | undefined;
    for (const item of this._record.pendingQueue ?? []) {
      const candidates = [item.notBefore, item.deadline].filter(
        (value): value is number => value !== undefined && value > now,
      );
      for (const candidate of candidates) {
        if (wakeAt === undefined || candidate < wakeAt) wakeAt = candidate;
      }
    }
    if (wakeAt === undefined) {
      this._clearQueueWakeTimer();
      if ((this._record.pendingQueue?.length ?? 0) > 0) {
        const timer = setTimeout(() => void this._maybeDrainQueue(), 0);
        this._unrefQueueTimerIfBackgroundOnly(timer);
      }
      return;
    }
    if (this._queueWakeAt !== undefined && this._queueWakeAt <= wakeAt) return;
    this._clearQueueWakeTimer();
    this._queueWakeAt = wakeAt;
    const delayMs = Math.min(Math.max(0, wakeAt - now), 2_147_483_647);
    this._queueWakeTimer = setTimeout(() => {
      this._queueWakeTimer = undefined;
      this._queueWakeAt = undefined;
      void this._maybeDrainQueue();
    }, delayMs);
    this._unrefQueueTimerIfBackgroundOnly(this._queueWakeTimer);
  }

  private _clearQueueWakeTimer(): void {
    if (this._queueWakeTimer !== undefined) {
      clearTimeout(this._queueWakeTimer);
      this._queueWakeTimer = undefined;
    }
    this._queueWakeAt = undefined;
  }

  private _unrefQueueTimerIfBackgroundOnly(timer: ReturnType<typeof setTimeout>): void {
    if (this._queueResolvers.size === 0) {
      timer.unref?.();
    }
  }

  /** @internal — used by the Harness on hydration to start replay drain. */
  async _kickQueueDrain(): Promise<void> {
    return this._maybeDrainQueue();
  }

  // -------------------------------------------------------------------------
  // Internal helpers.
  // -------------------------------------------------------------------------

  private _assertLive(_method: string): void {
    if (this._state === 'deleted') {
      throw new HarnessSessionDeletedError(this.id, this._record.resourceId, this._record.threadId);
    }
    if (this.isClosing) {
      throw harnessSessionClosingError(this);
    }
    if (this._state !== 'live') {
      throw new HarnessSessionClosedError(this.id);
    }
  }

  private _assertNotDeleted(): void {
    if (this._state === 'deleted') {
      throw new HarnessSessionDeletedError(this.id, this._record.resourceId, this._record.threadId);
    }
  }

  private _assertOpenForTurn(_method: string): void {
    if (this._state === 'deleted') {
      throw new HarnessSessionDeletedError(this.id, this._record.resourceId, this._record.threadId);
    }
    if (this._state === 'closed') {
      throw new HarnessSessionClosedError(this.id);
    }
    const cancelRequest = this._currentCancelRequest();
    if (cancelRequest !== undefined) {
      throw new HarnessSessionCancelledError(this.id, cancelRequest.reason);
    }
  }

  private _canDrainQueue(): boolean {
    if (this._record.cancelRequest !== undefined) return this._hasCompletedQueuedItemsAfterCancellation();
    if (this._state === 'live') return true;
    if (!this.isClosing) return false;
    return this._record.closeDeadlineAt === undefined || Date.now() < this._record.closeDeadlineAt;
  }

  private _hasCompletedQueuedItemsAfterCancellation(): boolean {
    return (this._record.pendingQueue ?? []).some(item => {
      const receipt = this._record.queueAdmissionReceipts?.[item.id];
      return receipt?.status === 'completed';
    });
  }

  /**
   * Apply an update to the in-memory record, CAS-write to storage, and
   * adopt the returned version. Single point of truth so every setter
   * stays consistent with the lease + version contract (§5.8).
   */
  private _flushUpdate(
    update: (prev: SessionRecord) => SessionRecord,
    opts?: {
      attachmentReferences?: SaveAttachmentReferenceInput[];
      ifVersion?: number;
      tokenUsageDelta?: TokenUsage | (() => TokenUsage | undefined);
    },
  ): Promise<void> {
    if (this._state === 'closed') {
      return Promise.reject(new HarnessSessionClosedError(this.id));
    }
    if (this._state === 'deleted') {
      return Promise.reject(new HarnessSessionDeletedError(this.id, this._record.resourceId, this._record.threadId));
    }
    if (this._state === 'evicted') {
      return Promise.reject(new HarnessSessionClosedError(this.id));
    }
    const run = async (): Promise<void> => {
      // §5.8 write-concurrency: a caller-supplied `ifVersion` (remote state
      // PATCH OCC) must fail closed on conflict — never retry. Internal flushes
      // may recover from a cross-owner CAS conflict once (same-process writes
      // serialize on the flush chain, so a conflict here means another owner).
      const maxRetries = opts?.ifVersion !== undefined ? 0 : 1;
      for (let attempt = 0; ; attempt++) {
        const leaseExpiresAt = this._record.leaseExpiresAt;
        if (
          (this._state === 'live' || this._state === 'closing') &&
          leaseExpiresAt !== undefined &&
          leaseExpiresAt <= Date.now()
        ) {
          await this._harness._internalEvictLiveSessionLeaseLost(this);
          throw new HarnessSessionLockedError(this.id, 'unknown', leaseExpiresAt);
        }
        if (opts?.ifVersion !== undefined && this._record.version !== opts.ifVersion) {
          throw new HarnessStateConflictError(this.id, opts.ifVersion, this._record.version);
        }
        // §10.2 state_changed: capture the pre-update durable state so we can
        // diff it after the write commits. Internal flushes that don't touch
        // `state` (mode/model/token/goal/pendingResume) keep the same reference
        // and produce no changed keys, so they never emit.
        const prevState = this._record.state;
        const updated = update(this._record);
        const tokenUsageDelta =
          typeof opts?.tokenUsageDelta === 'function' ? opts.tokenUsageDelta() : opts?.tokenUsageDelta;
        const tokenUsageForSave =
          tokenUsageDelta !== undefined
            ? {
                promptTokens: this._tokenUsage.promptTokens + tokenUsageDelta.promptTokens,
                completionTokens: this._tokenUsage.completionTokens + tokenUsageDelta.completionTokens,
                totalTokens: this._tokenUsage.totalTokens + tokenUsageDelta.totalTokens,
              }
            : this._tokenUsage;
        const next: SessionRecord = {
          ...updated,
          // Overlay the live token-usage counter so every CAS write persists the
          // latest aggregate. Updaters never need to thread `tokenUsage` through
          // their closures, and `_recordTurnCompletion` mutations between save
          // construction and post-save assignment are not lost — we re-overlay
          // from the live counter below.
          tokenUsage: { ...tokenUsageForSave },
          lastActivityAt: Date.now(),
        };
        const saveOpts = {
          harnessName: this._record.harnessName,
          ownerId: this._ownerId,
          ifVersion: this._record.version,
        };
        try {
          const saved =
            opts?.attachmentReferences && opts.attachmentReferences.length > 0
              ? await this._storage.saveSessionWithAttachmentReferences(next, saveOpts, opts.attachmentReferences)
              : await this._storage.saveSession(next, saveOpts);
          // `tokenUsageDelta` is applied only after a successful save, so a
          // failed attempt never double-counts on retry.
          this._applyTokenUsageDelta(tokenUsageDelta);
          this._clearPendingTokenUsageFlushErrorIfSaved(next.tokenUsage);
          // §5.8 advance-only lease expiry: `next` was built from an older
          // `_record`, so a subtree renewal that marked a later expiry on this
          // session while the save was in flight must not be regressed by this
          // reassignment. Carry the max so the local lease guard never fences a
          // still-valid session early. saveSession does not persist lease
          // metadata, so this is an in-memory reconciliation only.
          const committedLeaseExpiresAt = Math.max(next.leaseExpiresAt ?? 0, this._record.leaseExpiresAt ?? 0) || undefined;
          this._record = {
            ...next,
            tokenUsage: { ...this._tokenUsage },
            version: saved.version,
            leaseExpiresAt: committedLeaseExpiresAt,
          };
          // §10.2 state_changed — emit only when durable `session.state` actually
          // changed, after the record is committed so subscribers reading state
          // see the post-write value.
          const stateChangedKeys = diffStateKeys(prevState, this._record.state);
          if (stateChangedKeys.length > 0) {
            this._emit({
              type: 'state_changed',
              // §10.2: full post-commit root. `session.state` is `unknown` and may
              // be a scalar/array/object root; carry it as-is so a `'$'`-keyed root
              // change is readable from this field. `undefined` (no state yet) is
              // reported as `{}` to keep the prior empty-object default.
              state: (this._record.state ?? {}) as JsonValue,
              changedKeys: stateChangedKeys,
            });
          }
          return;
        } catch (err) {
          // §5.8: the save's lease-holder check fired — another owner holds the
          // lease, so subtree ownership was lost (root conflict) or split (child
          // conflict). Fence the WHOLE live subtree rather than only this session
          // or retrying into a contested record.
          if (err instanceof HarnessStorageLeaseConflictError) {
            await this._harness._internalEvictSubtreeLeaseLost(this);
            throw new HarnessSessionLockedError(this.id, err.heldBy, err.expiresAt);
          }
          if (!(err instanceof HarnessStorageVersionConflictError) || attempt >= maxRetries) {
            throw err;
          }
          // §5.8: before re-applying, prove this owner still holds the SUBTREE
          // lease. For a child this proves through the root, not the child's own
          // row — renewing the child alone would not detect a parent/root
          // ownership loss or subtree split. The helper renews root + active
          // descendants and fences the whole subtree (throwing locked/not-found)
          // if ownership can't be proven.
          await this._harness._internalRenewProveSubtree(
            this,
            this._getEffectiveLeaseTtlMs(this._harness._internalLeaseTtlMs),
          );
          const reloaded = await this._storage.loadSession({
            harnessName: this._record.harnessName,
            sessionId: this.id,
          });
          if (!reloaded) {
            await this._harness._internalEvictLiveSessionLeaseLost(this);
            throw new HarnessSessionDeletedError(this.id, this._record.resourceId, this._record.threadId);
          }
          // Adopt the concurrent durable state under our proven lease, then loop
          // to re-apply the (pure) updater against the reloaded record.
          this._record = { ...reloaded, ownerId: this._ownerId, leaseExpiresAt: this._record.leaseExpiresAt };
        }
      }
    };
    // Chain so concurrent callers serialize against the latest in-memory
    // version. Swallow chain-link errors so one caller's failure doesn't
    // poison subsequent flushes.
    const next = this._flushChain.then(run, run);
    this._flushChain = next.catch(() => {});
    return next;
  }

  /**
   * Build the toolset surface for a single turn:
   *   - mode.tools (replace) wins over agent's own tools
   *   - mode.additionalTools merges with agent's tools
   *   - per-call additionalTools layer on top of whatever the mode produced
   *
   * Returns undefined when no overrides apply (agent runs with its own tools).
   */
  private _buildToolsets(mode: HarnessMode, callAdditional?: ToolsInput): Record<string, ToolsInput> | undefined {
    const toolsets: Record<string, ToolsInput> = {};
    if (mode.tools) toolsets[`mode:${mode.id}`] = mode.tools;
    if (mode.additionalTools) toolsets[`mode:${mode.id}:add`] = mode.additionalTools;
    if (callAdditional) toolsets[`call:additional`] = callAdditional;

    // Built-in `spawn_subagent` tool. Registered automatically when the
    // harness has any subagent types configured. Closes over this session
    // so the tool can resolve the registry, create child sessions, bridge
    // events back, and enforce the depth cap (§9).
    const spawn = createSpawnSubagentTool(this);
    if (spawn) {
      toolsets['harness:builtin'] = { [SPAWN_SUBAGENT_TOOL_ID]: spawn };
    }

    return Object.keys(toolsets).length === 0 ? undefined : toolsets;
  }

  /**
   * Build the per-turn `RequestContext` that the agent passes to tools. The
   * `'harness'` slot exposes `HarnessRequestContext` (§6.1). Tools read it
   * with `context.requestContext.get('harness')`.
   *
   * The slot is constructed fresh per turn so identity reads, the state
   * snapshot, abort plumbing, and event emission all see the current state
   * of the session. Functional `setState` updates serialize through the
   * same `_flushUpdate` chain that backs `Session.setState`.
   */
  private async _buildRequestContext(turn: {
    modeId: string;
    modelId: string;
    abortSignal: AbortSignal;
    persistedRequestContext?: PersistedRequestContextInput;
    resolveWorkspace?: boolean;
  }): Promise<RequestContext> {
    const session = this;
    const stateSnapshot = (this._record.state ?? {}) as unknown;
    const persistedRequestContext = turn.persistedRequestContext
      ? clonePersistedRequestContext(turn.persistedRequestContext)
      : undefined;
    // §6.2 strict-lazy materialization: context construction MUST NOT cold-start a
    // (cloud) workspace solely to fill the `ctx.workspace` field. Default to the
    // cached handle (non-materializing); a tool that needs the filesystem calls
    // `ctx.resolveWorkspace()`, and the workspace's own built-in tools resolve
    // through the agent's workspace independently of this slot. This is UNIFORM
    // across top-level and subagent sessions: a `fresh`/per-session subagent owns
    // a DISTINCT workspace identity, but the backing sandbox is provisioned only
    // when that session first resolves it — `fresh` guarantees independence on
    // resolution, not allocation at spawn (task #36). A caller may force eager
    // materialization with `resolveWorkspace: true`.
    let workspace: Workspace | undefined;
    if (turn.resolveWorkspace === true) {
      try {
        workspace = await this._getWorkspaceUnchecked();
      } catch {
        // Leave undefined — tools that need a workspace will get a null slot.
        // The registry has already emitted workspace_error so subscribers know.
        workspace = undefined;
      }
    } else {
      workspace = this.peekWorkspace();
    }
    const harnessSlot: HarnessRequestContext<unknown> = {
      // §6.1: harnessName is the stable namespace; harnessInstanceId is per-process.
      harnessName: this._record.harnessName,
      harnessInstanceId: this._harness.ownerId,
      sessionId: this.id,
      threadId: this.threadId,
      resourceId: this.resourceId,
      modeId: turn.modeId,
      modelId: turn.modelId,
      ...(persistedRequestContext?.metadata ? { app: persistedRequestContext.metadata } : {}),
      ...(persistedRequestContext?.channel ? { channel: persistedRequestContext.channel } : {}),
      state: stateSnapshot,
      getState: () => (session._record.state ?? {}) as unknown,
      setState: ((updatesOrUpdater: unknown) =>
        session._setTurnState(
          updatesOrUpdater as Partial<unknown> | ((prev: unknown) => unknown),
        )) as HarnessRequestContext<unknown>['setState'],
      abortSignal: turn.abortSignal,
      registerQuestion: params => session._registerQuestion({ ...params, modeId: turn.modeId, modelId: turn.modelId }),
      registerPlanApproval: params =>
        session._registerPlanApproval({ ...params, modeId: turn.modeId, modelId: turn.modelId }),
      registerSandboxAccess: params =>
        session._registerSandboxAccess({ ...params, modeId: turn.modeId, modelId: turn.modelId }),
      emitCustomEvent: event => session._emitCustomEvent(event),
      extendLease: opts => session.extendLease(opts),
      // Subagent linkage — set from the record so spawned sessions report
      // their depth + parent linkage on the harness slot.
      subagentDepth: this._record.subagentDepth ?? 0,
      source: (this._record.subagentDepth ?? 0) > 0 ? 'subagent' : 'parent',
      parentSessionId: this._record.parentSessionId,
      getSubagentModel: params => {
        const agentType = params?.agentType;
        if (!agentType) return null;
        return this._record.subagentModelOverrides?.[agentType] ?? null;
      },
      // §6.1 workspace access — delegate to the owning session so the
      // materialization/lease logic stays in one place. `getWorkspace()` is the
      // sync cached read (non-materializing); `resolveWorkspace()` cold-starts.
      hasWorkspace: () => session.hasWorkspace(),
      isWorkspaceReady: () => session.isWorkspaceReady(),
      getWorkspace: () => session.peekWorkspace(),
      resolveWorkspace: () => session.resolveWorkspace(),
      // §5.6/§10.6 activity timeline read-model is not built in this Harness build.
      getActivityTimeline: () =>
        Promise.reject(
          new HarnessValidationError(
            'getActivityTimeline()',
            'the activity timeline read-model (§5.6/§10.6) is not implemented in this Harness build',
          ),
        ),
      // Tool-facing skill execution. Delegates back to the owning session
      // so resolution, args validation, prompt construction, and dispatch
      // stay in one place (§4.6).
      useSkill: (ref, opts) => session._skillsUse(ref, opts),
      ...(workspace ? { workspace } : {}),
    };
    const entries: [string, unknown][] = [['harness', harnessSlot]];
    if (persistedRequestContext?.metadata) {
      entries.push(['app', persistedRequestContext.metadata]);
    }
    if (persistedRequestContext?.channel) {
      entries.push(['channel', persistedRequestContext.channel]);
    }
    return new RequestContext(entries);
  }

  private async _setTurnState<TState = unknown>(
    updatesOrUpdater: Partial<TState> | ((prev: TState) => TState),
  ): Promise<void> {
    // Tool-facing state writes belong to an already-admitted turn, so they
    // remain valid while close drains. `_flushUpdate` still rejects after the
    // terminal closed marker lands.
    await this._flushUpdate(prev => {
      const current = (prev.state ?? {}) as TState;
      const next =
        typeof updatesOrUpdater === 'function'
          ? (updatesOrUpdater as (prev: TState) => TState)(current)
          : ({ ...(current as object), ...(updatesOrUpdater as object) } as TState);
      // §5.1: same pre-commit JSON-serializability guard as `setState()` — a tool
      // that writes a non-serializable value fails closed, not silently.
      const badPath = firstNonJsonStatePath(next, '$', new Set());
      if (badPath !== undefined) throw new HarnessStateSerializationError(this.id, badPath);
      return { ...prev, state: next };
    });
  }

  /** @internal — used by the Harness as soon as close starts. */
  _beginClosing(): void {
    if (this.isClosed) {
      return;
    }
    this._state = 'closing';
    this._rejectIdleWaiters(harnessSessionClosingError(this));
  }

  /** @internal — restore admission if close failed before the durable marker committed. */
  _restoreLiveAfterFailedClose(): void {
    if (this._state === 'closing' && this._record.closingAt === undefined && this._record.closedAt === undefined) {
      this._state = 'live';
    }
  }

  /**
   * @internal — used by the Harness after close starts. New work is rejected
   * immediately while previously admitted flushes serialize before the marker.
   */
  _flushClosingMarker(params: { closeTimeoutMs: number; closeDeadlineAt?: number }): Promise<SessionRecord> {
    if (this.isClosed) {
      return Promise.resolve(this._record);
    }
    this._beginClosing();

    const run = async (): Promise<SessionRecord> => {
      const closingAt = this._record.closingAt ?? Date.now();
      // §5.5: ONE fixed deadline for the whole subtree. The close owner stamps a
      // single `closeDeadlineAt` at the root and propagates it here, so a
      // descendant marked mid-walk never resets the clock to its own wall-clock
      // `now + closeTimeoutMs`. The per-node `closingAt + closeTimeoutMs` is only
      // a fallback for the root (or a standalone flush where no subtree deadline
      // is supplied) and is overridden by any deadline already persisted on the
      // record (resumed close).
      const closeDeadlineAt = this._record.closeDeadlineAt ?? params.closeDeadlineAt ?? closingAt + params.closeTimeoutMs;
      const next: SessionRecord = {
        ...this._record,
        closingAt,
        closeDeadlineAt,
        tokenUsage: { ...this._tokenUsage },
        lastActivityAt: Date.now(),
      };
      const saved = await this._storage.saveSession(next, {
        harnessName: this._record.harnessName,
        ownerId: this._ownerId,
        ifVersion: this._record.version,
      });
      this._record = { ...next, version: saved.version };
      return this._record;
    };
    const next = this._flushChain.then(run, run);
    this._flushChain = next.then(
      () => {},
      () => {},
    );
    return next;
  }

  /** @internal — used by the Harness after descendants are terminalized. */
  _flushClosedMarker(closedAt: number): Promise<SessionRecord> {
    const run = async (): Promise<SessionRecord> => {
      if (this._record.closedAt !== undefined) {
        return this._record;
      }
      const next: SessionRecord = {
        ...this._record,
        tokenUsage: { ...this._tokenUsage },
        lastActivityAt: closedAt,
        closedAt,
      };
      const saved = await this._storage.saveSession(next, {
        harnessName: this._record.harnessName,
        ownerId: this._ownerId,
        ifVersion: this._record.version,
      });
      this._record = { ...next, version: saved.version };
      return this._record;
    };
    const next = this._flushChain.then(run, run);
    this._flushChain = next.then(
      () => {},
      () => {},
    );
    return next;
  }

  /**
   * @internal — used by the Harness during `close()` to mark this instance
   * terminal. Does not touch storage or release the lease — those are the
   * harness's job. Idempotent.
   */
  _markClosed(updatedRecord: SessionRecord): void {
    this._clearQueueWakeTimer();
    this._leaseExtensionDeadline = undefined;
    this._record = updatedRecord;
    this._state = 'closed';
    this._tearDownThreadSubscription(new HarnessValidationError('session.close()', 'Session closed'));
    this._rejectIdleWaiters(new HarnessSessionClosedError(this.id));
  }

  /** @internal — used by Harness hard-delete after storage has removed the row. */
  _markDeleted(): void {
    const err = new HarnessSessionDeletedError(this.id, this._record.resourceId, this._record.threadId);
    this._leaseExtensionDeadline = undefined;
    this._state = 'deleted';
    this._rejectIdleWaiters(err);
    this._rejectActiveTurnWaiters(err);
    const activeTurn = this._currentTurnAbortController;
    if (activeTurn) {
      // §6.2: hard-delete is a terminal teardown — the record is gone, so tools
      // should run rollback/cleanup (`session_closed`), not the record-survives
      // `process_restart` semantics. The operation rejection above already carries
      // the precise HarnessSessionDeletedError.
      activeTurn.abort(new HarnessAbortedError(this.id, 'session_closed'));
      this._endTurn(activeTurn);
    }
    if (this._queuedResumeRecoveryTimer !== undefined) {
      clearTimeout(this._queuedResumeRecoveryTimer);
      this._queuedResumeRecoveryTimer = undefined;
    }
    this._clearQueueWakeTimer();
    this._currentQueuedItemId = undefined;
    this._currentQueuedItemSource = undefined;
    for (const [queuedItemId, resolver] of this._queueResolvers) {
      this._queueResolvers.delete(queuedItemId);
      resolver.reject(err);
    }
    this._tearDownThreadSubscription(err);
  }

  /** @internal — signal identities that may have written operation evidence for this live session. */
  _deletedOperationEvidenceSignalIds(): string[] {
    return Array.from(this._operationEvidenceSignalIds);
  }

  /** @internal — harness bridge subscription that remains valid while closing. */
  _subscribeInternal(listener: HarnessEventListener): HarnessEventUnsubscribe {
    return this._emitter.subscribe(listener);
  }

  /**
   * @internal — used by the Harness when an idle/pressure eviction drops the
   * instance from the live map (§5.4). The record stays active in storage;
   * the session can be re-hydrated. Currently unused; lands with eviction.
   */
  _markEvicted(updatedRecord: SessionRecord): void {
    const err = new HarnessValidationError('session.evict()', 'Session evicted');
    this._leaseExtensionDeadline = undefined;
    this._record = updatedRecord;
    this._state = 'evicted';
    this._tearDownThreadSubscription(err);
    this._rejectIdleWaiters(new HarnessSessionClosedError(this.id));
    this._rejectActiveTurnWaiters(err);
    const activeTurn = this._currentTurnAbortController;
    if (activeTurn) {
      // §6.2: eviction releases live process ownership without a durable close —
      // tools see `process_restart` so they do best-effort cleanup, not rollback.
      activeTurn.abort(new HarnessAbortedError(this.id, 'process_restart'));
      this._endTurn(activeTurn);
    }
    if (this._queuedResumeRecoveryTimer !== undefined) {
      clearTimeout(this._queuedResumeRecoveryTimer);
      this._queuedResumeRecoveryTimer = undefined;
    }
    this._clearQueueWakeTimer();
    this._currentQueuedItemId = undefined;
    this._currentQueuedItemSource = undefined;
    for (const [queuedItemId, resolver] of this._queueResolvers) {
      this._queueResolvers.delete(queuedItemId);
      resolver.reject(err);
    }
  }

  /** @internal — update local lease metadata after the owning Harness renews storage. */
  _markLeaseRenewed(expiresAt: number): void {
    if (this._state !== 'live' && this._state !== 'closing') return;
    // Advance-only (§5.8): a subtree renewal marks the root AND every live
    // descendant from the root's renewal chain, which can interleave with a
    // descendant's own `_flushUpdate`. Never regress the in-memory expiry below
    // a value already observed, so a racing flush carrying an older snapshot
    // can't shorten a freshly renewed lease. Storage remains authoritative via
    // `renewSessionLeaseSubtree`.
    const current = this._record.leaseExpiresAt;
    if (current !== undefined && current >= expiresAt) return;
    this._record = { ...this._record, leaseExpiresAt: expiresAt };
  }

  /**
   * Reject every outstanding `waitForIdle()` waiter with `reason`. Drains
   * `_idleWaiters` via each waiter's own `cleanup` so subscribers and
   * timers are properly disposed. Idempotent.
   */
  private _rejectIdleWaiters(reason: unknown): void {
    if (this._idleWaiters.size === 0) return;
    const waiters = Array.from(this._idleWaiters);
    this._idleWaiters.clear();
    for (const w of waiters) {
      w.cleanup();
      w.reject(reason);
    }
  }

  /**
   * Synchronous teardown for the thread subscription on close/evict/delete.
   * Unsubscribes, marks the subscription closed, and rejects every outstanding entry in
   * `_runCompletionPromises` so awaiters don't hang on a dead subscription.
   * The drain loop's `for-await` exits naturally once `unsubscribe()` wakes it.
   */
  private _tearDownThreadSubscription(reason: unknown): void {
    if (this._threadSubscriptionClosed) return;
    this._threadSubscriptionClosed = true;
    try {
      this._threadSubscription?.unsubscribe();
    } catch {
      // Best-effort — subscription may already be done.
    }
    for (const [, entry] of this._runCompletionPromises) {
      entry.reject(reason);
    }
    this._runCompletionPromises.clear();
  }

  /** @internal — accessor for the Harness when it needs the owner id back. */
  get _internalOwnerId(): string {
    return this._ownerId;
  }

  /** @internal — accessor for the Harness when it needs the record version. */
  get _internalRecordVersion(): number {
    return this._record.version;
  }

  /** @internal — accessor for the Harness when it needs the storage handle. */
  get _internalStorage(): HarnessStorage {
    return this._storage;
  }
}

type JsonValue = null | boolean | number | string | JsonValue[] | { [key: string]: JsonValue };

function compactJsonObject<T extends Record<string, unknown>>(value: T): Record<string, unknown> {
  return Object.fromEntries(Object.entries(value).filter(([, entry]) => entry !== undefined));
}

function getOwnRecordValue<T>(record: Record<string, T> | undefined, key: string): T | undefined {
  if (!record || !Object.prototype.hasOwnProperty.call(record, key)) return undefined;
  return record[key];
}

function publicErrorProjectionToError(error: { code: string; message: string }): Error {
  const projected = new Error(error.message);
  projected.name = error.code;
  (projected as Error & { code: string }).code = error.code;
  return projected;
}

function cloneAttachmentMetadata(metadata: Record<string, JsonValue>): Record<string, JsonValue> {
  return JSON.parse(JSON.stringify(metadata)) as Record<string, JsonValue>;
}

function clonePersistedAttachment(attachment: PersistedAttachment): PersistedAttachment {
  return JSON.parse(JSON.stringify(attachment)) as PersistedAttachment;
}

function clonePersistedRequestContext(input: PersistedRequestContextInput): PersistedRequestContextInput {
  return JSON.parse(JSON.stringify(input)) as PersistedRequestContextInput;
}

class QueueRecoveryPendingError extends HarnessError {
  readonly code = 'harness.queue_recovery_pending';
  readonly retryAt: number;

  constructor(retryAt: number) {
    super('queued turn was accepted by the signal runtime and is awaiting durable terminal result evidence');
    this.name = 'harness.queue_recovery_pending';
    this.retryAt = retryAt;
  }
}

class QueueRecoveryStaleError extends HarnessError {
  readonly code = 'harness.queue_recovery_stale';

  constructor() {
    super('queued turn was accepted by the signal runtime but no live run or durable terminal result is available');
    this.name = 'harness.queue_recovery_stale';
  }
}

class QueueResumeRecoveryStaleError extends HarnessError {
  readonly code = 'harness.queue_resume_recovery_stale';

  constructor() {
    super('queued turn resume was marked in flight but no terminal queue result is available');
    this.name = 'harness.queue_resume_recovery_stale';
  }
}

class QueuePostRunFinalizationPendingError extends HarnessError {
  readonly code = 'harness.queue_post_run_finalization_pending';
  readonly retryAt: number;
  readonly cause: unknown;

  constructor(retryAt: number, cause: unknown) {
    super('queued turn completed and is waiting for post-run finalization to persist');
    this.name = 'harness.queue_post_run_finalization_pending';
    this.retryAt = retryAt;
    this.cause = cause;
  }
}

function throwIfAborted(signal: AbortSignal | undefined, path: string): void {
  if (!signal?.aborted) return;
  throw signal.reason ?? new HarnessValidationError(path, 'operation aborted');
}

function delay(ms: number, signal?: AbortSignal): Promise<void> {
  throwIfAborted(signal, 'delay()');
  return new Promise((resolve, reject) => {
    let timer: ReturnType<typeof setTimeout>;
    const onAbort = () => {
      clearTimeout(timer);
      reject(signal?.reason ?? new HarnessValidationError('delay()', 'operation aborted'));
    };
    timer = setTimeout(() => {
      signal?.removeEventListener('abort', onAbort);
      resolve();
    }, ms);
    signal?.addEventListener('abort', onAbort, { once: true });
  });
}

function createDeferred<T>(): Deferred<T> {
  let resolve!: (value: T) => void;
  let reject!: (reason: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}
