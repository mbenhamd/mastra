/**
 * Harness v1 — top-level entry point.
 *
 * See HARNESS_V1_SPEC.md §4 for the full surface. This module currently
 * implements the local Harness shell:
 *
 *   - `new Harness(config)` validates modes/agents and binds storage.
 *   - `harness.session(opts)` finds-or-creates sessions per §5.3, acquiring
 *     the durable lease and hydrating from `HarnessStorage`.
 *   - `harness.closeSession`, `harness.deleteSession`, `harness.listSessions`,
 *     and `harness.shutdown` handle local lifecycle paths.
 *   - `harness.threads.*` composes with MemoryStorage for thread CRUD/settings.
 *   - `harness.models.*` exposes the static model catalog and auth-status
 *     resolver.
 *
 * Known remaining gaps are deliberately visible here: production server routes,
 * remote SDKs, full channel routing, wakeup producers/completion, and
 * acceptance evidence live in follow-up Harness v1 lanes.
 */

import { createHash, randomUUID } from 'node:crypto';

import type { Agent } from '../../agent';
import { Mastra } from '../../mastra';
import { MCPServerBase } from '../../mcp';
import type {
  PermissionRules,
  SessionGrants,
  SessionRecord,
  SessionSummary,
  TokenUsage,
  HarnessStorage,
  AttachmentSource,
  AttachmentRecord,
  AttachmentSemanticMetadata,
  JsonValue,
  HarnessRowErrorCode,
  ChannelActionReceipt,
  ChannelActionToken,
  ChannelDiagnosticsRows,
  ChannelInboxItem,
  ChannelOutboxEnqueueOptions,
  ChannelOutboxItem,
  ChannelProviderDeliveryReceipt,
  AgentSignalResultStatus,
  OperationAdmissionTombstone,
  QueueAdmissionReceipt,
  HarnessRuntimeDependencyRefs,
  SubtreeSessionLeaseResult,
  ChannelBinding,
  PersistedRequestContextInput,
} from '../../storage/domains/harness';
import {
  HarnessStorageAttachmentInUseError,
  HarnessStorageChannelInboxClaimConflictError,
  HarnessStorageChannelOutboxClaimConflictError,
  HarnessStorageLeaseConflictError,
  HarnessStorageParentSessionUnavailableError,
  HarnessStorageSessionEventReplayUnsupportedError,
  HarnessStorageSessionNotFoundError,
  HarnessStorageThreadDeleteFenceConflictError,
  HarnessStorageThreadDeleteFenceUnsupportedError,
  HarnessStorageVersionConflictError,
} from '../../storage/domains/harness';
import type { MemoryStorage } from '../../storage/domains/memory/base';

import { InMemoryStore } from '../../storage/mock';
import type { Workspace } from '../../workspace';

import { HarnessChannelRegistry } from './channel-registry';
import {
  HarnessAttachmentInUseError,
  HarnessAttachmentUnavailableError,
  HarnessConfigError,
  HarnessLiveSessionLimitError,
  HarnessModelNotFoundError,
  HarnessOverrideConflictError,
  HarnessQueueFullError,
  HarnessRuntimeDriftError,
  HarnessSessionClosedError,
  HarnessSessionClosingError,
  harnessSessionClosingError,
  HarnessSessionConflictError,
  HarnessSessionCorruptError,
  HarnessSessionDeleteBlockedError,
  type HarnessSessionDeleteBlocker,
  HarnessSessionDeletedError,
  HarnessSessionLockedError,
  HarnessSessionNotFoundError,
  HarnessStorageError,
  HarnessThreadNotFoundError,
  HarnessValidationError,
  HarnessWorkspaceProviderMismatchError,
} from './errors';
import { sha256CanonicalJson } from './canonical-json';
import { EventEmitter, projectHarnessPublicError } from './events';
import type { HarnessEvent, HarnessEventListener, HarnessEventUnsubscribe } from './events';
import { Session } from './session';
import type {
  AttachmentDeleteOptions,
  AttachmentRef,
  AttachmentUploadOptions,
  ChannelOutboxDispatchOptions,
  ChannelOutboxDispatchResult,
  HarnessChannelDiagnostics,
  HarnessChannelDiagnosticsOptions,
  HarnessChannelBinding,
  HarnessChannelConfig,
  HarnessChannelRouteContext,
  HarnessChannelTransportRequest,
  HarnessChannelInboundResult,
  ChannelIngressContext,
  HarnessConfig,
  HarnessFileConfig,
  HarnessMode,
  HarnessQueueBackpressurePolicy,
  HarnessSkill,
  HarnessSkillActionMetadata,
  HarnessSkillActionPermissionHints,
  HarnessSkillActionShortcut,
  ModelAuthStatus,
  ModelInfo,
  PermissionPolicy,
  SessionListOptions,
  SessionDeleteOptions,
  SessionLoadByIdOptions,
  SessionResolveOptions,
  ShutdownOptions,
  SubagentDefinition,
  ThreadCloneOptions,
  ThreadCreateOptions,
  ThreadDeleteOptions,
  ThreadGetOptions,
  ThreadGetSettingOptions,
  ThreadGetSettingsOptions,
  ThreadListOptions,
  ThreadListResult,
  ThreadRecord,
  ThreadRenameOptions,
  ThreadSetSettingsOptions,
  ToolCategory,
} from './types';
import { WorkspaceRegistry } from './workspace-registry';

const DEFAULT_LEASE_TTL_MS = 30_000;
const DEFAULT_LEASE_RENEW_MS = 10_000;
const DEFAULT_LOCK_WAIT_MS = 5_000;
const DEFAULT_IDLE_TIMEOUT_MS = 2 * 60 * 60 * 1_000;
const DEFAULT_MAX_QUEUE_DEPTH = 100;
const DEFAULT_CLOSE_TIMEOUT_MS = 30_000;
const MAX_CLOSE_TIMEOUT_MS = 2_147_483_647;
// Bounded best-effort budget for draining a victim's buffered session-record /
// token-usage flush chain during pressure/idle eviction (§5.4). Caps the hot
// path so a slow/stuck storage write can never block eviction indefinitely.
const EVICTION_FLUSH_DRAIN_BUDGET_MS = 5_000;
const DEFAULT_SUBAGENT_MAX_DEPTH = 1;
const DEFAULT_GOAL_MAX_TURNS = 50;
const DEFAULT_PERMISSION_POLICY: PermissionPolicy = 'ask';
const DEFAULT_CHANNEL_OUTBOX_CLAIM_TTL_MS = 30_000;
const DEFAULT_CHANNEL_OUTBOX_BATCH_SIZE = 10;
const DEFAULT_CHANNEL_OUTBOX_MAX_ATTEMPTS = 3;
const CHANNEL_DIAGNOSTICS_DEFAULT_LIMIT = 50;
const CHANNEL_DIAGNOSTICS_MAX_DESCENDANT_DEPTH = 32;
const CHANNEL_DIAGNOSTICS_MAX_VISIBLE_SESSIONS = 256;

type CloseTreeNode = {
  record: SessionRecord;
  depth: number;
  live?: Session;
  leaseAcquired: boolean;
};

function cloneHarnessSkill(skill: HarnessSkill): HarnessSkill {
  return {
    ...skill,
    ...(skill.action ? { action: cloneHarnessSkillActionMetadata(skill.action) } : {}),
    ...(skill.metadata ? { metadata: cloneSkillMetadata(skill.metadata, new WeakMap()) } : {}),
  };
}

function cloneHarnessSkillActionMetadata(action: HarnessSkillActionMetadata): HarnessSkillActionMetadata {
  return {
    ...action,
    ...(action.shortcuts ? { shortcuts: action.shortcuts.map(cloneHarnessSkillActionShortcut) } : {}),
    ...(action.inputSchema ? { inputSchema: cloneSkillMetadata(action.inputSchema, new WeakMap()) } : {}),
    ...(action.outputSchema ? { outputSchema: cloneSkillMetadata(action.outputSchema, new WeakMap()) } : {}),
    ...(action.artifactTypes ? { artifactTypes: [...action.artifactTypes] } : {}),
    ...(action.permissions ? { permissions: cloneHarnessSkillActionPermissionHints(action.permissions) } : {}),
  };
}

function cloneHarnessSkillActionShortcut(shortcut: HarnessSkillActionShortcut): HarnessSkillActionShortcut {
  return {
    ...shortcut,
    ...(shortcut.keys ? { keys: [...shortcut.keys] } : {}),
  };
}

function cloneHarnessSkillActionPermissionHints(
  permissions: HarnessSkillActionPermissionHints,
): HarnessSkillActionPermissionHints {
  return {
    ...(permissions.tools ? { tools: [...permissions.tools] } : {}),
    ...(permissions.fileScopes ? { fileScopes: [...permissions.fileScopes] } : {}),
    ...(permissions.networkScopes ? { networkScopes: [...permissions.networkScopes] } : {}),
    ...(permissions.mcpScopes ? { mcpScopes: [...permissions.mcpScopes] } : {}),
  };
}

function cloneSkillMetadata(
  metadata: Record<string, unknown>,
  seen: WeakMap<object, unknown>,
): Record<string, unknown> {
  const existing = seen.get(metadata);
  if (existing && typeof existing === 'object' && !Array.isArray(existing)) {
    return existing as Record<string, unknown>;
  }
  const clone: Record<string, unknown> = {};
  seen.set(metadata, clone);
  for (const [key, value] of Object.entries(metadata)) {
    clone[key] = cloneSkillMetadataValue(value, seen);
  }
  return clone;
}

function cloneSkillMetadataValue(value: unknown, seen: WeakMap<object, unknown>): unknown {
  if (Array.isArray(value)) {
    const existing = seen.get(value);
    if (Array.isArray(existing)) return existing;
    const clone: unknown[] = [];
    seen.set(value, clone);
    for (const child of value) {
      clone.push(cloneSkillMetadataValue(child, seen));
    }
    return clone;
  }
  if (value && typeof value === 'object') {
    const prototype = Object.getPrototypeOf(value);
    if (prototype === Object.prototype || prototype === null) {
      return cloneSkillMetadata(value as Record<string, unknown>, seen);
    }
  }
  return value;
}

function isPlainSkillMetadata(value: unknown): value is Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return false;
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}

function hasOnlyCloneableSkillMetadataValues(value: unknown, seen: WeakSet<object>): boolean {
  if (typeof value === 'function') return false;
  if (Array.isArray(value)) {
    if (seen.has(value)) return true;
    seen.add(value);
    const supported = value.every(child => hasOnlyCloneableSkillMetadataValues(child, seen));
    seen.delete(value);
    return supported;
  }
  if (value && typeof value === 'object') {
    if (!isPlainSkillMetadata(value)) return false;
    if (seen.has(value)) return true;
    seen.add(value);
    const supported = Object.values(value).every(child => hasOnlyCloneableSkillMetadataValues(child, seen));
    seen.delete(value);
    return supported;
  }
  return true;
}

function assertHarnessSkillActionMetadata(
  action: unknown,
  skillName: string,
): asserts action is HarnessSkillActionMetadata {
  if (!isPlainSkillMetadata(action)) {
    throw new HarnessConfigError('skills', `entry "${skillName}" action must be an object`);
  }
  const metadata = action as HarnessSkillActionMetadata;
  if (metadata.displayName !== undefined && typeof metadata.displayName !== 'string') {
    throw new HarnessConfigError('skills', `entry "${skillName}" action.displayName must be a string`);
  }
  if (metadata.icon !== undefined && typeof metadata.icon !== 'string') {
    throw new HarnessConfigError('skills', `entry "${skillName}" action.icon must be a string`);
  }
  if (metadata.shortcuts !== undefined) {
    if (!Array.isArray(metadata.shortcuts)) {
      throw new HarnessConfigError('skills', `entry "${skillName}" action.shortcuts must be an array`);
    }
    const ids = new Set<string>();
    for (const shortcut of metadata.shortcuts) {
      assertHarnessSkillActionShortcut(shortcut, skillName);
      if (ids.has(shortcut.id)) {
        throw new HarnessConfigError(
          'skills',
          `entry "${skillName}" action.shortcuts has duplicate id "${shortcut.id}"`,
        );
      }
      ids.add(shortcut.id);
    }
  }
  assertOptionalPlainActionSchema(metadata.inputSchema, skillName, 'inputSchema');
  assertOptionalPlainActionSchema(metadata.outputSchema, skillName, 'outputSchema');
  assertOptionalStringArray(metadata.artifactTypes, skillName, 'action.artifactTypes');
  if (metadata.permissions !== undefined) {
    assertHarnessSkillActionPermissionHints(metadata.permissions, skillName);
  }
}

function assertHarnessSkillActionShortcut(
  shortcut: unknown,
  skillName: string,
): asserts shortcut is HarnessSkillActionShortcut {
  if (!isPlainSkillMetadata(shortcut)) {
    throw new HarnessConfigError('skills', `entry "${skillName}" action.shortcuts entries must be objects`);
  }
  if (typeof shortcut.id !== 'string' || shortcut.id.length === 0) {
    throw new HarnessConfigError('skills', `entry "${skillName}" action.shortcuts entries must have a non-empty id`);
  }
  if (shortcut.label !== undefined && typeof shortcut.label !== 'string') {
    throw new HarnessConfigError(
      'skills',
      `entry "${skillName}" action.shortcuts["${shortcut.id}"].label must be a string`,
    );
  }
  assertOptionalStringArray(shortcut.keys, skillName, `action.shortcuts["${shortcut.id}"].keys`);
}

function assertOptionalPlainActionSchema(
  value: unknown,
  skillName: string,
  field: 'inputSchema' | 'outputSchema',
): void {
  if (value === undefined) return;
  if (!isPlainSkillMetadata(value)) {
    throw new HarnessConfigError('skills', `entry "${skillName}" action.${field} must be an object`);
  }
  if (!hasOnlyCloneableSkillMetadataValues(value, new WeakSet<object>())) {
    throw new HarnessConfigError(
      'skills',
      `entry "${skillName}" action.${field} must contain only primitives, arrays, and plain objects`,
    );
  }
}

function assertHarnessSkillActionPermissionHints(permissions: unknown, skillName: string): void {
  if (!isPlainSkillMetadata(permissions)) {
    throw new HarnessConfigError('skills', `entry "${skillName}" action.permissions must be an object`);
  }
  assertOptionalStringArray(permissions.tools, skillName, 'action.permissions.tools');
  assertOptionalStringArray(permissions.fileScopes, skillName, 'action.permissions.fileScopes');
  assertOptionalStringArray(permissions.networkScopes, skillName, 'action.permissions.networkScopes');
  assertOptionalStringArray(permissions.mcpScopes, skillName, 'action.permissions.mcpScopes');
}

function assertOptionalStringArray(value: unknown, skillName: string, field: string): void {
  if (value === undefined) return;
  if (!Array.isArray(value) || !value.every(item => typeof item === 'string' && item.length > 0)) {
    throw new HarnessConfigError('skills', `entry "${skillName}" ${field} must be an array of non-empty strings`);
  }
}

function assertAttachmentJsonValue(value: unknown, field: string, seen: WeakSet<object> = new WeakSet()): JsonValue {
  if (value === null || typeof value === 'string' || typeof value === 'boolean') return value;
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) {
      throw new HarnessValidationError(field, 'attachment JSON values must be finite numbers');
    }
    return value;
  }
  if (Array.isArray(value)) {
    if (seen.has(value)) throw new HarnessValidationError(field, 'attachment value must not contain cycles');
    seen.add(value);
    const out: JsonValue[] = [];
    try {
      for (let index = 0; index < value.length; index += 1) {
        if (!(index in value)) {
          throw new HarnessValidationError(`${field}[${index}]`, 'attachment arrays must not contain holes');
        }
        out.push(assertAttachmentJsonValue(value[index], `${field}[${index}]`, seen));
      }
    } finally {
      seen.delete(value);
    }
    return out;
  }
  if (value && typeof value === 'object' && isPlainSkillMetadata(value)) {
    if (seen.has(value)) throw new HarnessValidationError(field, 'attachment value must not contain cycles');
    seen.add(value);
    const out: Record<string, JsonValue> = {};
    try {
      for (const [key, child] of Object.entries(value)) {
        if (child !== undefined) out[key] = assertAttachmentJsonValue(child, `${field}.${key}`, seen);
      }
    } finally {
      seen.delete(value);
    }
    return out;
  }
  throw new HarnessValidationError(field, 'attachment value must be JSON-serialisable');
}

function assertAttachmentJsonRecord(value: unknown, field: string): Record<string, JsonValue> {
  const record = assertAttachmentJsonValue(value, field);
  if (record === null || typeof record !== 'object' || Array.isArray(record)) {
    throw new HarnessValidationError(field, 'attachment metadata must be a JSON object');
  }
  return record;
}

function canonicalAttachmentJson(value: JsonValue): string {
  if (value === null || typeof value !== 'object') return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(canonicalAttachmentJson).join(',')}]`;
  return `{${Object.keys(value)
    .sort()
    .map(key => `${JSON.stringify(key)}:${canonicalAttachmentJson(value[key]!)}`)
    .join(',')}}`;
}

function optionalAttachmentJsonMatches(current: JsonValue | undefined, next: JsonValue | undefined): boolean {
  if (current === undefined && next === undefined) return true;
  if (current === undefined || next === undefined) return false;
  return canonicalAttachmentJson(current) === canonicalAttachmentJson(next);
}

function attachmentSemanticMatches(current: AttachmentSemanticMetadata, next: AttachmentSemanticMetadata): boolean {
  return (
    current.kind === next.kind &&
    current.primitiveType === next.primitiveType &&
    current.elementType === next.elementType &&
    current.schemaId === next.schemaId &&
    optionalAttachmentJsonMatches(current.renderer as JsonValue | undefined, next.renderer as JsonValue | undefined) &&
    optionalAttachmentJsonMatches(current.metadata as JsonValue | undefined, next.metadata as JsonValue | undefined) &&
    optionalAttachmentJsonMatches(current.object as JsonValue | undefined, next.object as JsonValue | undefined)
  );
}

function attachmentSemanticFromRecord(record: AttachmentRecord): AttachmentSemanticMetadata {
  return {
    ...(record.kind ? { kind: record.kind } : {}),
    ...(record.primitiveType ? { primitiveType: record.primitiveType } : {}),
    ...(record.elementType ? { elementType: record.elementType } : {}),
    ...(record.renderer ? { renderer: record.renderer } : {}),
    ...(record.schemaId ? { schemaId: record.schemaId } : {}),
    ...(record.metadata ? { metadata: record.metadata } : {}),
    ...(record.object ? { object: record.object } : {}),
  };
}

function encodeAttachmentJson(value: JsonValue): Uint8Array {
  return new TextEncoder().encode(canonicalAttachmentJson(value));
}

function hasExternalSessionStorageOwner(metadata: unknown): boolean {
  return (
    !!metadata &&
    typeof metadata === 'object' &&
    !Array.isArray(metadata) &&
    (metadata as Record<string, unknown>)[EXTERNAL_SESSION_STORAGE_OWNER_METADATA_KEY] === true
  );
}

function hasHarnessThreadDeleteInProgress(metadata: unknown): boolean {
  return (
    !!metadata &&
    typeof metadata === 'object' &&
    !Array.isArray(metadata) &&
    (metadata as Record<string, unknown>)[HARNESS_THREAD_DELETE_IN_PROGRESS_METADATA_KEY] === true
  );
}

const boundHarnessesByMastra = new WeakMap<Mastra, Set<Harness>>();
const boundHarnessesByMemory = new WeakMap<object, Set<Harness>>();
const EXTERNAL_SESSION_STORAGE_OWNER_METADATA_KEY = '__mastraHarnessExternalSessionStorageOwner';
const HARNESS_THREAD_DELETE_IN_PROGRESS_METADATA_KEY = '__mastraHarnessThreadDeleteInProgress';
const HARNESS_INTERNAL_THREAD_METADATA_KEYS = new Set([
  EXTERNAL_SESSION_STORAGE_OWNER_METADATA_KEY,
  HARNESS_THREAD_DELETE_IN_PROGRESS_METADATA_KEY,
]);

function assertNoHarnessInternalThreadMetadata(metadata: unknown, callsite: string): void {
  if (!metadata || typeof metadata !== 'object' || Array.isArray(metadata)) return;
  for (const key of Object.keys(metadata)) {
    if (HARNESS_INTERNAL_THREAD_METADATA_KEYS.has(key)) {
      throw new HarnessConfigError(callsite, `metadata key "${key}" is reserved for Harness internals`);
    }
  }
}

function stripHarnessInternalThreadMetadata(
  metadata: Record<string, unknown> | undefined,
): Record<string, unknown> | undefined {
  if (!metadata) return undefined;
  const publicMetadata: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(metadata)) {
    if (!HARNESS_INTERNAL_THREAD_METADATA_KEYS.has(key)) publicMetadata[key] = value;
  }
  return Object.keys(publicMetadata).length > 0 ? publicMetadata : undefined;
}

function projectChannelDiagnostics(
  session: SessionRecord,
  bindings: HarnessChannelBinding[],
  rows: ChannelDiagnosticsRows,
  visibleSessionIds: string[],
  limit: number,
  visibleSessionIdsTruncated: boolean,
): HarnessChannelDiagnostics {
  const trim = <T>(items: T[]) => items.slice(0, limit);
  const visibleBindingIds = new Set<string>();
  for (const item of rows.inbox) if (item.bindingId !== undefined) visibleBindingIds.add(item.bindingId);
  for (const item of rows.actionTokens) visibleBindingIds.add(item.bindingId);
  for (const item of rows.actionReceipts) visibleBindingIds.add(item.bindingId);
  for (const item of rows.outbox) visibleBindingIds.add(item.bindingId);
  const truncated =
    visibleSessionIdsTruncated ||
    rows.inbox.length > limit ||
    rows.actionTokens.length > limit ||
    rows.actionReceipts.length > limit ||
    rows.outbox.length > limit;
  const now = Date.now();
  return {
    harnessName: session.harnessName,
    resourceId: session.resourceId,
    sessionId: session.id,
    visibleSessionIds,
    bindings: bindings.filter(binding => visibleBindingIds.has(binding.bindingId)),
    inbox: trim(rows.inbox).map(projectChannelInboxDiagnostic),
    actionTokens: trim(rows.actionTokens).map(token => projectChannelActionTokenDiagnostic(token, now)),
    actionReceipts: trim(rows.actionReceipts).map(projectChannelActionReceiptDiagnostic),
    outbox: trim(rows.outbox).map(projectChannelOutboxDiagnostic),
    limit,
    truncated,
    redacted: true,
  };
}

function resolveChannelDiagnosticsLimit(limit: number | undefined): number {
  if (limit === undefined || !Number.isFinite(limit)) return CHANNEL_DIAGNOSTICS_DEFAULT_LIMIT;
  return Math.min(Math.max(Math.trunc(limit), 1), CHANNEL_DIAGNOSTICS_DEFAULT_LIMIT);
}

function projectChannelLeaseDiagnostic(row: {
  attempts: number;
  claimExpiresAt?: number;
  nextAttemptAt?: number;
}): HarnessChannelDiagnostics['inbox'][number]['lease'] {
  return {
    attempts: row.attempts,
    ...(row.claimExpiresAt !== undefined ? { claimExpiresAt: row.claimExpiresAt } : {}),
    ...(row.nextAttemptAt !== undefined ? { nextAttemptAt: row.nextAttemptAt } : {}),
  };
}

// §13.3f.1: storage rows record BARE `HarnessRowErrorCode` values, but bare
// codes MUST NOT cross the v1 wire. Every public DTO / event that surfaces a
// row's cause projects the bare code through this total function into a
// fully-namespaced `harness.*` wire code (+ `reason` when the spec table
// collapses several bare codes onto one envelope). Unknown codes fall back to
// `harness.internal` carrying the bare code as `reason` so the wire never sees
// a bare literal.
// Total function over BOTH the impl's `HarnessRowErrorCode` union and the
// §4.5d bare codes named in the §13.3f.1 table (some of which the impl does not
// yet store but may flow through a thrown cause). Keyed by `string` so a code
// outside the impl union still projects deterministically.
const ROW_ERROR_WIRE_PROJECTION: Record<string, { code: string; reason?: string }> = {
  // impl `HarnessRowErrorCode` union
  session_closed: { code: 'harness.session_closed' },
  session_closing: { code: 'harness.session_closing' },
  session_deleted: { code: 'harness.session_deleted' },
  live_session_limit: { code: 'harness.live_session_limit' },
  session_locked: { code: 'harness.session_locked' },
  queue_full: { code: 'harness.queue_full' },
  override_conflict: { code: 'harness.override_conflict' },
  channel_binding_closed: { code: 'harness.channel_binding_closed' },
  channel_payload_conflict: { code: 'harness.channel_action_conflict', reason: 'channel_payload_conflict' },
  delivery_operation_unavailable: {
    code: 'harness.channel_delivery_unavailable',
    reason: 'delivery_operation_unavailable',
  },
  provider_payload_invalid: { code: 'harness.channel_delivery_unavailable', reason: 'provider_payload_invalid' },
  worker_unavailable: { code: 'harness.worker_unavailable' },
  unknown: { code: 'harness.internal', reason: 'unknown' },
  // §13.3f.1 table bare codes (§4.5d) — collapse onto shared envelopes.
  platform_unlinked: { code: 'harness.channel_binding_closed', reason: 'platform_unlinked' },
  operator_closed: { code: 'harness.channel_binding_closed', reason: 'operator_closed' },
  pending_state_corrupt: { code: 'harness.session_corrupt', reason: 'pending_state_corrupt' },
  tool_surface_unrehydratable: { code: 'harness.session_corrupt', reason: 'tool_surface_unrehydratable' },
  runtime_dependency_drifted: { code: 'harness.runtime_drift' },
};

function projectRowErrorCode(code: string): { code: string; reason?: string } {
  return ROW_ERROR_WIRE_PROJECTION[code as HarnessRowErrorCode] ?? { code: 'harness.internal', reason: code };
}

function projectChannelError(
  error: { code: HarnessRowErrorCode; retryable?: boolean } | undefined,
): { code: string; reason?: string; retryable?: boolean } | undefined {
  if (!error) return undefined;
  const projected = projectRowErrorCode(error.code);
  return {
    code: projected.code,
    ...(projected.reason !== undefined ? { reason: projected.reason } : {}),
    ...(error.retryable !== undefined ? { retryable: error.retryable } : {}),
  };
}

function projectChannelInboxDiagnostic(item: ChannelInboxItem): HarnessChannelDiagnostics['inbox'][number] {
  return {
    id: item.id,
    status: item.status,
    channelId: item.channelId,
    providerId: item.providerId,
    ...(item.bindingId !== undefined ? { bindingId: item.bindingId } : {}),
    admissionId: item.admissionId,
    ...(item.resourceId !== undefined ? { resourceId: item.resourceId } : {}),
    ...(item.threadId !== undefined ? { threadId: item.threadId } : {}),
    ...(item.sessionId !== undefined ? { sessionId: item.sessionId } : {}),
    ...(item.runId !== undefined ? { runId: item.runId } : {}),
    ...(item.signalId !== undefined ? { signalId: item.signalId } : {}),
    ...(item.queuedItemId !== undefined ? { queuedItemId: item.queuedItemId } : {}),
    externalMessageId: item.externalMessageId,
    ...(item.delivery !== undefined ? { delivery: item.delivery } : {}),
    ...(item.mode !== undefined ? { mode: item.mode } : {}),
    ...(item.model !== undefined ? { model: item.model } : {}),
    receivedAt: item.receivedAt,
    ...(item.admittedAt !== undefined ? { admittedAt: item.admittedAt } : {}),
    ...(item.acceptedAt !== undefined ? { acceptedAt: item.acceptedAt } : {}),
    ...(item.queuedAt !== undefined ? { queuedAt: item.queuedAt } : {}),
    ...(item.failedAt !== undefined ? { failedAt: item.failedAt } : {}),
    ...(item.deadAt !== undefined ? { deadAt: item.deadAt } : {}),
    updatedAt: item.updatedAt,
    lease: projectChannelLeaseDiagnostic(item),
    ...(item.lastError !== undefined ? { lastError: projectChannelError(item.lastError) } : {}),
  };
}

function projectChannelActionTokenDiagnostic(
  token: ChannelActionToken,
  now: number,
): HarnessChannelDiagnostics['actionTokens'][number] {
  const status =
    token.revokedAt !== undefined
      ? 'revoked'
      : token.expiresAt !== undefined && token.expiresAt <= now
        ? 'expired'
        : 'active';
  return {
    actionTokenId: token.actionTokenId,
    status,
    channelId: token.channelId,
    providerId: token.providerId,
    bindingId: token.bindingId,
    bindingGeneration: token.bindingGeneration,
    resourceId: token.resourceId,
    owningSessionId: token.owningSessionId,
    itemId: token.itemId,
    kind: token.kind,
    runId: token.runId,
    pendingRequestedAt: token.pendingRequestedAt,
    ...(token.expiresAt !== undefined ? { expiresAt: token.expiresAt } : {}),
    ...(token.revokedAt !== undefined ? { revokedAt: token.revokedAt } : {}),
    ...(token.revokedReason !== undefined ? { revokedReason: token.revokedReason } : {}),
    createdAt: token.createdAt,
    updatedAt: token.updatedAt,
  };
}

function projectChannelActionReceiptDiagnostic(
  receipt: ChannelActionReceipt,
): HarnessChannelDiagnostics['actionReceipts'][number] {
  return {
    id: receipt.id,
    status: receipt.status,
    channelId: receipt.channelId,
    providerId: receipt.providerId,
    actionTokenId: receipt.actionTokenId,
    actionId: receipt.actionId,
    bindingId: receipt.bindingId,
    bindingGeneration: receipt.bindingGeneration,
    resourceId: receipt.resourceId,
    owningSessionId: receipt.owningSessionId,
    itemId: receipt.itemId,
    kind: receipt.kind,
    runId: receipt.runId,
    pendingRequestedAt: receipt.pendingRequestedAt,
    ...(receipt.conflictReason !== undefined ? { conflictReason: receipt.conflictReason } : {}),
    ...(receipt.acceptedAt !== undefined ? { acceptedAt: receipt.acceptedAt } : {}),
    ...(receipt.appliedAt !== undefined ? { appliedAt: receipt.appliedAt } : {}),
    ...(receipt.failedAt !== undefined ? { failedAt: receipt.failedAt } : {}),
    ...(receipt.deadAt !== undefined ? { deadAt: receipt.deadAt } : {}),
    createdAt: receipt.createdAt,
    updatedAt: receipt.updatedAt,
    lease: projectChannelLeaseDiagnostic(receipt),
    ...(receipt.lastError !== undefined ? { lastError: projectChannelError(receipt.lastError) } : {}),
  };
}

function projectChannelOutboxDiagnostic(item: ChannelOutboxItem): HarnessChannelDiagnostics['outbox'][number] {
  return {
    id: item.id,
    status: item.status,
    channelId: item.channelId,
    providerId: item.providerId,
    bindingId: item.bindingId,
    bindingGeneration: item.bindingGeneration,
    resourceId: item.resourceId,
    threadId: item.threadId,
    ...(item.sessionId !== undefined ? { sessionId: item.sessionId } : {}),
    ...(item.owningSessionId !== undefined ? { owningSessionId: item.owningSessionId } : {}),
    ...(item.source !== undefined
      ? { source: { kind: item.source.kind, ...(item.source.id ? { id: item.source.id } : {}) } }
      : {}),
    kind: item.kind,
    operationKind: item.operationKind,
    ...(item.operationName !== undefined ? { operationName: item.operationName } : {}),
    deliverySemantics: item.deliverySemantics,
    ...(item.sentAt !== undefined ? { sentAt: item.sentAt } : {}),
    ...(item.failedAt !== undefined ? { failedAt: item.failedAt } : {}),
    ...(item.deadAt !== undefined ? { deadAt: item.deadAt } : {}),
    createdAt: item.createdAt,
    updatedAt: item.updatedAt,
    lease: projectChannelLeaseDiagnostic(item),
    ...(item.lastError !== undefined ? { lastError: projectChannelError(item.lastError) } : {}),
  };
}

export class Harness {
  /** Process-scoped owner id used as the lease holder for all sessions. */
  readonly ownerId: string;

  /**
   * The Mastra instance backing this harness. Either supplied at
   * construction (`new Harness({ mastra })`), built internally from
   * inline `agents`/`storage`, or installed by `__registerMastra(parent)`
   * when the harness is registered as a child of a parent Mastra.
   *
   * Reads of agents and storage always go through this. Tools and
   * primitives that need the harness's Mastra (TUI, server) read it via
   * `harness.mastra`.
   */
  private _mastra?: Mastra;
  private _harnessName = 'default';
  private _registeredHarnessName?: string;
  private _hasAdoptedSessions = false;
  private _guardPreboundDefaultNamespace = false;
  private readonly _storageOverride?: HarnessStorage;
  private readonly _modesById: Map<string, HarnessMode>;
  private readonly _defaultModeId?: string;
  private readonly _liveSessions = new Map<string, Session>();
  private readonly _leaseTtlMs: number;
  private readonly _lockMode: 'fail' | 'wait';
  private readonly _lockWaitMs: number;
  private readonly _lockRenewMs: number;
  private readonly _maxLive: number;
  private readonly _idleTimeoutMs: number;
  private _leaseRenewalTimer?: ReturnType<typeof setInterval>;
  private readonly _leaseRenewingSessionIds = new Set<string>();
  // §14.2 channel-ingress recovery worker scheduler. Starts when the harness is
  // bound with at least one channel (so crashed inbox rows recover even with no
  // live session); stopped on shutdown. Reentrancy-guarded.
  private _channelInboxRecoveryTimer?: ReturnType<typeof setInterval>;
  private _channelInboxRecoveryRunning = false;
  private readonly _maxQueueDepth: number;
  private readonly _queueBackpressure: HarnessQueueBackpressurePolicy;
  /** §10.5: when false, skip persisting transient streaming deltas (text_delta / subagent_text_delta). */
  private readonly _persistTransientStreamingEvents: boolean;
  private readonly _closeTimeoutMs: number;
  private readonly _fileConfig: Readonly<HarnessFileConfig>;
  private readonly _subagentTypes: ReadonlyMap<string, SubagentDefinition>;
  private readonly _subagentMaxDepth: number;
  private readonly _goalDefaults: { defaultJudgeModel?: string; defaultMaxTurns: number };
  private readonly _defaultPermissionPolicy: PermissionPolicy;
  private readonly _toolCategoryResolver?: (toolName: string) => ToolCategory | null;
  private readonly _modelCatalog: ReadonlyMap<string, ModelInfo>;
  private readonly _modelAuthStatusResolver?: (modelId: string) => ModelAuthStatus | Promise<ModelAuthStatus>;
  private readonly _codeSkills: ReadonlyMap<string, HarnessSkill>;
  private readonly _channelRegistry: HarnessChannelRegistry;
  private readonly _runtimeCompatibilityGeneration?: string;
  private readonly _emitter = new EventEmitter();
  /** Per-session unsubscribers so harness-level subscribers see session events too. */
  private readonly _sessionEventBridges = new Map<string, HarnessEventUnsubscribe>();
  /** In-process close de-dupe by any session id currently covered by a close tree. */
  private readonly _closePromises = new Map<string, Promise<void>>();
  private readonly _shutdownEvictedSessionIds = new Set<string>();
  /** Workspace registry — owns lifecycle across `shared`/`per-resource`/`per-session`. */
  readonly _workspaceRegistry: WorkspaceRegistry;
  /** Snapshot of the workspace kind for fast read paths. `undefined` when not configured. */
  readonly _workspaceKind?: 'shared' | 'per-resource' | 'per-session';
  private readonly _workspaceEager: boolean;

  private _initialized = false;
  private _initPromise?: Promise<void>;
  private _shutdown = false;

  constructor(config: HarnessConfig) {
    this.ownerId = `harness-${randomUUID()}`;
    const runtimeCompatibilityGeneration = config.runtimeCompatibilityGeneration;
    if (
      runtimeCompatibilityGeneration !== undefined &&
      (typeof runtimeCompatibilityGeneration !== 'string' || runtimeCompatibilityGeneration.trim().length === 0)
    ) {
      throw new HarnessConfigError('runtimeCompatibilityGeneration', 'must be a non-empty string when provided');
    }
    this._runtimeCompatibilityGeneration = runtimeCompatibilityGeneration?.trim();
    this._leaseTtlMs = config.sessions?.lockTtlMs ?? DEFAULT_LEASE_TTL_MS;
    if (!Number.isInteger(this._leaseTtlMs) || this._leaseTtlMs < 1) {
      throw new HarnessConfigError('sessions.lockTtlMs', 'must be a positive integer');
    }
    const lockMode = config.sessions?.lockMode ?? 'fail';
    if (lockMode !== 'fail' && lockMode !== 'wait') {
      throw new HarnessConfigError(
        'sessions.lockMode',
        'must be "fail" or "wait" ("steal" is reserved and not yet implemented)',
      );
    }
    this._lockMode = lockMode;
    // §10.5: default true (persist all events — upstream-safe, backs storage SSE replay).
    this._persistTransientStreamingEvents = config.sessions?.persistTransientStreamingEvents ?? true;
    this._lockRenewMs = config.sessions?.lockRenewMs ?? DEFAULT_LEASE_RENEW_MS;
    if (!Number.isInteger(this._lockRenewMs) || this._lockRenewMs < 1 || this._lockRenewMs >= this._leaseTtlMs) {
      throw new HarnessConfigError('sessions.lockRenewMs', 'must be a positive integer less than lockTtlMs');
    }
    this._lockWaitMs = config.sessions?.lockWaitMs ?? DEFAULT_LOCK_WAIT_MS;
    if (!Number.isInteger(this._lockWaitMs) || this._lockWaitMs < 0) {
      throw new HarnessConfigError('sessions.lockWaitMs', 'must be a non-negative integer');
    }
    this._maxLive = config.sessions?.maxLive ?? Number.POSITIVE_INFINITY;
    if (this._maxLive < 1 || (Number.isFinite(this._maxLive) && !Number.isInteger(this._maxLive))) {
      throw new HarnessConfigError('sessions.maxLive', 'must be a positive integer or Infinity');
    }
    this._idleTimeoutMs = config.sessions?.idleTimeoutMs ?? DEFAULT_IDLE_TIMEOUT_MS;
    if (!Number.isInteger(this._idleTimeoutMs) || this._idleTimeoutMs < 1) {
      throw new HarnessConfigError('sessions.idleTimeoutMs', 'must be a positive integer');
    }
    this._storageOverride = config.sessions?.storage;
    this._maxQueueDepth = config.sessions?.maxQueueDepth ?? DEFAULT_MAX_QUEUE_DEPTH;
    if (this._maxQueueDepth < 1) {
      throw new HarnessConfigError('sessions.maxQueueDepth', 'must be a positive integer');
    }
    this._queueBackpressure = config.sessions?.queueBackpressure ?? 'reject';
    if (this._queueBackpressure !== 'reject' && this._queueBackpressure !== 'drop-oldest') {
      throw new HarnessConfigError('sessions.queueBackpressure', 'must be "reject" or "drop-oldest"');
    }
    this._closeTimeoutMs = config.sessions?.closeTimeoutMs ?? DEFAULT_CLOSE_TIMEOUT_MS;
    if (
      !Number.isInteger(this._closeTimeoutMs) ||
      this._closeTimeoutMs < 1 ||
      this._closeTimeoutMs > MAX_CLOSE_TIMEOUT_MS
    ) {
      throw new HarnessConfigError(
        'sessions.closeTimeoutMs',
        `must be a positive integer no greater than ${MAX_CLOSE_TIMEOUT_MS}`,
      );
    }
    const normalizedFileConfig: HarnessFileConfig = {
      ...(config.files ?? {}),
      ...(config.files?.allowedUrlMimeTypes
        ? { allowedUrlMimeTypes: Object.freeze([...config.files.allowedUrlMimeTypes]) }
        : {}),
    };
    if (
      normalizedFileConfig.allowPrivateNetworkUrls !== undefined &&
      typeof normalizedFileConfig.allowPrivateNetworkUrls !== 'boolean'
    ) {
      throw new HarnessConfigError('files.allowPrivateNetworkUrls', 'must be a boolean');
    }
    for (const [key, value] of Object.entries(normalizedFileConfig)) {
      if (key === 'allowPrivateNetworkUrls' || key === 'allowedUrlMimeTypes') continue;
      if (value !== undefined && (typeof value !== 'number' || !Number.isInteger(value) || value < 0)) {
        throw new HarnessConfigError(`files.${key}`, 'must be a non-negative integer');
      }
    }
    if (
      normalizedFileConfig.allowedUrlMimeTypes !== undefined &&
      (!Array.isArray(normalizedFileConfig.allowedUrlMimeTypes) ||
        normalizedFileConfig.allowedUrlMimeTypes.some(value => typeof value !== 'string' || value.length === 0))
    ) {
      throw new HarnessConfigError('files.allowedUrlMimeTypes', 'must be an array of non-empty strings');
    }
    this._fileConfig = Object.freeze(normalizedFileConfig);

    // Subagent registry. Shape validation up front (uniqueness, mutual
    // exclusion of tool overlays); agent-existence resolution happens at
    // _bindMastra so it matches how modes are validated.
    const subagentTypes = new Map<string, SubagentDefinition>();
    if (config.subagents) {
      for (const [agentType, def] of Object.entries(config.subagents.types ?? {})) {
        if (typeof def?.agentId !== 'string' || def.agentId.length === 0) {
          throw new HarnessConfigError(`subagents.types["${agentType}"].agentId`, 'is required');
        }
        if (typeof def.description !== 'string' || def.description.length === 0) {
          throw new HarnessConfigError(`subagents.types["${agentType}"].description`, 'is required');
        }
        subagentTypes.set(agentType, def);
      }
      this._subagentMaxDepth = config.subagents.maxDepth ?? DEFAULT_SUBAGENT_MAX_DEPTH;
      if (this._subagentMaxDepth < 1) {
        throw new HarnessConfigError('subagents.maxDepth', 'must be a positive integer');
      }
    } else {
      this._subagentMaxDepth = DEFAULT_SUBAGENT_MAX_DEPTH;
    }
    this._subagentTypes = subagentTypes;

    // Goal-loop defaults (§4.7). Optional; resolved per-call at setGoal().
    const goalsCfg = config.goals;
    if (goalsCfg?.defaultMaxTurns !== undefined && goalsCfg.defaultMaxTurns < 1) {
      throw new HarnessConfigError('goals.defaultMaxTurns', 'must be a positive integer');
    }
    this._goalDefaults = {
      ...(goalsCfg?.defaultJudgeModel !== undefined ? { defaultJudgeModel: goalsCfg.defaultJudgeModel } : {}),
      defaultMaxTurns: goalsCfg?.defaultMaxTurns ?? DEFAULT_GOAL_MAX_TURNS,
    };

    // Permission gate config (§4.2e).
    if (
      config.defaultPermissionPolicy !== undefined &&
      config.defaultPermissionPolicy !== 'allow' &&
      config.defaultPermissionPolicy !== 'ask' &&
      config.defaultPermissionPolicy !== 'deny'
    ) {
      throw new HarnessConfigError(
        'defaultPermissionPolicy',
        `must be one of 'allow' | 'ask' | 'deny' (received: ${JSON.stringify(config.defaultPermissionPolicy)})`,
      );
    }
    if (config.toolCategoryResolver !== undefined && typeof config.toolCategoryResolver !== 'function') {
      throw new HarnessConfigError('toolCategoryResolver', 'must be a function');
    }
    if (
      config.toolCategories !== undefined &&
      (typeof config.toolCategories !== 'object' ||
        config.toolCategories === null ||
        Array.isArray(config.toolCategories))
    ) {
      throw new HarnessConfigError('toolCategories', 'must be a Record<string, ToolCategory>');
    }
    this._defaultPermissionPolicy = config.defaultPermissionPolicy ?? DEFAULT_PERMISSION_POLICY;
    // `toolCategoryResolver` is primary; `toolCategories` is sugar that
    // desugars to `(name) => toolCategories[name] ?? null`. When both are
    // provided the resolver wins (§9.1 sugar contract).
    if (config.toolCategoryResolver) {
      this._toolCategoryResolver = config.toolCategoryResolver;
    } else if (config.toolCategories) {
      const map = config.toolCategories;
      this._toolCategoryResolver = (name: string) => map[name] ?? null;
    } else {
      this._toolCategoryResolver = undefined;
    }

    // Model catalog (§9). Static list of `ModelInfo`; ids must be unique
    // within the catalog. The catalog is independent of modes — modes may
    // reference models outside the catalog, and the catalog may include
    // models not bound to any mode. Pure UX surface.
    const catalog = new Map<string, ModelInfo>();
    if (config.models) {
      if (!Array.isArray(config.models)) {
        throw new HarnessConfigError('models', 'must be an array of ModelInfo');
      }
      for (const entry of config.models) {
        if (!entry || typeof entry.id !== 'string' || entry.id.length === 0) {
          throw new HarnessConfigError('models', 'every entry must have a non-empty string `id`');
        }
        if (typeof entry.providerId !== 'string' || entry.providerId.length === 0) {
          throw new HarnessConfigError('models', `entry "${entry.id}" must have a non-empty string \`providerId\``);
        }
        if (catalog.has(entry.id)) {
          throw new HarnessConfigError('models', `duplicate model id "${entry.id}"`);
        }
        catalog.set(entry.id, entry);
      }
    }
    this._modelCatalog = catalog;
    this._modelAuthStatusResolver = config.modelAuthStatusResolver;

    // Code-registered skills (§4.6 / §9). Static deployment catalog; session
    // workspace skills are layered after these and lose on name conflicts.
    const codeSkills = new Map<string, HarnessSkill>();
    if (config.skills !== undefined) {
      if (!Array.isArray(config.skills)) {
        throw new HarnessConfigError('skills', 'must be an array of HarnessSkill');
      }
      for (const entry of config.skills) {
        if (!entry || typeof entry.name !== 'string' || entry.name.length === 0) {
          throw new HarnessConfigError('skills', 'every entry must have a non-empty string `name`');
        }
        if (typeof entry.description !== 'string') {
          throw new HarnessConfigError('skills', `entry "${entry.name}" must have a string \`description\``);
        }
        if (typeof entry.instructions !== 'string') {
          throw new HarnessConfigError('skills', `entry "${entry.name}" must have a string \`instructions\``);
        }
        if (entry.category !== undefined && typeof entry.category !== 'string') {
          throw new HarnessConfigError('skills', `entry "${entry.name}" must have a string \`category\``);
        }
        if (entry.filePath !== undefined && (typeof entry.filePath !== 'string' || entry.filePath.length === 0)) {
          throw new HarnessConfigError('skills', `entry "${entry.name}" must have a non-empty string \`filePath\``);
        }
        if (entry.action !== undefined) {
          assertHarnessSkillActionMetadata(entry.action, entry.name);
        }
        if (entry.metadata !== undefined && !isPlainSkillMetadata(entry.metadata)) {
          throw new HarnessConfigError('skills', `entry "${entry.name}" must have object \`metadata\``);
        }
        if (
          entry.metadata !== undefined &&
          !hasOnlyCloneableSkillMetadataValues(entry.metadata, new WeakSet<object>())
        ) {
          throw new HarnessConfigError(
            'skills',
            `entry "${entry.name}" metadata must contain only primitives, arrays, and plain objects`,
          );
        }
        if (codeSkills.has(entry.name)) {
          throw new HarnessConfigError('skills', `duplicate skill name "${entry.name}"`);
        }
        codeSkills.set(entry.name, cloneHarnessSkill(entry));
      }
    }
    this._codeSkills = codeSkills;

    this._channelRegistry = new HarnessChannelRegistry(config.channels);

    // Workspace (§2.7). Three ownership models; registry handles lifecycle.
    // Cross-checks against the subagent registry happen below.
    this._workspaceKind = config.workspace?.kind;
    this._workspaceEager = Boolean(config.workspace?.eager);
    // §2.7/§10.2: workspace lifecycle/error transitions are NOT public
    // HarnessEventV1 events. No `onNotice` is wired here, so they stay off the
    // public `subscribe`/SSE stream; provisioning failures still throw.
    this._workspaceRegistry = new WorkspaceRegistry({
      config: config.workspace,
    });

    // Subagent `workspace: 'fresh'` is only valid under `per-session`. Validate
    // at config time so misconfigurations don't reach the runtime spawn path.
    if (this._workspaceKind !== 'per-session') {
      for (const [agentType, def] of subagentTypes) {
        if (def.workspace === 'fresh') {
          throw new HarnessConfigError(
            `subagents.types["${agentType}"].workspace`,
            `"fresh" requires harness workspace kind "per-session" (current: "${this._workspaceKind ?? 'unconfigured'}")`,
          );
        }
      }
    }

    // Validate mode shape (uniqueness, tools/additionalTools mutual
    // exclusion, transitionsTo resolution) up front. Agent-existence
    // validation happens once a Mastra is bound — either here (if the
    // caller supplied one) or in __registerMastra.
    this._modesById = new Map();
    for (const mode of config.modes ?? []) {
      if (this._modesById.has(mode.id)) {
        throw new HarnessConfigError(`modes`, `duplicate mode id "${mode.id}"`);
      }
      if (mode.tools && mode.additionalTools) {
        throw new HarnessConfigError(
          `modes[${mode.id}]`,
          `cannot set both "tools" and "additionalTools" — choose replace OR augment`,
        );
      }
      this._modesById.set(mode.id, mode);
    }
    for (const mode of this._modesById.values()) {
      if (mode.transitionsTo && !this._modesById.has(mode.transitionsTo)) {
        throw new HarnessConfigError(
          `modes[${mode.id}].transitionsTo`,
          `references unknown mode "${mode.transitionsTo}"`,
        );
      }
    }

    if (config.defaultModeId !== undefined) {
      if (!this._modesById.has(config.defaultModeId)) {
        throw new HarnessConfigError(`defaultModeId`, `references unknown mode "${config.defaultModeId}"`);
      }
      this._defaultModeId = config.defaultModeId;
    } else if (this._modesById.size > 0) {
      throw new HarnessConfigError(`defaultModeId`, `must be set when "modes" is non-empty`);
    }

    // Resolve the Mastra binding. Three shapes:
    //   1. Caller passed a pre-built Mastra
    //   2. Caller passed inline agents (and optionally storage) — we build
    //      our own Mastra so the harness is fully self-contained. If no
    //      storage was supplied we default to InMemoryStore so that both
    //      the harness storage domain *and* the memory domain (used by
    //      thread CRUD) are available without the caller having to wire
    //      a composite by hand.
    //   3. Neither — defer; a parent Mastra will install itself via
    //      __registerMastra during its own construction.
    if (config.mastra) {
      this._bindMastra(config.mastra);
    } else if (config.agents !== undefined || config.storage !== undefined) {
      if (this._channelRegistry.hasPending()) {
        throw new HarnessConfigError(
          'channels',
          'channel bindings require a Mastra with channel providers; pass `mastra` or register the harness on a parent Mastra',
        );
      }
      const storage = config.storage ?? new InMemoryStore();
      const internal = new Mastra({
        agents: config.agents,
        storage,
      });
      this._bindMastra(internal);
    }
    // Otherwise: stay unbound. session() will throw HarnessConfigError
    // with a clear message until the parent Mastra registers.
  }

  /**
   * The Mastra instance powering this harness. Throws if the harness has
   * not been bound to a Mastra yet (i.e., it was constructed with no
   * `mastra` / `agents` / `storage` and has not been registered onto a
   * parent Mastra). Once bound, the reference is stable for the harness's
   * lifetime.
   */
  get mastra(): Mastra {
    if (!this._mastra) {
      throw new HarnessConfigError(
        'mastra',
        'harness is not yet bound to a Mastra — pass `mastra`/`agents`/`storage` at construction or register it on a parent Mastra',
      );
    }
    return this._mastra;
  }

  /**
   * Mastra lifecycle readiness hook. Validates that the harness is bound and
   * materializes eager shared workspace dependencies before server routes are
   * admitted.
   */
  async init(): Promise<void> {
    if (this._shutdown) {
      throw new HarnessConfigError('shutdown', 'harness cannot be initialized after shutdown');
    }
    if (this._initialized) return;
    if (this._initPromise) return this._initPromise;

    this._initPromise = this._initOnce();
    try {
      await this._initPromise;
    } catch (error) {
      this._initPromise = undefined;
      throw error;
    }
  }

  private async _initOnce(): Promise<void> {
    // Accessor intentionally provides the existing bound-Mastra error shape.
    this.mastra;

    let acquiredSharedWorkspace = false;
    if (this._workspaceKind === 'shared' && this._workspaceEager) {
      try {
        await this._workspaceRegistry.acquireShared();
      } catch (error) {
        if (this._shutdown) {
          throw new HarnessConfigError('shutdown', 'harness cannot be initialized after shutdown');
        }
        throw error;
      }
      acquiredSharedWorkspace = true;
    }

    if (this._shutdown) {
      if (acquiredSharedWorkspace) {
        await this._workspaceRegistry.destroyShared();
      }
      throw new HarnessConfigError('shutdown', 'harness cannot be initialized after shutdown');
    }

    this._initialized = true;
  }

  /**
   * @internal — called by `Mastra` during its own construction when this
   * harness is registered under `harnesses.<name>`. Idempotent for the
   * same parent; throws if called twice with different parents.
   */
  __registerMastra(mastra: Mastra, harnessName?: string): void {
    if (this._mastra && this._mastra !== mastra) {
      throw new HarnessConfigError('mastra', 'harness is already bound to a different Mastra instance');
    }

    if (harnessName !== undefined) {
      if (this._registeredHarnessName !== undefined && harnessName !== this._registeredHarnessName) {
        throw new HarnessConfigError('mastra', 'harness is already registered under a different harness name');
      }
      if (this._registeredHarnessName === undefined && harnessName !== this._harnessName && this._hasAdoptedSessions) {
        throw new HarnessConfigError(
          'mastra',
          'harness already has sessions under the default harness name and cannot be renamed',
        );
      }
    }

    if (this._mastra === mastra) {
      if (harnessName !== undefined) {
        const previousHarnessName = this._harnessName;
        const previousRegisteredHarnessName = this._registeredHarnessName;
        const previousGuardPreboundDefaultNamespace = this._guardPreboundDefaultNamespace;
        if (this._registeredHarnessName === undefined && this._harnessName === 'default' && harnessName !== 'default') {
          this._guardPreboundDefaultNamespace = true;
        }
        this._harnessName = harnessName;
        this._registeredHarnessName = harnessName;
        try {
          this._channelRegistry.bind(mastra, this._harnessName);
        } catch (err) {
          this._harnessName = previousHarnessName;
          this._registeredHarnessName = previousRegisteredHarnessName;
          this._guardPreboundDefaultNamespace = previousGuardPreboundDefaultNamespace;
          this._channelRegistry.bind(mastra, previousHarnessName);
          throw err;
        }
      }
      return;
    }

    const previousHarnessName = this._harnessName;
    const previousRegisteredHarnessName = this._registeredHarnessName;
    const previousGuardPreboundDefaultNamespace = this._guardPreboundDefaultNamespace;
    if (harnessName !== undefined) {
      this._harnessName = harnessName;
      this._registeredHarnessName = harnessName;
    }
    try {
      this._bindMastra(mastra);
    } catch (err) {
      this._harnessName = previousHarnessName;
      this._registeredHarnessName = previousRegisteredHarnessName;
      this._guardPreboundDefaultNamespace = previousGuardPreboundDefaultNamespace;
      throw err;
    }
  }

  /**
   * Validate every mode's `agentId` against the Mastra's agent registry
   * and stash the binding for runtime use.
   */
  private _bindMastra(mastra: Mastra): void {
    for (const mode of this._modesById.values()) {
      let agent: Agent | undefined;
      try {
        agent = mastra.getAgent(mode.agentId as never) as Agent | undefined;
      } catch {
        agent = undefined;
      }
      if (!agent) {
        throw new HarnessConfigError(
          `modes[${mode.id}].agentId`,
          `references unknown agent "${mode.agentId}" — Mastra has no such agent registered`,
        );
      }
    }
    for (const [agentType, def] of this._subagentTypes) {
      let agent: Agent | undefined;
      try {
        agent = mastra.getAgent(def.agentId as never) as Agent | undefined;
      } catch {
        agent = undefined;
      }
      if (!agent) {
        throw new HarnessConfigError(
          `subagents.types["${agentType}"].agentId`,
          `references unknown agent "${def.agentId}" — Mastra has no such agent registered`,
        );
      }
      if (def.modeId !== undefined && !this._modesById.has(def.modeId)) {
        throw new HarnessConfigError(
          `subagents.types["${agentType}"].modeId`,
          `references unknown mode "${def.modeId}"`,
        );
      }
    }
    let boundHarnesses = boundHarnessesByMastra.get(mastra);
    if (!boundHarnesses) {
      boundHarnesses = new Set();
      boundHarnessesByMastra.set(mastra, boundHarnesses);
    }
    boundHarnesses.add(this);
    this._mastra = mastra;
    try {
      this._channelRegistry.bind(mastra, this._harnessName);
      this._trackMemoryStorage(mastra.getStorage()?.stores?.memory);
      // §14.2: start the channel-ingress recovery worker now (not at session
      // adoption) so durable inbox rows from a prior process are recovered even
      // before any session goes live. No-op when no channels are configured.
      this._ensureChannelInboxRecoveryLoop();
    } catch (err) {
      boundHarnesses.delete(this);
      this._mastra = undefined;
      throw err;
    }
  }

  // -------------------------------------------------------------------------
  // Events — §10.
  // -------------------------------------------------------------------------

  /**
   * Subscribe to harness-scoped events. Includes lifecycle events for every
   * live session (session_created, session_closing, session_closed,
   * session_evicted) and any harness-level custom events. Per-session turn
   * events (agent_start, message_*, tool_*, suspension_*, mode_changed,
   * model_changed) are forwarded here so a single subscriber can render the
   * whole harness.
   * Per-turn events also include `message_*` (assistant text streaming) and
   * `tool_input_*` (model-side argument streaming) — see §10.2.
   *
   * Listeners see only future events.
   */
  subscribe(listener: HarnessEventListener): HarnessEventUnsubscribe {
    return this._emitter.subscribe(listener);
  }

  /** @internal — listener count for tests. */
  _internalListenerCount(): number {
    return this._emitter.listenerCount;
  }

  // -------------------------------------------------------------------------
  // Workspace — §2.7 / §4.1.
  // -------------------------------------------------------------------------

  /**
   * Returns the shared workspace when the harness is configured with
   * `kind: 'shared'`. For `per-resource` and `per-session`, returns
   * `undefined` — those models don't have a meaningful harness-level
   * workspace. Tools should always go through `session.getWorkspace()`.
   *
   * The shared workspace materialises lazily on first call (or eagerly
   * during `init()` when `eager: true`).
   */
  async getWorkspace(): Promise<Workspace | undefined> {
    if (this._workspaceKind !== 'shared') return undefined;
    return this._workspaceRegistry.acquireShared();
  }

  /**
   * Tear down the workspace bound to a given resource. Only valid under
   * `kind: 'per-resource'`. Throws `HarnessResourceWorkspaceInUseError` if any
   * sessions are still holding the workspace; callers are expected to
   * close them first.
   */
  async destroyResourceWorkspace(opts: { resourceId: string }): Promise<void> {
    if (this._workspaceKind !== 'per-resource') {
      throw new HarnessConfigError(
        'workspace.kind',
        `destroyResourceWorkspace requires kind: "per-resource" (current: "${this._workspaceKind ?? 'unconfigured'}")`,
      );
    }
    await this._workspaceRegistry.destroyResourceWorkspace(opts);
  }

  /** @internal — emit a harness-level event. Used by tests and helpers. */
  _emit(event: Parameters<EventEmitter['emit']>[0], overrides?: Parameters<EventEmitter['emit']>[1]): HarnessEvent {
    return this._emitter.emit(event, overrides);
  }

  /**
   * Resolve the backing `Agent` for a mode through the bound Mastra.
   * Throws if the harness is not yet bound.
   */
  getAgentForMode(modeId: string): Agent {
    const mode = this._modesById.get(modeId);
    if (!mode) {
      throw new HarnessConfigError('modeId', `unknown mode "${modeId}"`);
    }
    const mastra = this.mastra;
    let agent: Agent | undefined;
    try {
      agent = mastra.getAgent(mode.agentId as never) as Agent | undefined;
    } catch {
      agent = undefined;
    }
    if (!agent) {
      throw new HarnessConfigError(
        `modes[${mode.id}].agentId`,
        `references unknown agent "${mode.agentId}" — Mastra has no such agent registered`,
      );
    }
    return agent;
  }

  /** @internal — capture stable runtime ids for work that may be recovered after restart. */
  _runtimeDependenciesForMode(modeId: string, modelId?: string): HarnessRuntimeDependencyRefs {
    const mode = this._getMode(modeId);
    return {
      modeId,
      agentId: mode.agentId,
      ...(this._runtimeCompatibilityGeneration
        ? { runtimeCompatibilityGeneration: this._runtimeCompatibilityGeneration }
        : {}),
      ...(modelId ? { modelId } : {}),
      workspaceProviderId: this._workspaceDependencyId(),
    };
  }

  /** @internal — validate persisted runtime ids before recovered work invokes an agent. */
  _resolveAgentForRuntimeDependencies(
    refs: HarnessRuntimeDependencyRefs,
    context: string,
  ): { mode: HarnessMode; agent: Agent } {
    const mode = this._modesById.get(refs.modeId);
    if (!mode) {
      throw new HarnessRuntimeDriftError({ missingRefs: [{ kind: 'mode', ref: refs.modeId }], context });
    }
    if (refs.agentId !== undefined && refs.agentId !== mode.agentId) {
      throw new HarnessRuntimeDriftError({
        driftedRefs: [{ kind: 'agent', ref: refs.agentId }],
        context: `${context}: recorded agent "${refs.agentId}" for mode "${refs.modeId}", but the mode now points at agent "${mode.agentId}"`,
      });
    }
    const agentId = refs.agentId ?? mode.agentId;
    const mastra = this.mastra;
    let agent: Agent | undefined;
    try {
      agent = mastra.getAgent(agentId as never) as Agent | undefined;
    } catch {
      agent = undefined;
    }
    if (!agent) {
      throw new HarnessRuntimeDriftError({ missingRefs: [{ kind: 'agent', ref: agentId }], context });
    }
    if (
      refs.runtimeCompatibilityGeneration !== undefined &&
      refs.runtimeCompatibilityGeneration !== this._runtimeCompatibilityGeneration
    ) {
      throw new HarnessRuntimeDriftError({
        driftedRefs: [
          {
            kind: 'mode',
            ref: refs.modeId,
            expectedGeneration: refs.runtimeCompatibilityGeneration,
            actualGeneration: this._runtimeCompatibilityGeneration,
          },
        ],
        context,
      });
    }
    if ('workspaceProviderId' in refs && this._workspaceDependencyId() !== refs.workspaceProviderId) {
      throw new HarnessRuntimeDriftError({
        driftedRefs: [{ kind: 'workspace_provider', ref: refs.workspaceProviderId ?? 'unconfigured' }],
        context: `${context}: recorded workspace provider "${refs.workspaceProviderId ?? 'unconfigured'}", but the current workspace dependency is "${this._workspaceDependencyId() ?? 'unconfigured'}"`,
      });
    }
    return { mode, agent };
  }

  private _workspaceDependencyId(): string | null {
    if (this._workspaceKind === undefined) return null;
    // §13.3f.1 runtime-drift identity — MUST be restart-stable. A shared workspace
    // is a single refcounted `workspace` with no provider/providerId (see the
    // `kind: 'shared'` config variant), so it has no per-owner identity; using the
    // process-local `ownerId` here produced a spurious `runtime_dependency_drifted`
    // on every cross-process resume. A constant token is stable AND still detects a
    // workspace-KIND change (shared → per-resource/per-session yields a providerId
    // that mismatches `'shared'`); a shared-Workspace-instance swap has no id to
    // detect either way.
    if (this._workspaceKind === 'shared') return 'shared';
    return this._workspaceRegistry.providerId ?? null;
  }

  /**
   * @internal — Session reads the subagent-type registry when wiring
   * the built-in `spawn_subagent` tool. Returns undefined for unknown
   * types so the tool can return a `HarnessValidationError`-shaped
   * payload rather than throwing through the agent stream.
   */
  _getSubagentType(agentType: string): SubagentDefinition | undefined {
    return this._subagentTypes.get(agentType);
  }

  /** @internal — Session reads this to render the `agentType` enum in the spawn tool's input schema. */
  _listSubagentTypeIds(): string[] {
    return Array.from(this._subagentTypes.keys());
  }

  /** @internal — Session merges static skills before workspace-discovered skills. */
  _listCodeSkills(): HarnessSkill[] {
    return Array.from(this._codeSkills.values()).map(cloneHarnessSkill);
  }

  /** @internal — Session resolves code-registered skills by name. */
  _getCodeSkill(ref: string): HarnessSkill | undefined {
    const byName = this._codeSkills.get(ref);
    if (byName) return cloneHarnessSkill(byName);
    return undefined;
  }

  /** @internal — Session reads registered MCP servers for read-only desktop catalogs. */
  _listMcpServers(): Array<[string, MCPServerBase]> {
    const servers = this.mastra.listMCPServers();
    if (!servers) return [];
    return Object.entries(servers);
  }

  /** @internal — Session resolves one registered MCP server by Mastra registration key. */
  _getMcpServer(key: string): MCPServerBase | undefined {
    const server = this.mastra.getMCPServer(key as never) as unknown;
    return server instanceof MCPServerBase ? server : undefined;
  }

  /** @internal — Session enforces the subagent depth cap inside the spawn tool. */
  _getSubagentMaxDepth(): number {
    return this._subagentMaxDepth;
  }

  /** @internal — Session reads the resolved mode for per-turn overlays. */
  _getMode(modeId: string): HarnessMode {
    const mode = this._modesById.get(modeId);
    if (!mode) {
      throw new HarnessConfigError('modeId', `unknown mode "${modeId}"`);
    }
    return mode;
  }

  /**
   * Enumerate every mode registered on this harness, in declaration order.
   *
   * Returned array is a fresh copy — callers may sort or filter without
   * affecting harness state. Used by TUIs to render a mode picker and by
   * scripts that need to discover what modes exist before opening a session.
   */
  listModes(): HarnessMode[] {
    return Array.from(this._modesById.values());
  }

  /**
   * Look up a single mode by id. Returns `undefined` if no mode with that id
   * is registered. For the throwing variant used during request resolution,
   * see the internal `_getMode` helper.
   */
  getMode(modeId: string): HarnessMode | undefined {
    return this._modesById.get(modeId);
  }

  /**
   * Enumerate Harness channel bindings after the harness is bound to Mastra.
   * Once parent registration completes, the returned durable ids include the
   * resolved harness namespace and are stable inputs for later route, ingress,
   * and outbox workers.
   */
  listChannelBindings(): HarnessChannelBinding[] {
    void this.mastra;
    return this._channelRegistry.list();
  }

  /**
   * Look up one registered Harness channel binding by `channelId`.
   */
  getChannelBinding(channelId: string): HarnessChannelBinding | undefined {
    void this.mastra;
    return this._channelRegistry.get(channelId);
  }

  /**
   * @internal §14.1 binding resolution for durable ingress. Applies the channel's
   * `ChannelIngressPolicy.resolveResource` to map the verified platform tuple to
   * a Harness `resourceId`/`threadId`/`sessionId`, then commits (or idempotently
   * reuses) the durable `ChannelBinding` via the storage primitive. Returns the
   * binding plus the resolved identity for the ingress route to admit a turn
   * (`harness.session(...)` + `signal`/`queue`). It does NOT create the session
   * (avoids an orphan session before the binding exists) — the route resolves the
   * session from the returned identity.
   */
  async _resolveChannelBindingForIngress(ctx: ChannelIngressContext): Promise<{
    binding: ChannelBinding;
    resolved: {
      resourceId: string;
      threadId: string;
      sessionId: string;
      mode: ChannelBinding['mode'];
      admission?: { delivery?: 'message' | 'queue'; mode?: string; model?: string };
    };
  }> {
    const storage = this._requireStorage('channels.resolveBinding()');
    const config = this._channelRegistry.getConfig(ctx.channelId);
    if (config === undefined) {
      throw new HarnessConfigError(`channels["${ctx.channelId}"]`, 'no channel is registered under this id');
    }
    const resolved = await config.ingress.resolveResource(ctx);
    // §14.1: an explicit owning session must come with its own thread — never
    // pair a caller-supplied sessionId with a freshly derived thread (mispairing
    // would split the conversation). The policy owns both, or neither.
    if (resolved.sessionId !== undefined && resolved.threadId === undefined) {
      throw new HarnessValidationError(
        `channels["${ctx.channelId}"].ingress.resolveResource`,
        'must return threadId when sessionId is provided',
      );
    }
    const threadId = resolved.threadId ?? deriveChannelThreadId(ctx, resolved.resourceId);
    const sessionId = resolved.sessionId ?? deriveChannelSessionId(resolved.resourceId, threadId);
    // The durable binding mode is the §5.1h 3-value set; a 'custom' policy
    // resolution records as 'shared-resource' (its broadest logical-resource form).
    const mode: ChannelBinding['mode'] = resolved.mode === 'custom' ? 'shared-resource' : resolved.mode;
    const now = ctx.receivedAt;
    const candidate: ChannelBinding = {
      id: `binding-${randomUUID()}`,
      harnessName: this._harnessName,
      channelId: ctx.channelId,
      providerId: ctx.providerId,
      status: 'active',
      platform: ctx.platform,
      ...(ctx.externalTenantId !== undefined ? { externalTenantId: ctx.externalTenantId } : {}),
      ...(ctx.externalChannelId !== undefined ? { externalChannelId: ctx.externalChannelId } : {}),
      externalThreadId: ctx.externalThreadId,
      resourceId: resolved.resourceId,
      threadId,
      sessionId,
      mode,
      generation: 1,
      createdAt: now,
      updatedAt: now,
      lastInboundAt: now,
    };
    // Ordinary ingress never replaces — replacement is an explicit, policy-driven
    // fresh-generation operation, not a retry side effect.
    const result = await storage.resolveChannelBinding({ candidate });
    let binding = result.binding;
    // Idempotent reuse of an existing active binding: advance the activity marker
    // ONLY forward (a delayed/out-of-order older ingress must not regress it). The
    // forward-only merge runs atomically in storage against the authoritative
    // current row — a caller-side read-modify-write here would let a slower older
    // save clobber a newer concurrent one under same-binding ingress (§14.1).
    if (!result.created) {
      const touched = await storage.touchChannelBindingInbound({
        harnessName: binding.harnessName,
        bindingId: binding.id,
        at: now,
      });
      if (touched) binding = touched;
    }
    return {
      binding,
      resolved: {
        resourceId: binding.resourceId,
        threadId: binding.threadId,
        sessionId: binding.sessionId,
        mode: binding.mode,
        ...(resolved.admission !== undefined ? { admission: resolved.admission } : {}),
      },
    };
  }

  /**
   * §13.2/§14.2 channel webhook ingress bridge. The single core entrypoint a
   * transport route (`POST /harness/:name/channels/:channelId/inbound`) calls: it
   * resolves the registry route context, runs provider verification through the
   * channel adapter (`verifyInbound` owns signature auth), applies the §13.6
   * worker-readiness gate BEFORE any durable row is created, then admits through
   * {@link admitChannelInbound}. Returns a transport-neutral {@link HarnessChannelInboundResult}
   * the route maps to an HTTP status + §13.3 error envelope. Defaults to
   * `continueAdmission: false` (record-only ACK; a recovery worker finishes
   * admission) per the §14.2 provider-ACK/admission split.
   */
  async handleChannelInboundRequest(
    channelId: string,
    request: HarnessChannelTransportRequest,
    opts?: { continueAdmission?: boolean },
  ): Promise<HarnessChannelInboundResult> {
    // §13.2: an unregistered (harnessName, channelId) pair fails at the registry
    // boundary, before adapter verification or body parsing.
    const resolved = this._resolveChannelRouteContext(channelId, 'inbound');
    if (!resolved) {
      return {
        kind: 'not_found',
        httpStatus: 404,
        error: {
          code: 'harness.not_found',
          message: `channel "${channelId}" is not registered on harness "${this._harnessName}"`,
        },
      };
    }
    const { routeContext, adapter } = resolved;
    if (typeof adapter.verifyInbound !== 'function') {
      return {
        kind: 'verify_failed',
        httpStatus: 400,
        error: {
          code: 'harness.bad_request',
          message: `channel "${channelId}" adapter does not support inbound verification`,
        },
      };
    }

    // Provider signature verification + payload normalization (adapter-owned).
    let envelope;
    try {
      envelope = await adapter.verifyInbound(request, routeContext);
    } catch (err) {
      // The adapter owns the provider auth boundary; an unverified/invalid payload
      // is a permission failure. (400-vs-401 malformed-vs-signature refinement needs
      // a typed adapter error — deferred.)
      //
      // §13.3f.1: this route is PUBLIC/unauthenticated, so the raw adapter error
      // message (signing-secret config errors, expected-vs-actual signature hints,
      // stack-derived prose) MUST NOT be echoed to the anonymous caller. Surface a
      // fixed redacted message; the real cause stays local-only (logged here).
      this._mastra
        ?.getLogger?.()
        ?.error?.('[harness/v1] channel inbound verification failed', {
          harnessName: this._harnessName,
          channelId,
          providerId: routeContext.providerId,
          error: err instanceof Error ? { name: err.name, message: err.message, stack: err.stack } : err,
        });
      return {
        kind: 'verify_failed',
        httpStatus: 401,
        error: {
          code: 'harness.permission_denied',
          message: 'channel inbound verification failed',
        },
      };
    }

    // §13.6 worker-readiness gate — before creating/claiming a durable row.
    const readiness = this.channelWorkerReadiness({ channelId });
    if (!readiness.ready) {
      return {
        kind: 'not_ready',
        httpStatus: 503,
        error: {
          code: 'harness.worker_unavailable',
          message: `channel "${channelId}" ingress worker is unavailable`,
          retryable: true,
          details: {
            harnessName: this._harnessName,
            channelId,
            scope: 'channel_inbox',
            reason: readiness.reason,
          },
        },
      };
    }

    const ctx: ChannelIngressContext = {
      ...envelope,
      harnessName: this._harnessName,
      channelId,
      providerId: routeContext.providerId,
      // §14.7: the registry route context — not the adapter-returned envelope — is
      // authoritative for (harnessName, channelId, providerId, platform). Override
      // platform so a buggy adapter cannot persist a mismatched platform into the
      // durable binding / requestContext.channel.
      platform: routeContext.platform,
    };
    const result = await this.admitChannelInbound(ctx, {
      continueAdmission: opts?.continueAdmission ?? false,
    });
    if (result.conflict) {
      // §14.2: same idempotency key, different payload — not an exact-retry duplicate.
      return {
        kind: 'conflict',
        httpStatus: 409,
        error: {
          code: 'harness.channel_action_conflict',
          message: 'channel ingress payload conflict for an existing idempotency key',
          details: { harnessName: this._harnessName, channelId, inboxItemId: result.inboxItemId },
        },
      };
    }
    // §14.2: a non-terminal `received`/`admitted` row is an in-progress ACK (202);
    // a terminal/accepted/queued outcome or an idempotent duplicate is 200.
    const ackStatus: 200 | 202 = result.status === 'received' || result.status === 'admitted' ? 202 : 200;
    return {
      kind: 'ok',
      ackStatus,
      inboxItemId: result.inboxItemId,
      status: result.status,
      duplicate: result.duplicate,
      ...(result.binding !== undefined ? { binding: result.binding } : {}),
      ...(result.sessionId !== undefined ? { sessionId: result.sessionId } : {}),
      ...(result.queuedItemId !== undefined ? { queuedItemId: result.queuedItemId } : {}),
    };
  }

  /**
   * Resolve the §14.1 channel-level binding + config + provider into the
   * `HarnessChannelRouteContext` the adapter needs for `verifyInbound`/`verifyAction`.
   * Returns `undefined` when the `(harnessName, channelId)` pair is not registered
   * or the provider is unavailable — the caller maps that to a 404 at the registry
   * boundary (it never reaches adapter verification).
   */
  private _resolveChannelRouteContext(
    channelId: string,
    route: 'inbound' | 'action',
  ): { routeContext: HarnessChannelRouteContext; adapter: HarnessChannelConfig['adapter'] } | undefined {
    const binding = this.getChannelBinding(channelId);
    const config = this._channelRegistry.getConfig(channelId);
    if (!binding || !config) return undefined;
    const provider = this.mastra.getChannelProvider(binding.providerId);
    if (!provider || provider.id !== binding.platform) return undefined;
    return {
      routeContext: {
        harnessName: this._harnessName,
        channelId,
        providerId: binding.providerId,
        platform: binding.platform,
        provider,
        route,
      },
      adapter: config.adapter,
    };
  }

  /**
   * §14.2 durable ingress admission core. Idempotently records the inbound as a
   * durable `ChannelInboxItem`, resolves the §14.1 binding + owning session, and
   * admits the turn (queue delivery — signal delivery is a follow-up) carrying the
   * trusted `requestContext.channel` projection. This is the shared admission path
   * both the auto-mounted ingress route and the recovery worker call (worker-first
   * design): the route records the row + ACKs; the worker drives this to
   * completion. Status transitions received → admitted → queued bound the crash
   * replay window. `admissionId` (the inbox item id) makes provider/worker retries
   * idempotent.
   */
  async admitChannelInbound(
    ctx: ChannelIngressContext,
    opts?: { continueAdmission?: boolean },
  ): Promise<{
    inboxItemId: string;
    status: ChannelInboxItem['status'];
    binding?: ChannelBinding;
    sessionId?: string;
    queuedItemId?: string;
    duplicate: boolean;
    /**
     * §14.2: set when the same idempotency key arrived with a DIFFERENT payload —
     * an admission conflict, NOT an exact-retry duplicate. The transport bridge
     * maps this to `409 harness.channel_action_conflict`.
     */
    conflict?: boolean;
  }> {
    const storage = this._requireStorage('channels.admitInbound()');
    // `receivedAt` is the provider event time (durable record field). Claim TTL
    // and status-transition timestamps are real processing time — the claim
    // expiry is validated against the wall clock, not the event time.
    const receivedAt = ctx.receivedAt;
    const now = Date.now();
    // §14.2: idempotency is scoped by the full route + canonical conversation
    // identity (NOT externalMessageId alone — provider message ids can collide
    // across tenants/channels); payloadHash distinguishes exact retries from
    // same-key payload conflicts.
    const idempotencyKey = sha256CanonicalJson([
      ctx.harnessName,
      ctx.channelId,
      ctx.providerId,
      ctx.platform,
      normChannelExternalId(ctx.externalTenantId),
      normChannelExternalId(ctx.externalChannelId),
      ctx.externalThreadId,
      ctx.externalMessageId,
    ]);
    // §14.2: the payload hash must distinguish inbound messages that differ only by their
    // attachments, otherwise two distinct messages collide on the same idempotency key and one is
    // dropped as a false duplicate. We hash a STABLE identity projection of each provider file
    // (durable id + digest + name/type, never transient byte/metadata fields) and only widen the
    // hash when files are present, so the common no-file case stays byte-identical to before.
    // Full attachment normalization + durable persistence remains the §13.7 follow-up.
    const payloadHash = sha256CanonicalJson(
      ctx.files !== undefined && ctx.files.length > 0
        ? {
            content: ctx.content,
            files: ctx.files.map(f => ({
              attachmentId: f.attachmentId,
              resourceId: f.resourceId,
              ...(f.sha256 !== undefined ? { sha256: f.sha256 } : {}),
              ...(f.name !== undefined ? { name: f.name } : {}),
              ...(f.mimeType !== undefined ? { mimeType: f.mimeType } : {}),
            })),
          }
        : { content: ctx.content },
    );
    const inboxItemId = `inbox-${randomUUID()}`;
    const claimId = `inbox-claim-${randomUUID()}`;
    const created = await storage.createOrLoadChannelInboxItem(
      {
        id: inboxItemId,
        harnessName: this._harnessName,
        channelId: ctx.channelId,
        providerId: ctx.providerId,
        idempotencyKey,
        payloadHash,
        admissionId: inboxItemId,
        externalMessageId: ctx.externalMessageId,
        receivedAt,
        updatedAt: now,
        status: 'received',
        attempts: 0,
        // Preliminary channel context (no bindingId yet — binding resolves below).
        requestContext: buildChannelRequestContext(ctx),
        content: ctx.content,
        // Payload-hash idempotency now covers file identity (above). Attachment normalisation into
        // Harness-owned persisted refs remains a §13.7 follow-up; raw provider files are not yet
        // stored on the durable row.
        attachments: [],
      },
      { initialClaim: { claimId, now, claimTtlMs: DEFAULT_CHANNEL_OUTBOX_CLAIM_TTL_MS } },
    );
    const item = created.item;
    // §14.2: same idempotency key + a DIFFERENT payload is a conflict, not a
    // duplicate — surface it and do not admit.
    if (created.conflict) {
      this._emitter.emit({
        type: 'channel_ingress_failed',
        harnessName: this._harnessName,
        channelId: ctx.channelId,
        inboxItemId: item.id,
        externalMessageId: ctx.externalMessageId,
        error: { code: 'harness.channel_action_conflict', message: 'channel ingress payload conflict' },
      });
      return { inboxItemId: item.id, status: item.status, duplicate: true, conflict: true };
    }
    // Terminal row (queued/accepted/dead) — idempotent replay returns the prior
    // outcome without re-admitting or re-claiming. A `dead` row is a terminal
    // give-up; replaying it must not be mistaken for an in-progress duplicate.
    if (item.status === 'queued' || item.status === 'accepted' || item.status === 'dead') {
      return {
        inboxItemId: item.id,
        status: item.status,
        ...(item.sessionId !== undefined ? { sessionId: item.sessionId } : {}),
        ...(item.queuedItemId !== undefined ? { queuedItemId: item.queuedItemId } : {}),
        duplicate: true,
      };
    }
    // A non-terminal duplicate whose claim we did NOT take is being admitted by
    // another caller (concurrent provider retry / worker). Don't steal its claim
    // (that would claim-conflict and emit a false failure) — report in-progress.
    if (!created.claimed) {
      return {
        inboxItemId: item.id,
        status: item.status,
        ...(item.sessionId !== undefined ? { sessionId: item.sessionId } : {}),
        duplicate: true,
      };
    }
    if (!created.duplicate) {
      this._emitter.emit({
        type: 'channel_ingress_received',
        harnessName: this._harnessName,
        channelId: ctx.channelId,
        inboxItemId: item.id,
        externalMessageId: ctx.externalMessageId,
      });
    }
    // §14.2 record-only / ACK-after-received: a route may durably record the
    // 'received' row and acknowledge the provider immediately, leaving binding
    // resolution + admission to the recovery worker. Release our claim so the
    // worker can reclaim it on its next tick (the release is CAS-safe —
    // `updateChannelInboxItem` only honors a write under the matching claimId, so
    // it can never clear another owner's later claim).
    if (opts?.continueAdmission === false) {
      if (created.claimed) {
        try {
          await storage.updateChannelInboxItem(
            { ...item, claimId: undefined, claimExpiresAt: undefined, updatedAt: now },
            { claimId },
          );
        } catch (releaseErr) {
          if (!(releaseErr instanceof HarnessStorageChannelInboxClaimConflictError)) throw releaseErr;
        }
      }
      return { inboxItemId: item.id, status: item.status, duplicate: created.duplicate };
    }
    // The latest row state durably committed under our claim. The catch path
    // spreads THIS (not the stale pre-admission `item`) so a failure after the
    // `admitted` write preserves admissionHash / resolved ids / requestContext —
    // otherwise recovery would lose the persisted admission and wrongly re-run
    // policy (the row is recovery-complete once `admitted`).
    let persisted: ChannelInboxItem = item;
    try {
      const { binding, resolved } = await this._resolveChannelBindingForIngress(ctx);
      const session = await this.session({
        resourceId: resolved.resourceId,
        threadId: resolved.threadId,
        sessionId: resolved.sessionId,
      } as SessionResolveOptions);
      // §14.3: the trusted channel request-context now carries the resolved
      // bindingId (built from binding + envelope evidence, never caller input).
      const requestContext = buildChannelRequestContext(ctx, binding.id);
      // §14.2 step 7: the policy-selected admission payload (content + persisted
      // attachments + mode/model + trusted requestContext). Queue delivery only —
      // signal delivery is a documented follow-up; a policy that selects
      // `delivery: 'message'` still queues here until the signal path lands.
      // mode/model are written UNCONDITIONALLY (undefined clears any stale value
      // from a prior failed attempt) so the persisted payload always matches the
      // admissionHash, which omits absent fields.
      const admissionMode = resolved.admission?.mode;
      const admissionModel = resolved.admission?.model;
      const admissionHash = session._channelQueueAdmissionHash(
        { content: ctx.content, mode: admissionMode, model: admissionModel },
        item.attachments,
        requestContext,
      );
      // Persist the policy-selected fields + admissionHash BEFORE runtime admission
      // (spec step 7/8) and mark `admitted`, so a crash between admit and durable
      // acceptance is recoverable: the worker replays the SAME payload (validated
      // against this admissionHash) and never re-runs policy.
      const admittedRow: ChannelInboxItem = {
        ...item,
        status: 'admitted',
        bindingId: binding.id,
        resourceId: resolved.resourceId,
        threadId: resolved.threadId,
        sessionId: resolved.sessionId,
        delivery: 'queue',
        admissionHash,
        mode: admissionMode,
        model: admissionModel,
        requestContext,
        admittedAt: now,
        updatedAt: now,
        // Clear any failure metadata from a prior failed attempt being retried,
        // so a successful admission doesn't leave stale failure evidence.
        failedAt: undefined,
        deadAt: undefined,
        nextAttemptAt: undefined,
        lastError: undefined,
      };
      await storage.updateChannelInboxItem(admittedRow, { claimId });
      persisted = admittedRow;
      const admit = await session._admitChannelQueueTurn({
        content: ctx.content,
        admissionId: item.id,
        requestContext,
        attachments: item.attachments,
        expectedAdmissionHash: admissionHash,
        mode: admissionMode,
        model: admissionModel,
      });
      const queuedRow: ChannelInboxItem = {
        ...admittedRow,
        status: 'queued',
        queuedItemId: admit.queuedItemId,
        queuedAt: now,
        updatedAt: now,
      };
      await storage.updateChannelInboxItem(queuedRow, { claimId });
      persisted = queuedRow;
      this._emitter.emit({
        type: 'channel_ingress_admitted',
        harnessName: this._harnessName,
        channelId: ctx.channelId,
        inboxItemId: item.id,
        bindingId: binding.id,
        delivery: 'queue',
        queuedItemId: admit.queuedItemId,
      });
      return {
        inboxItemId: item.id,
        status: 'queued',
        binding,
        sessionId: resolved.sessionId,
        queuedItemId: admit.queuedItemId,
        duplicate: created.duplicate,
      };
    } catch (err) {
      const projected = projectHarnessPublicError(err);
      try {
        // §5.1: a failed/dead inbox row MUST carry durable failure evidence. The
        // row stores the bare `HarnessRowErrorCode` (projected to the namespaced
        // wire code elsewhere); the public channel_ingress_failed event below
        // already carries the projected code. `retryable: true` lets the recovery
        // worker re-attempt (a typed retry/dead taxonomy is a follow-up). Spreading
        // `persisted` preserves the recovery-complete admission (admissionHash,
        // resolved ids, requestContext) when the failure happened after `admitted`.
        await storage.updateChannelInboxItem(
          {
            ...persisted,
            status: 'failed',
            failedAt: now,
            updatedAt: now,
            lastError: { code: 'unknown', message: projected.message, retryable: true },
          },
          { claimId },
        );
      } catch {
        // Best-effort: the claim may already be lost; the worker reclaims and retries.
      }
      this._emitter.emit({
        type: 'channel_ingress_failed',
        harnessName: this._harnessName,
        channelId: ctx.channelId,
        inboxItemId: item.id,
        externalMessageId: ctx.externalMessageId,
        error: projected,
      });
      throw err;
    }
  }

  /**
   * §9 inbox recovery config for `channelId`, with spec defaults applied. When
   * `channelId` is omitted (an all-channels sweep) only the defaults apply —
   * per-channel inbox overrides require a per-channel worker tick (follow-up).
   */
  private _resolveInboxRecoveryConfig(channelId?: string): {
    maxAttempts: number;
    claimTtlMs: number;
    claimRenewMs: number;
    batchSize: number;
    retryBackoffMs: (attempt: number) => number;
  } {
    const inbox = channelId !== undefined ? this._channelRegistry.getConfig(channelId)?.inbox : undefined;
    const claimTtlMs = inbox?.claimTtlMs ?? 30_000;
    return {
      maxAttempts: inbox?.maxAttempts ?? 10,
      claimTtlMs,
      claimRenewMs: inbox?.claimRenewMs ?? Math.floor(claimTtlMs / 3),
      batchSize: inbox?.batchSize ?? 50,
      retryBackoffMs: inbox?.retryBackoffMs ?? defaultInboxRetryBackoffMs,
    };
  }

  /** §14 channel_ingress_admitted emit shared by the route and recovery worker. */
  private _emitChannelIngressAdmitted(row: ChannelInboxItem, queuedItemId: string, bindingId: string): void {
    this._emitter.emit({
      type: 'channel_ingress_admitted',
      harnessName: this._harnessName,
      channelId: row.channelId,
      inboxItemId: row.id,
      bindingId,
      delivery: 'queue',
      queuedItemId,
    });
  }

  /**
   * §13.6 worker-readiness gate for record-only channel ingress. Before a route
   * durably records a `received` row and ACKs the provider, it must confirm the
   * recovery worker that will later admit that row is actually running — otherwise
   * the row would be orphaned. This is a CHEAP synchronous check (no DB probe)
   * tied to the worker this harness runs:
   *   - `server_draining`     — the harness is shutting down
   *   - `storage_unavailable` — no harness storage is configured
   *   - `channel_not_configured` — no such channel id
   *   - `channel_not_bound`   — configured but not yet bound to a provider
   *   - `worker_not_started`  — the recovery loop has not started (e.g. not bound)
   * Maps to the §13.3f `harness.worker_unavailable` wire error at the route.
   */
  channelWorkerReadiness(opts: { channelId: string }): { ready: true } | { ready: false; reason: string } {
    if (this._shutdown) return { ready: false, reason: 'server_draining' };
    try {
      this._requireStorage('channelWorkerReadiness()');
    } catch {
      return { ready: false, reason: 'storage_unavailable' };
    }
    if (this._channelRegistry.getConfig(opts.channelId) === undefined) {
      return { ready: false, reason: 'channel_not_configured' };
    }
    if (this._channelRegistry.get(opts.channelId) === undefined) {
      return { ready: false, reason: 'channel_not_bound' };
    }
    if (this._channelInboxRecoveryTimer === undefined) {
      return { ready: false, reason: 'worker_not_started' };
    }
    return { ready: true };
  }

  /**
   * @internal §14.2 recovery worker — ONE reclaim+resume pass over non-terminal
   * channel inbox rows. Claims a batch and resumes each row from its first
   * incomplete step, discriminating by persisted `admissionHash`:
   *
   *  - `admissionHash` PRESENT → replay the persisted admission ONLY (policy
   *    already ran; §14.2 forbids re-running it). Rehydrate the owning session and
   *    re-admit with the stored payload + `expectedAdmissionHash`; the queue
   *    boundary de-dupes on `admissionId`, so a crash after queue-append but before
   *    the `queued` write recovers without a double turn.
   *  - `admissionHash` ABSENT → resume from binding resolution (policy may run):
   *    reconstruct the `ChannelIngressContext` from the durable row and run the
   *    same resolve → admitted → admit → queued sequence as {@link admitChannelInbound}.
   *    (The sequence is intentionally duplicated rather than extracted: the route
   *    rethrows on error while the worker applies attempts/backoff/dead-letter, so
   *    a shared helper would need mode flags. Keep both in lockstep with §14.2.)
   *
   * Per-error disposition follows the §14.2 taxonomy (`classifyChannelInboxFailure`):
   * retryable backpressure → `failed` + backoff → `dead` at `maxAttempts`;
   * session-closing → deadline-clamped retry then `dead`; closed/deleted/override/
   * unrecoverable → terminal `dead`. The claim is held across slow work by
   * `_withChannelInboxClaimHeartbeat` and released on failure so retries honor the
   * backoff. Rows at/over `maxAttempts` dead-letter for operator repair.
   */
  async recoverChannelInboxOnce(opts?: {
    channelId?: string;
    now?: number;
    batchSize?: number;
  }): Promise<{ claimed: number; queued: number; failed: number; dead: number }> {
    const storage = this._requireStorage('channels.recoverInbox()');
    const now = opts?.now ?? Date.now();
    const cfg = this._resolveInboxRecoveryConfig(opts?.channelId);
    const claimId = `inbox-worker-${randomUUID()}`;
    const batch = await storage.claimChannelInboxItems({
      harnessName: this._harnessName,
      ...(opts?.channelId !== undefined ? { channelId: opts.channelId } : {}),
      statuses: ['received', 'admitted', 'failed'],
      claimId,
      limit: opts?.batchSize ?? cfg.batchSize,
      now,
      claimTtlMs: cfg.claimTtlMs,
    });
    let queued = 0;
    let failed = 0;
    let dead = 0;
    for (const row of batch) {
      const outcome = await this._resumeClaimedChannelInboxRow(storage, row, claimId, now, cfg);
      if (outcome === 'queued') queued++;
      else if (outcome === 'failed') failed++;
      else if (outcome === 'dead') dead++;
    }
    return { claimed: batch.length, queued, failed, dead };
  }

  /** Resume a single claimed inbox row; owns its own failure persistence so the
   * caller never loses the latest committed admission on a throw. */
  private async _resumeClaimedChannelInboxRow(
    storage: HarnessStorage,
    row: ChannelInboxItem,
    claimId: string,
    now: number,
    cfg: {
      maxAttempts: number;
      claimTtlMs: number;
      claimRenewMs: number;
      retryBackoffMs: (attempt: number) => number;
    },
  ): Promise<'queued' | 'failed' | 'dead'> {
    // A row already at the attempt ceiling dead-letters without another attempt.
    if (row.attempts >= cfg.maxAttempts) {
      return this._failClaimedChannelInboxRow(storage, row, row, claimId, now, cfg, {
        message: `channel inbox row exceeded maxAttempts (${cfg.maxAttempts})`,
        forceDead: true,
      });
    }
    // The latest row state committed under our claim — the failure path spreads
    // THIS so admissionHash/resolved ids survive a post-admitted error (mirrors
    // admitChannelInbound's `persisted` invariant).
    let persisted = row;
    try {
      // Keep the claim alive across the (possibly slow: binding resolution,
      // session cold-start, queue admission) work so a long row doesn't lose its
      // claim mid-flight and false-conflict its own writes (§14.2 claim renewal).
      return await this._withChannelInboxClaimHeartbeat(
        storage,
        row,
        claimId,
        cfg.claimTtlMs,
        cfg.claimRenewMs,
        async (): Promise<'queued'> => {
          if (row.admissionHash !== undefined) {
        // REPLAY-ONLY: never re-run channel policy once admissionHash exists.
        if (
          row.resourceId === undefined ||
          row.threadId === undefined ||
          row.sessionId === undefined ||
          row.bindingId === undefined
        ) {
          throw new HarnessValidationError(
            'recoverChannelInboxOnce',
            'inbox row carries admissionHash but is missing resolved binding/resource/thread/session ids',
          );
        }
        const bindingId = row.bindingId;
        const session = await this.session({
          resourceId: row.resourceId,
          threadId: row.threadId,
          sessionId: row.sessionId,
        } as SessionResolveOptions);
        // The inbox state machine forbids a direct failed → queued transition; a
        // retried 'failed' row must pass back through 'admitted' first (clearing
        // its stale failure metadata). An already-'admitted' row goes straight to
        // 'queued'.
        let base = row;
        if (row.status === 'failed') {
          const readmittedRow: ChannelInboxItem = {
            ...row,
            status: 'admitted',
            admittedAt: row.admittedAt ?? now,
            updatedAt: now,
            failedAt: undefined,
            deadAt: undefined,
            nextAttemptAt: undefined,
            lastError: undefined,
          };
          await storage.updateChannelInboxItem(readmittedRow, { claimId });
          persisted = readmittedRow;
          base = readmittedRow;
        }
        const admit = await session._admitChannelQueueTurn({
          content: row.content,
          admissionId: row.id,
          requestContext: row.requestContext,
          attachments: row.attachments,
          expectedAdmissionHash: row.admissionHash,
          ...(row.mode !== undefined ? { mode: row.mode } : {}),
          ...(row.model !== undefined ? { model: row.model } : {}),
        });
        const queuedRow: ChannelInboxItem = {
          ...base,
          status: 'queued',
          queuedItemId: admit.queuedItemId,
          queuedAt: base.queuedAt ?? now,
          updatedAt: now,
          failedAt: undefined,
          deadAt: undefined,
          nextAttemptAt: undefined,
          lastError: undefined,
        };
        await storage.updateChannelInboxItem(queuedRow, { claimId });
        persisted = queuedRow;
        this._emitChannelIngressAdmitted(row, admit.queuedItemId, bindingId);
        return 'queued';
      }
      // FROM-SCRATCH: no admissionHash → resume from binding resolution. Keep this
      // sequence in lockstep with admitChannelInbound (§14.2 steps 5-9).
      const ctx = reconstructChannelIngressContext(row);
      const { binding, resolved } = await this._resolveChannelBindingForIngress(ctx);
      const session = await this.session({
        resourceId: resolved.resourceId,
        threadId: resolved.threadId,
        sessionId: resolved.sessionId,
      } as SessionResolveOptions);
      const requestContext = buildChannelRequestContext(ctx, binding.id);
      const admissionMode = resolved.admission?.mode;
      const admissionModel = resolved.admission?.model;
      const admissionHash = session._channelQueueAdmissionHash(
        { content: ctx.content, mode: admissionMode, model: admissionModel },
        row.attachments,
        requestContext,
      );
      const admittedRow: ChannelInboxItem = {
        ...row,
        status: 'admitted',
        bindingId: binding.id,
        resourceId: resolved.resourceId,
        threadId: resolved.threadId,
        sessionId: resolved.sessionId,
        delivery: 'queue',
        admissionHash,
        mode: admissionMode,
        model: admissionModel,
        requestContext,
        admittedAt: now,
        updatedAt: now,
        failedAt: undefined,
        deadAt: undefined,
        nextAttemptAt: undefined,
        lastError: undefined,
      };
      await storage.updateChannelInboxItem(admittedRow, { claimId });
      persisted = admittedRow;
      const admit = await session._admitChannelQueueTurn({
        content: ctx.content,
        admissionId: row.id,
        requestContext,
        attachments: row.attachments,
        expectedAdmissionHash: admissionHash,
        mode: admissionMode,
        model: admissionModel,
      });
      const queuedRow: ChannelInboxItem = {
        ...admittedRow,
        status: 'queued',
        queuedItemId: admit.queuedItemId,
        queuedAt: now,
        updatedAt: now,
      };
      await storage.updateChannelInboxItem(queuedRow, { claimId });
      persisted = queuedRow;
      this._emitChannelIngressAdmitted(row, admit.queuedItemId, binding.id);
      return 'queued';
        },
      );
    } catch (err) {
      return this._failClaimedChannelInboxRow(storage, row, persisted, claimId, now, cfg, { error: err });
    }
  }

  /** Persist a recovery failure: bump attempts, schedule the next attempt (or
   * dead-letter at the cap), preserve the recovery-complete admission, emit. */
  private async _failClaimedChannelInboxRow(
    storage: HarnessStorage,
    originalRow: ChannelInboxItem,
    persisted: ChannelInboxItem,
    claimId: string,
    now: number,
    cfg: { maxAttempts: number; retryBackoffMs: (attempt: number) => number },
    failure: { error?: unknown; message?: string; forceDead?: boolean },
  ): Promise<'failed' | 'dead'> {
    const projected = failure.error !== undefined ? projectHarnessPublicError(failure.error) : undefined;
    const message = projected?.message ?? failure.message ?? 'channel inbox recovery failed';
    const attempts = failure.forceDead ? originalRow.attempts : originalRow.attempts + 1;
    // §14.2 error taxonomy: classify the thrown error into the durable failure
    // shape (status / bare lastError.code / retryable / nextAttemptAt). The
    // entry-level `forceDead` path (maxAttempts exhausted on claim) keeps the row's
    // own last code rather than reclassifying.
    const classified = failure.forceDead
      ? { status: 'dead' as const, code: originalRow.lastError?.code ?? 'unknown', retryable: false, nextAttemptAt: undefined }
      : classifyChannelInboxFailure(failure.error, {
          attempts,
          maxAttempts: cfg.maxAttempts,
          now,
          retryBackoffMs: cfg.retryBackoffMs,
        });
    const dead = classified.status === 'dead';
    try {
      await storage.updateChannelInboxItem(
        {
          ...persisted,
          status: classified.status,
          attempts,
          updatedAt: now,
          // RELEASE the claim on failure so a retryable row reclaims at its
          // `nextAttemptAt` backoff rather than waiting out the full claim TTL.
          // (A KEEP write would preserve the live claim and block reclaim until
          // it expires; `claimId: undefined` is the storage release signal.)
          claimId: undefined,
          claimExpiresAt: undefined,
          ...(dead
            ? { deadAt: now, nextAttemptAt: undefined }
            : { failedAt: now, nextAttemptAt: classified.nextAttemptAt }),
          lastError: { code: classified.code, message, retryable: classified.retryable },
        },
        { claimId },
      );
    } catch (writeErr) {
      // A lost/expired claim is expected (another worker took the row, or the TTL
      // lapsed) — swallow and let the next tick reclaim. Any OTHER error (e.g. an
      // illegal transition) signals a bug and must surface, not be masked.
      if (!(writeErr instanceof HarnessStorageChannelInboxClaimConflictError)) {
        throw writeErr;
      }
    }
    this._emitter.emit({
      type: 'channel_ingress_failed',
      harnessName: this._harnessName,
      channelId: originalRow.channelId,
      inboxItemId: originalRow.id,
      externalMessageId: originalRow.externalMessageId,
      error: projected ?? { code: 'harness.internal', message },
    });
    return dead ? 'dead' : 'failed';
  }

  /**
   * Return read-only, redacted diagnostics for channel ledger rows visible to a
   * session. This method delegates to the storage read-only diagnostics contract
   * and never claims, retries, dispatches, or reconciles work.
   */
  async getChannelDiagnostics(opts: HarnessChannelDiagnosticsOptions): Promise<HarnessChannelDiagnostics | null> {
    const storage = this._requireStorage('getChannelDiagnostics()');
    const root = await storage.loadSession({ harnessName: this._harnessName, sessionId: opts.sessionId });
    if (!root || root.resourceId !== opts.resourceId) return null;

    const visibleSessions = await this._visibleChannelDiagnosticSessionIds(root);
    const limit = resolveChannelDiagnosticsLimit(opts.limit);
    const rows = await storage.listChannelDiagnosticsRows({
      harnessName: this._harnessName,
      resourceId: opts.resourceId,
      sessionIds: visibleSessions.sessionIds,
      limit: limit + 1,
    });
    return projectChannelDiagnostics(
      root,
      this.listChannelBindings(),
      rows,
      visibleSessions.sessionIds,
      limit,
      visibleSessions.truncated,
    );
  }

  channels = {
    diagnostics: (opts: HarnessChannelDiagnosticsOptions): Promise<HarnessChannelDiagnostics | null> =>
      this.getChannelDiagnostics(opts),

    enqueueOutbox: async (
      opts: ChannelOutboxEnqueueOptions,
    ): Promise<{
      outboxItemId: string;
      duplicate: boolean;
      conflict: boolean;
    }> => {
      const storage = this._requireStorage('channels.enqueueOutbox()');
      const { binding, config } = this._requireChannelRuntime(opts.channelId);
      const provider = this._requireChannelProvider(binding);
      if (opts.target.platform !== binding.platform) {
        throw new HarnessValidationError(
          'channels.enqueueOutbox().target.platform',
          `must match channel binding platform "${binding.platform}"`,
        );
      }
      const plan = (await config.adapter.resolveDeliveryPlan?.(opts, {
        harnessName: this._harnessName,
        channelId: binding.channelId,
        providerId: binding.providerId,
        platform: binding.platform,
        provider,
        binding,
      })) ?? {
        operationKind: opts.operationKind,
        operationName: opts.operationName,
        deliverySemantics: this._resolveChannelDeliverySemantics(opts, config),
      };
      const now = Date.now();
      const result = await storage.enqueueChannelOutbox({
        id: `outbox-${randomUUID()}`,
        harnessName: this._harnessName,
        channelId: binding.channelId,
        providerId: binding.providerId,
        bindingId: binding.bindingId,
        bindingGeneration: 1,
        idempotencyKey: opts.idempotencyKey,
        payloadHash: opts.payloadHash ?? sha256CanonicalJson(opts.payload),
        resourceId: opts.resourceId,
        threadId: opts.threadId,
        ...(opts.sessionId !== undefined ? { sessionId: opts.sessionId } : {}),
        ...(opts.owningSessionId !== undefined ? { owningSessionId: opts.owningSessionId } : {}),
        ...(opts.source !== undefined ? { source: opts.source } : {}),
        target: opts.target,
        kind: opts.kind,
        operationKind: plan.operationKind,
        ...(plan.operationName !== undefined ? { operationName: plan.operationName } : {}),
        payload: opts.payload,
        deliverySemantics: plan.deliverySemantics,
        status: 'pending',
        attempts: 0,
        createdAt: now,
        updatedAt: now,
      });
      // §10.2 channel_outbox_enqueued — only for a freshly-created row (idempotent
      // re-enqueues and payload-hash conflicts are not new outbound work).
      if (!result.duplicate && !result.conflict) {
        this._emitter.emit({
          type: 'channel_outbox_enqueued',
          harnessName: this._harnessName,
          channelId: binding.channelId,
          outboxItemId: result.outboxItemId,
          bindingId: binding.bindingId,
          kind: opts.kind,
        });
      }
      return result;
    },

    dispatchOutbox: async (opts: ChannelOutboxDispatchOptions = {}): Promise<ChannelOutboxDispatchResult> => {
      const storage = this._requireStorage('channels.dispatchOutbox()');
      const claimId = opts.claimId ?? `outbox-claim-${randomUUID()}`;
      const claimed = await storage.claimChannelOutbox({
        harnessName: this._harnessName,
        ...(opts.channelId !== undefined ? { channelId: opts.channelId } : {}),
        claimId,
        limit: opts.limit ?? this._channelOutboxBatchSize(opts.channelId),
        now: opts.now ?? Date.now(),
        claimTtlMs: opts.claimTtlMs ?? this._channelOutboxClaimTtlMs(opts.channelId),
      });
      const result: ChannelOutboxDispatchResult = { claimed: claimed.length, sent: 0, failed: 0, dead: 0, items: [] };
      const itemResults = await Promise.allSettled(
        claimed.map(item => this._dispatchClaimedChannelOutboxItem(storage, item, claimId, opts)),
      );
      for (const [index, settled] of itemResults.entries()) {
        const itemResult =
          settled.status === 'fulfilled'
            ? settled.value
            : ({
                outboxItemId: claimed[index]!.id,
                status: 'failed',
                error: {
                  code: 'unknown',
                  message: settled.reason instanceof Error ? settled.reason.message : 'channel outbox dispatch failed',
                },
              } satisfies ChannelOutboxDispatchResult['items'][number]);
        if (itemResult.status === 'sent') result.sent += 1;
        if (itemResult.status === 'failed') result.failed += 1;
        if (itemResult.status === 'dead') result.dead += 1;
        result.items.push(itemResult);
      }
      return result;
    },
  };

  private _requireChannelRuntime(channelId: string): { binding: HarnessChannelBinding; config: HarnessChannelConfig } {
    const binding = this.getChannelBinding(channelId);
    const config = this._channelRegistry.getConfig(channelId);
    if (!binding || !config) {
      throw new HarnessConfigError(`channels["${channelId}"]`, 'is not registered on this harness');
    }
    return { binding, config };
  }

  private _requireChannelProvider(binding: HarnessChannelBinding) {
    const provider = this.mastra.getChannelProvider(binding.providerId);
    if (!provider || provider.id !== binding.platform) {
      throw new HarnessConfigError(
        `channels["${binding.channelId}"].providerId`,
        `provider "${binding.providerId}" is unavailable or no longer matches platform "${binding.platform}"`,
      );
    }
    return provider;
  }

  private async _visibleChannelDiagnosticSessionIds(
    root: SessionRecord,
  ): Promise<{ sessionIds: string[]; truncated: boolean }> {
    const summaries = await this.listSessions({ resourceId: root.resourceId, includeClosed: true });
    const childrenByParent = new Map<string, SessionSummary[]>();
    for (const summary of summaries) {
      if (!summary.parentSessionId) continue;
      if (summary.resourceId !== root.resourceId) continue;
      const children = childrenByParent.get(summary.parentSessionId) ?? [];
      children.push(summary);
      childrenByParent.set(summary.parentSessionId, children);
    }

    const visible = new Set<string>([root.id]);
    let truncated = false;
    const stack: Array<{ id: string; depth: number }> = [{ id: root.id, depth: 0 }];
    while (stack.length > 0) {
      const { id: parentId, depth } = stack.pop()!;
      if (depth >= CHANNEL_DIAGNOSTICS_MAX_DESCENDANT_DEPTH) {
        if ((childrenByParent.get(parentId)?.length ?? 0) > 0) truncated = true;
        continue;
      }
      for (const child of childrenByParent.get(parentId) ?? []) {
        if (visible.has(child.id)) continue;
        if (visible.size >= CHANNEL_DIAGNOSTICS_MAX_VISIBLE_SESSIONS) {
          truncated = true;
          return { sessionIds: Array.from(visible), truncated };
        }
        visible.add(child.id);
        stack.push({ id: child.id, depth: depth + 1 });
      }
    }
    return { sessionIds: Array.from(visible), truncated };
  }

  private _resolveChannelDeliverySemantics(
    opts: ChannelOutboxEnqueueOptions,
    config: HarnessChannelConfig,
  ): NonNullable<ChannelOutboxEnqueueOptions['deliverySemantics']> {
    return (
      opts.deliverySemantics ??
      config.adapter.deliverySemanticsByOperation?.[opts.operationKind] ??
      config.adapter.deliverySemantics ??
      'at-least-once'
    );
  }

  private _channelOutboxClaimTtlMs(channelId: string | undefined): number {
    if (channelId === undefined) return DEFAULT_CHANNEL_OUTBOX_CLAIM_TTL_MS;
    return this._channelRegistry.getConfig(channelId)?.outbox?.claimTtlMs ?? DEFAULT_CHANNEL_OUTBOX_CLAIM_TTL_MS;
  }

  private _channelOutboxBatchSize(channelId: string | undefined): number {
    if (channelId === undefined) return DEFAULT_CHANNEL_OUTBOX_BATCH_SIZE;
    return this._channelRegistry.getConfig(channelId)?.outbox?.batchSize ?? DEFAULT_CHANNEL_OUTBOX_BATCH_SIZE;
  }

  private async _dispatchClaimedChannelOutboxItem(
    storage: HarnessStorage,
    item: ChannelOutboxItem,
    claimId: string,
    opts: ChannelOutboxDispatchOptions,
  ): Promise<ChannelOutboxDispatchResult['items'][number]> {
    const { binding, config } = this._channelRuntimeForDispatch(item);
    const maxAttempts = config?.outbox?.maxAttempts ?? DEFAULT_CHANNEL_OUTBOX_MAX_ATTEMPTS;
    const claimTtlMs = opts.claimTtlMs ?? config?.outbox?.claimTtlMs ?? DEFAULT_CHANNEL_OUTBOX_CLAIM_TTL_MS;
    const claimRenewMs = config?.outbox?.claimRenewMs ?? Math.max(1, Math.floor(claimTtlMs / 2));
    const markFailure = async (code: HarnessRowErrorCode, message: string, retryable = true) => {
      const dead = !retryable || item.attempts >= maxAttempts;
      const retryBaseNow = opts.now ?? Date.now();
      try {
        await storage.markChannelOutboxFailed({
          outboxItemId: item.id,
          claimId,
          dead,
          ...(!dead ? { retryAt: retryBaseNow + this._channelOutboxRetryBackoffMs(config, item.attempts) } : {}),
          error: { code, message, retryable: !dead },
        });
      } catch (err) {
        if (!(err instanceof HarnessStorageChannelOutboxClaimConflictError)) throw err;
        return {
          outboxItemId: item.id,
          status: 'failed' as const,
          error: { code, message: `${message}; claim was lost before failure could be recorded` },
        };
      }
      // §10.2 channel_outbox_failed — emitted only after the durable failure
      // transition commits (not on the claim-conflict path above, where it was
      // not recorded). `dead` distinguishes terminal from retryable. §13.3f.1:
      // project the bare row code to its namespaced harness.* wire code — a bare
      // row code must never cross the public event boundary.
      const projectedError = projectRowErrorCode(code);
      this._emitter.emit({
        type: 'channel_outbox_failed',
        harnessName: this._harnessName,
        channelId: item.channelId,
        outboxItemId: item.id,
        bindingId: item.bindingId,
        attempts: item.attempts,
        dead,
        error: {
          code: projectedError.code,
          ...(projectedError.reason !== undefined ? { reason: projectedError.reason } : {}),
          message,
        },
      });
      return {
        outboxItemId: item.id,
        status: dead ? ('dead' as const) : ('failed' as const),
        error: { code, message },
      };
    };

    if (!binding || !config || binding.providerId !== item.providerId || item.bindingGeneration !== 1) {
      return markFailure('delivery_operation_unavailable', 'channel binding is unavailable for outbox delivery');
    }

    const provider = this.mastra.getChannelProvider(binding.providerId);
    if (!provider || provider.id !== binding.platform) {
      return markFailure('delivery_operation_unavailable', 'channel provider is unavailable for outbox delivery');
    }

    const ctx = {
      harnessName: this._harnessName,
      channelId: binding.channelId,
      providerId: binding.providerId,
      platform: binding.platform,
      provider,
      binding,
    };

    let providerMessageId: string | undefined;
    let providerReceipt: ChannelProviderDeliveryReceipt | undefined;
    let deliveryConfirmed = false;
    try {
      await storage.renewChannelOutboxClaim({
        outboxItemId: item.id,
        claimId,
        now: Date.now(),
        claimTtlMs,
      });

      if (item.deliverySemantics === 'lookup-reconcile' && item.attempts > 1) {
        if (!config.adapter.reconcileDelivery) {
          return markFailure(
            'delivery_operation_unavailable',
            'channel adapter cannot reconcile lookup-reconcile outbox delivery',
          );
        }
        const reconciliation = await this._withChannelOutboxClaimHeartbeat(
          storage,
          item,
          claimId,
          claimTtlMs,
          claimRenewMs,
          () => config.adapter.reconcileDelivery!(item, ctx),
        );
        if (reconciliation.delivered) {
          providerMessageId = reconciliation.providerMessageId ?? reconciliation.providerReceipt?.providerMessageId;
          providerReceipt = reconciliation.providerReceipt;
          deliveryConfirmed = true;
        }
      }

      if (!deliveryConfirmed) {
        const delivery = await this._withChannelOutboxClaimHeartbeat(
          storage,
          item,
          claimId,
          claimTtlMs,
          claimRenewMs,
          () => config.adapter.deliver(item, ctx),
        );
        providerMessageId = delivery.providerMessageId ?? delivery.providerReceipt?.providerMessageId;
        providerReceipt = delivery.providerReceipt;
        deliveryConfirmed = true;
      }
    } catch (err) {
      return markFailure('unknown', err instanceof Error ? err.message : 'channel outbox delivery failed');
    }
    try {
      await storage.markChannelOutboxSent({
        outboxItemId: item.id,
        claimId,
        ...(providerMessageId !== undefined ? { providerMessageId } : {}),
        ...(providerReceipt !== undefined ? { providerReceipt } : {}),
      });
    } catch (err) {
      if (!(err instanceof HarnessStorageChannelOutboxClaimConflictError)) throw err;
      return {
        outboxItemId: item.id,
        status: 'failed',
        error: { code: 'unknown', message: 'channel outbox claim was lost before sent delivery could be recorded' },
      };
    }
    // §10.2 channel_outbox_sent — emitted only after the durable sent transition
    // commits, so clients never observe a false delivery.
    this._emitter.emit({
      type: 'channel_outbox_sent',
      harnessName: this._harnessName,
      channelId: item.channelId,
      outboxItemId: item.id,
      bindingId: item.bindingId,
      ...(providerMessageId !== undefined ? { providerMessageId } : {}),
    });
    return {
      outboxItemId: item.id,
      status: 'sent',
      ...(providerMessageId !== undefined ? { providerMessageId } : {}),
    };
  }

  private _channelRuntimeForDispatch(item: ChannelOutboxItem): {
    binding?: HarnessChannelBinding;
    config?: HarnessChannelConfig;
  } {
    const binding = this.getChannelBinding(item.channelId);
    const config = this._channelRegistry.getConfig(item.channelId);
    if (!binding || !config) return {};
    if (binding.bindingId !== item.bindingId) return {};
    return { binding, config };
  }

  private _channelOutboxRetryBackoffMs(config: HarnessChannelConfig | undefined, attempt: number): number {
    return config?.outbox?.retryBackoffMs?.(attempt) ?? Math.min(60_000, 1000 * 2 ** Math.max(0, attempt - 1));
  }

  private async _withChannelOutboxClaimHeartbeat<T>(
    storage: HarnessStorage,
    item: ChannelOutboxItem,
    claimId: string,
    claimTtlMs: number,
    claimRenewMs: number,
    operation: () => Promise<T>,
  ): Promise<T> {
    let heartbeatError: unknown;
    let interval: ReturnType<typeof setInterval> | undefined;
    const renew = async () => {
      try {
        await storage.renewChannelOutboxClaim({
          outboxItemId: item.id,
          claimId,
          now: Date.now(),
          claimTtlMs,
        });
        heartbeatError = undefined;
      } catch (err) {
        heartbeatError = err;
        if (interval !== undefined) clearInterval(interval);
      }
    };
    interval = setInterval(() => {
      void renew();
    }, claimRenewMs);
    interval.unref?.();
    try {
      const result = await operation();
      if (heartbeatError) throw heartbeatError;
      return result;
    } finally {
      if (interval !== undefined) clearInterval(interval);
    }
  }

  /**
   * §14.2 claim renewal for the channel-ingress recovery worker. Keeps the inbox
   * row's claim alive while a slow admission runs (binding resolution, session
   * hydration / cold-start, queue admission can each near the claim TTL). Renews
   * at `claimRenewMs` against a FRESH `Date.now()` (NOT the worker tick's `now`,
   * which never advances during the row's async work); a renewal failure just
   * stops the heartbeat.
   *
   * Unlike `_withChannelOutboxClaimHeartbeat`, this does NOT throw a post-operation
   * heartbeat error: every step the wrapped operation performs is a claim-checked
   * `updateChannelInboxItem` write, so a genuine claim loss makes that write throw
   * (which the worker's catch already handles). Throwing a late renewal error here
   * would mis-report a row that actually committed `queued` — once the terminal
   * write lands, the renewal naturally fails (terminal rows are unclaimable), and
   * that benign failure must not surface as a `channel_ingress_failed`.
   */
  private async _withChannelInboxClaimHeartbeat<T>(
    storage: HarnessStorage,
    item: ChannelInboxItem,
    claimId: string,
    claimTtlMs: number,
    claimRenewMs: number,
    operation: () => Promise<T>,
  ): Promise<T> {
    let interval: ReturnType<typeof setInterval> | undefined;
    const renew = async () => {
      try {
        await storage.renewChannelInboxClaim({ inboxItemId: item.id, claimId, now: Date.now(), claimTtlMs });
      } catch {
        // Stop renewing. A renewal that fails because the row is now terminal
        // (the operation already committed) is benign; one that fails because the
        // claim was lost is surfaced by the next claim-checked write throwing.
        if (interval !== undefined) clearInterval(interval);
      }
    };
    interval = setInterval(() => {
      void renew();
    }, claimRenewMs);
    interval.unref?.();
    try {
      return await operation();
    } finally {
      if (interval !== undefined) clearInterval(interval);
    }
  }

  // -------------------------------------------------------------------------
  // Permission gate accessors — §4.2e.
  // -------------------------------------------------------------------------

  /**
   * Resolve a tool name to its category, using the resolver configured on
   * `HarnessConfig.toolCategoryResolver`. Returns `null` for tools with no
   * configured resolver or that the resolver explicitly leaves
   * uncategorised. Used by the permission gate and by TUIs that want to
   * render tools grouped by category.
   */
  getToolCategory(opts: { toolName: string }): ToolCategory | null {
    if (!this._toolCategoryResolver) return null;
    return this._toolCategoryResolver(opts.toolName) ?? null;
  }

  /** @internal — Session reads this as the floor when no per-tool / per-category rule applies. */
  _getDefaultPermissionPolicy(): PermissionPolicy {
    return this._defaultPermissionPolicy;
  }

  // -------------------------------------------------------------------------
  // Session resolver — §4.1, §5.3.
  // -------------------------------------------------------------------------

  async session(opts: SessionResolveOptions): Promise<Session> {
    if (this._shutdown) {
      throw new Error('Harness is shut down');
    }
    const storage = this._requireStorage('session()');

    // 1) sessionId-only lookups.
    if ('sessionId' in opts && opts.sessionId && !('threadId' in opts && opts.threadId)) {
      return this._resolveById(storage, opts.sessionId, opts.resourceId);
    }

    // 2) threadId resolution. May be `{ fresh: true }` to force a new thread.
    if ('threadId' in opts && opts.threadId !== undefined) {
      return this._resolveByThread(storage, opts);
    }

    // §5.3: there is NO resource-only resolver. "Continue latest" is product
    // selection policy (read listSessions(...) then resolve a concrete
    // sessionId/threadId), kept out of the core lifecycle API so old
    // selectOrCreateThread "continue latest" semantics cannot become a hidden
    // fallback. Bare `{ resourceId }` is therefore invalid resolver options.
    throw new HarnessConfigError('session()', 'invalid resolver options');
  }

  private async _resolveById(storage: HarnessStorage, sessionId: string, resourceId?: string): Promise<Session> {
    // In-memory hit — return live instance, enforce resourceId scoping.
    const live = this._liveSessions.get(sessionId);
    if (live) {
      if (resourceId !== undefined && live.resourceId !== resourceId) {
        // Don't leak existence across tenants.
        throw new HarnessSessionNotFoundError(sessionId);
      }
      if (live.isClosing) {
        throw harnessSessionClosingError(live);
      }
      return live;
    }

    const stored = await storage.loadSession({ harnessName: this._harnessName, sessionId });
    if (!stored) throw new HarnessSessionNotFoundError(sessionId);
    if (resourceId !== undefined && stored.resourceId !== resourceId) {
      // Cross-tenant existence is never leaked.
      throw new HarnessSessionNotFoundError(sessionId);
    }
    // §5.2a/§5.3: before reopening or leasing a direct-ID record, confirm it is
    // still the current owner for its (harness, resource, thread). A different
    // current owner means the store was corrupted or operator-mutated out of
    // band — fail closed instead of reopening/leasing a superseded record. This
    // must run BEFORE the closed→reopen branch so reopen cannot resurrect a
    // superseded record into a second active owner.
    const currentOwner = await storage.loadSessionByThread({
      harnessName: this._harnessName,
      threadId: stored.threadId,
      resourceId: stored.resourceId,
    });
    if (currentOwner && currentOwner.id !== stored.id) {
      // §2.2/§4.5: one current owner per (harness, resource, thread). If the
      // REQUESTED record is superseded (closed/closing) while a different session
      // currently owns the thread, this is an expected CONFLICT — the caller
      // supplied a stale session id; tell it the active owner instead of reopening
      // a superseded record into a second active owner. Only when the requested
      // record is itself still ACTIVE alongside a different active owner is this
      // genuine duplicate-owner corruption.
      if (stored.closedAt !== undefined || stored.closingAt !== undefined) {
        throw new HarnessSessionConflictError(stored.resourceId, stored.threadId, stored.id, currentOwner.id);
      }
      throw new HarnessSessionCorruptError({
        reason: 'duplicate_session_owner',
        sessionId: stored.id,
        resourceId: stored.resourceId,
        threadId: stored.threadId,
        ownerSessionIds: [currentOwner.id],
      });
    }

    if (stored.closedAt !== undefined) {
      // §5.3: direct-ID lookup of a Closed (reopenable) record reopens it under
      // the normal resource/lease checks, then hydrates.
      return this._reopen(storage, stored);
    }
    if (stored.closingAt !== undefined) {
      throw harnessSessionClosingError(stored);
    }

    await this._markExternalSessionStorageOwner(stored.threadId, { requireExisting: false });
    return this._hydrate(storage, stored);
  }

  private async _resolveByThread(
    storage: HarnessStorage,
    opts: Extract<SessionResolveOptions, { threadId: any }>,
  ): Promise<Session> {
    const wantsFreshThread = typeof opts.threadId !== 'string';
    const resourceId = opts.resourceId!;

    if (wantsFreshThread) {
      // Force a brand-new thread + session. ownsThread = true so the cascade
      // can later tear the thread down with the session.
      return this._createFresh(storage, {
        resourceId,
        threadId: this._mintThreadId(),
        ownsThread: true,
        sessionId: opts.sessionId,
        parentSessionId: opts.parentSessionId,
        origin: opts.origin ?? 'top-level',
        modeId: opts.modeId,
        modelId: opts.modelId,
        subagentDepth: opts.subagentDepth,
      });
    }

    const threadId = opts.threadId as string;

    // In-memory hit by (threadId, resourceId)?
    for (const live of this._liveSessions.values()) {
      if (live.threadId === threadId && live.resourceId === resourceId) {
        if (live.isClosing) {
          throw harnessSessionClosingError(live);
        }
        return live;
      }
    }

    // Storage lookup — returns the current owner including Closed/Closing (§5.2a).
    const stored = await storage.loadSessionByThread({ harnessName: this._harnessName, threadId, resourceId });
    if (stored) {
      if (stored.closedAt !== undefined) {
        // §5.3/§5.5: reopen the closed owning record instead of ignoring it and
        // creating a fresh active record on the same thread.
        return this._reopen(storage, stored);
      }
      if (stored.closingAt !== undefined) {
        throw harnessSessionClosingError(stored);
      }
      await this._markExternalSessionStorageOwner(stored.threadId, { requireExisting: false });
      return this._hydrate(storage, stored);
    }

    await this._assertNoPreboundDefaultNamespaceShadow(storage, threadId, resourceId);

    // No active record — create a fresh session bound to this thread.
    return this._createFresh(storage, {
      resourceId,
      threadId,
      ownsThread: false,
      sessionId: opts.sessionId,
      parentSessionId: opts.parentSessionId,
      origin: opts.origin ?? 'top-level',
      modeId: opts.modeId,
      modelId: opts.modelId,
      subagentDepth: opts.subagentDepth,
    });
  }


  // -------------------------------------------------------------------------
  // Session creation / hydration.
  // -------------------------------------------------------------------------

  private async _createFresh(
    storage: HarnessStorage,
    init: {
      resourceId: string;
      threadId: string;
      ownsThread: boolean;
      sessionId?: string;
      parentSessionId?: string;
      origin: 'top-level' | 'subagent-tool';
      modeId?: string;
      modelId?: string;
      subagentDepth?: number;
    },
  ): Promise<Session> {
    await this._enforceMaxLiveCap();
    const sessionId = init.sessionId ?? `sess-${randomUUID()}`;
    const now = Date.now();

    const modeId = init.modeId ?? this._defaultModeId;
    if (modeId === undefined) {
      throw new HarnessConfigError(
        'session()',
        'cannot create a session without a modeId — config has no modes and no override was supplied',
      );
    }
    const mode = this._modesById.get(modeId);
    if (!mode) {
      throw new HarnessConfigError('session().modeId', `unknown mode "${modeId}"`);
    }
    const record: SessionRecord = {
      id: sessionId,
      harnessName: this._harnessName,
      resourceId: init.resourceId,
      threadId: init.threadId,
      parentSessionId: init.parentSessionId,
      origin: init.origin,
      ownsThread: init.ownsThread,
      subagentDepth: init.subagentDepth ?? 0,
      modeId,
      modelId: init.modelId ?? '',
      subagentModelOverrides: {},
      permissionRules: emptyPermissionRules(),
      sessionGrants: emptySessionGrants(),
      tokenUsage: zeroTokenUsage(),
      pendingQueue: [],
      state: {},
      createdAt: now,
      lastActivityAt: now,
      version: 0,
      ownerId: this.ownerId,
      leaseExpiresAt: now + this._leaseTtlMs,
    };

    const liveParentError = this._getLiveParentAdmissionError(init.parentSessionId, init.resourceId);
    if (liveParentError) {
      const existing = await storage.loadSessionByThread({
        harnessName: this._harnessName,
        threadId: init.threadId,
        resourceId: init.resourceId,
      });
      if (existing) {
        if (existing.closedAt !== undefined) {
          return this._reopen(storage, existing);
        }
        if (existing.closingAt !== undefined) {
          throw harnessSessionClosingError(existing);
        }
        return this._hydrate(storage, existing);
      }
      throw liveParentError;
    }

    await this._markExternalSessionStorageOwner(init.threadId, { requireExisting: !init.ownsThread });

    let admitted;
    try {
      admitted = await storage.createOrLoadActiveSession(record, {
        initialLease: { ownerId: this.ownerId, ttlMs: this._leaseTtlMs },
      });
    } catch (err) {
      if (err instanceof HarnessStorageParentSessionUnavailableError) {
        if (err.reason === 'closing')
          throw harnessSessionClosingError({
            id: err.parentSessionId,
            closingAt: err.closingAt,
            closeDeadlineAt: err.closeDeadlineAt,
          });
        if (err.reason === 'closed') throw new HarnessSessionClosedError(err.parentSessionId);
        throw new HarnessSessionNotFoundError(err.parentSessionId);
      }
      // A version conflict on first insert means another writer beat us to
      // this id (only realistic for deterministic ids passed by the caller).
      if (err instanceof HarnessStorageVersionConflictError) {
        throw new HarnessSessionLockedError(sessionId, 'unknown', 0);
      }
      throw new HarnessStorageError({ operation: 'session_create', sessionId, cause: err });
    }

    if (!admitted.created) {
      // A current owner already holds this (harness, resource, thread) key.
      // §5.3/§5.5: reopen a Closed owner, fail a Closing one, hydrate an Active
      // one — never create a second active owner behind a non-active record.
      if (admitted.record.closedAt !== undefined) {
        return this._reopen(storage, admitted.record);
      }
      if (admitted.record.closingAt !== undefined) {
        throw harnessSessionClosingError(admitted.record);
      }
      return this._hydrate(storage, admitted.record);
    }

    return this._publish(storage, admitted.record);
  }

  /**
   * @internal §6.2 strict-lazy + subagent `inherit`. A child inheriting its
   * parent's workspace requires the parent's per-session registry entry to exist,
   * but under strict-lazy materialization a parent turn may never have resolved
   * its workspace. Resolve the (live) parent's workspace first — idempotent if it
   * was already materialized — so the child's `inheritPerSession` acquire finds it.
   * No-op when the parent is not live; the registry then surfaces its clear
   * "no workspace to inherit" error.
   */
  async _internalEnsureParentWorkspaceForInherit(parentSessionId: string): Promise<void> {
    const liveParent = this._liveSessions.get(parentSessionId);
    if (liveParent && !liveParent.isClosed && !liveParent.isClosing) {
      await liveParent.getWorkspace();
    }
  }

  private _getLiveParentAdmissionError(parentSessionId: string | undefined, resourceId: string): Error | undefined {
    if (!parentSessionId) return undefined;
    const liveParent = this._liveSessions.get(parentSessionId);
    if (!liveParent) return undefined;
    if (liveParent.resourceId !== resourceId) return new HarnessSessionNotFoundError(parentSessionId);
    if (liveParent.isClosing) return harnessSessionClosingError(liveParent);
    if (liveParent.isClosed) return new HarnessSessionClosedError(parentSessionId);
    return undefined;
  }

  private async _assertNoPreboundDefaultNamespaceShadow(
    storage: HarnessStorage,
    threadId: string,
    resourceId: string,
  ): Promise<void> {
    if (!this._guardPreboundDefaultNamespace || this._harnessName === 'default') return;
    const existingDefault = await storage.loadSessionByThread({
      harnessName: 'default',
      threadId,
      resourceId,
    });
    if (!existingDefault) return;
    throw new HarnessConfigError(
      'session().threadId',
      'cannot create a registered harness session for a thread/resource with an active default-namespace session; close or migrate the default session first',
    );
  }

  private async _hydrate(storage: HarnessStorage, stored: SessionRecord): Promise<Session> {
    await this._enforceMaxLiveCap();
    const lease = await this._acquireLease(storage, stored.harnessName, stored.id);
    const record: SessionRecord = {
      ...stored,
      ownerId: this.ownerId,
      leaseExpiresAt: lease.expiresAt,
      version: lease.version,
    };
    const session = this._publish(storage, record, await this._eventReplaySeedFor(storage, record));
    // §10.2: a session re-loaded from storage into the live cache emits a
    // (session-scoped, non-terminal) `session_hydrated` observer notification.
    session._emit({ type: 'session_hydrated' });
    return session;
  }

  /**
   * Reopen a Closed (reopenable) record (§5.3/§5.5): prove ownership via the
   * lease, then durably clear the closed/closing markers under CAS so the
   * record returns to its current-owner key as Active. Reuses the same lease +
   * CAS primitives as hydrate — `saveSession` replaces the record (clearing
   * `closedAt`) and preserves the lease metadata that `acquireSessionLease`
   * just set, so no separate atomic reopen storage op is required.
   */
  private async _reopen(storage: HarnessStorage, stored: SessionRecord): Promise<Session> {
    await this._enforceMaxLiveCap();
    const lease = await this._acquireLease(storage, stored.harnessName, stored.id);
    const reopened: SessionRecord = {
      ...stored,
      closedAt: undefined,
      closingAt: undefined,
      closeDeadlineAt: undefined,
      ownerId: this.ownerId,
      leaseExpiresAt: lease.expiresAt,
      version: lease.version,
    };
    let saved: { version: number };
    try {
      saved = await storage.saveSession(reopened, {
        harnessName: stored.harnessName,
        ownerId: this.ownerId,
        ifVersion: lease.version,
      });
    } catch (err) {
      // Lost a race with a concurrent close/delete/reopen on the same record.
      if (err instanceof HarnessStorageVersionConflictError) {
        throw new HarnessSessionLockedError(stored.id, 'unknown', 0);
      }
      throw new HarnessStorageError({ operation: 'session_save', sessionId: stored.id, cause: err });
    }
    const record: SessionRecord = { ...reopened, version: saved.version };
    await this._markExternalSessionStorageOwner(record.threadId, { requireExisting: false });
    return this._publish(storage, record, await this._eventReplaySeedFor(storage, record));
  }

  private _publish(
    storage: HarnessStorage,
    record: SessionRecord,
    eventReplaySeed?: { epoch: string; nextSequence: number },
  ): Session {
    return this._adoptSession(storage, record, { emitCreated: true, kickQueueDrain: true, eventReplaySeed });
  }

  private _adoptSession(
    storage: HarnessStorage,
    record: SessionRecord,
    opts: {
      emitCreated: boolean;
      kickQueueDrain: boolean;
      eventReplaySeed?: { epoch: string; nextSequence: number };
    },
  ): Session {
    // Workspace provider validation (§2.7). If the stored record carries a
    // workspace state blob, the configured provider must match. Mismatch is
    // a hard error — refuse to hand the record to the wrong implementation.
    // Non-resumable providers can never restore from stored state; flag the
    // session as "lost" so the first getWorkspace() call surfaces the error.
    let workspaceLost = false;
    if (record.workspace?.providerId && this._workspaceKind === 'per-session') {
      const configured = this._workspaceRegistry.providerId;
      if (configured && configured !== record.workspace.providerId) {
        throw new HarnessWorkspaceProviderMismatchError(record.id, configured, record.workspace.providerId);
      }
      if (!this._workspaceRegistry.resumable) {
        // Provider can't resume — first getWorkspace() throws HarnessWorkspaceLostError.
        workspaceLost = true;
      }
    }

    const session = new Session({
      harness: this,
      storage,
      ownerId: this.ownerId,
      record,
      leaseExpiresAt: record.leaseExpiresAt ?? Date.now() + this._leaseTtlMs,
      eventReplaySeed: opts.eventReplaySeed,
      persistTransientStreamingEvents: this._persistTransientStreamingEvents,
    });
    if (workspaceLost) session._markWorkspaceLost();
    this._hasAdoptedSessions = true;
    this._liveSessions.set(record.id, session);

    // Bridge the session's events onto the harness-level emitter so a single
    // harness.subscribe() sees every session's turn activity. Forwarded
    // events keep their original id/timestamp/sessionId.
    const bridge = session._subscribeInternal(event => this._emitter.forward(event));
    this._sessionEventBridges.set(record.id, bridge);
    this._ensureLeaseRenewalLoop();

    if (opts.emitCreated) {
      // Surface session creation to harness-level subscribers AFTER the bridge
      // is wired. Stamps `sessionId` via the override so harness emitter
      // (no scope) can carry it.
      this._emitter.emit(
        {
          type: 'session_created',
          resourceId: record.resourceId,
          threadId: record.threadId,
          ...(record.parentSessionId !== undefined && { parentSessionId: record.parentSessionId }),
          modeId: record.modeId,
          modelId: record.modelId,
        },
        { sessionId: record.id },
      );
    }

    // If the hydrated record has queued items waiting and no live suspension
    // blocking them, kick the drain. A `pendingResume` with `resumedAt` is
    // also kicked so stale queued-resume recovery can clear/fail it instead
    // of leaving the queue permanently busy. Items recovered this way emit
    // `queue_item_replayed` instead of `queue_item_started` because the
    // original `queue()` caller's resolver is gone.
    if (
      opts.kickQueueDrain &&
      (record.pendingQueue?.length ?? 0) > 0 &&
      (record.pendingResume === undefined || record.pendingResume.resumedAt !== undefined)
    ) {
      void session._kickQueueDrain();
    }

    return session;
  }

  private _ensureLeaseRenewalLoop(): void {
    if (this._shutdown) return;
    if (this._leaseRenewalTimer !== undefined) return;
    const intervalMs = Math.max(1_000, this._lockRenewMs);
    this._leaseRenewalTimer = setInterval(() => {
      void this._renewLiveSessionLeases();
    }, intervalMs);
    this._leaseRenewalTimer.unref?.();
  }

  private _stopLeaseRenewalLoop(): void {
    if (this._leaseRenewalTimer === undefined) return;
    clearInterval(this._leaseRenewalTimer);
    this._leaseRenewalTimer = undefined;
  }

  private _stopLeaseRenewalLoopIfIdle(): void {
    if (this._liveSessions.size > 0) return;
    this._stopLeaseRenewalLoop();
  }

  // ---------------------------------------------------------------------------
  // §14.2 channel-ingress recovery worker scheduler. Mirrors the lease-renewal
  // loop, but starts on BIND (not session adoption): crashed/received/failed
  // inbox rows must be recovered even when no session is live. Idle harnesses
  // without channels never start the timer. `unref`ed so it never holds the
  // process open; stopped on shutdown.
  // ---------------------------------------------------------------------------

  private _ensureChannelInboxRecoveryLoop(): void {
    if (this._shutdown) return;
    if (this._channelInboxRecoveryTimer !== undefined) return;
    if (this._channelRegistry.list().length === 0) return; // no channels → no worker
    const intervalMs = this._resolveChannelInboxPollIntervalMs();
    this._channelInboxRecoveryTimer = setInterval(() => {
      void this._tickChannelInboxRecovery();
    }, intervalMs);
    this._channelInboxRecoveryTimer.unref?.();
  }

  private _stopChannelInboxRecoveryLoop(): void {
    if (this._channelInboxRecoveryTimer === undefined) return;
    clearInterval(this._channelInboxRecoveryTimer);
    this._channelInboxRecoveryTimer = undefined;
  }

  /** §9 poll cadence: the smallest configured channel `inbox.pollIntervalMs`,
   * floored at 1s (a single harness-level tick sweeps every channel). */
  private _resolveChannelInboxPollIntervalMs(): number {
    let interval = 1_000;
    let seen = false;
    for (const binding of this._channelRegistry.list()) {
      const poll = this._channelRegistry.getConfig(binding.channelId)?.inbox?.pollIntervalMs;
      if (poll !== undefined && (!seen || poll < interval)) {
        interval = poll;
        seen = true;
      }
    }
    return Math.max(1_000, interval);
  }

  /**
   * @internal §14.2 recovery worker tick — one `recoverChannelInboxOnce` pass per
   * configured channel. Reentrancy-guarded so a slow pass never overlaps the next
   * interval. A per-channel failure is isolated (storage errors surface as a
   * `storage_error` observer event; others are swallowed and retried next tick) so
   * one bad channel never starves the others.
   */
  async _tickChannelInboxRecovery(): Promise<void> {
    if (this._shutdown || this._channelInboxRecoveryRunning) return;
    this._channelInboxRecoveryRunning = true;
    try {
      for (const binding of this._channelRegistry.list()) {
        if (this._shutdown) break;
        try {
          await this.recoverChannelInboxOnce({ channelId: binding.channelId });
        } catch (err) {
          if (err instanceof HarnessStorageError) this._emitStorageError(err);
          // else: transient/per-row failure — the next tick reclaims and retries.
        }
      }
    } finally {
      this._channelInboxRecoveryRunning = false;
    }
  }

  private async _renewLiveSessionLeases(): Promise<void> {
    if (this._shutdown || this._liveSessions.size === 0) {
      this._stopLeaseRenewalLoopIfIdle();
      return;
    }
    const storage = this._requireStorage('session lease renewal');
    // §5.8: only roots renew. A root renewal extends the root AND every active
    // descendant atomically (`renewSessionLeaseSubtree`). Children have no
    // separately-renewable lease, so they are never renewed independently here.
    const live = Array.from(this._liveSessions.values());
    const roots: Session[] = [];
    const orphans: Session[] = [];
    for (const session of live) {
      if (session.parentSessionId === undefined) {
        roots.push(session);
      } else if (this._resolveLiveRoot(session) === undefined) {
        // A live descendant whose ancestry is not fully live (e.g. a child
        // hydrated by direct id/thread access without its root) can never be
        // covered by a root subtree renewal — fence it as lease_lost (§5.8).
        orphans.push(session);
      }
    }
    await Promise.all(roots.map(root => this._renewLiveSessionLeaseSubtree(storage, root)));
    for (const orphan of orphans) {
      if (this._liveSessions.get(orphan.id) === orphan) await this._evictLiveSession(orphan, 'lease_lost');
    }
    await this._evictIdleSessions();
    this._stopLeaseRenewalLoopIfIdle();
  }

  /**
   * §5.4 idle eviction: drop live sessions with no activity for `idleTimeoutMs`,
   * skipping any pinned (subtree-pinned) session. Runs on the keep-alive
   * cadence; the evicted record stays in storage and rehydrates on next access.
   */
  private async _evictIdleSessions(): Promise<void> {
    if (!Number.isFinite(this._idleTimeoutMs)) return;
    const cutoff = Date.now() - this._idleTimeoutMs;
    const idle: Session[] = [];
    for (const session of this._liveSessions.values()) {
      if (session.lastActivityAt > cutoff) continue;
      if (this._isPinnedSubtree(session)) continue;
      idle.push(session);
    }
    for (const session of idle) {
      if (this._liveSessions.get(session.id) === session) {
        await this._evictLiveSession(session, 'idle');
      }
    }
  }

  /**
   * §5.8 background renewal for one live root: extend the root + every active
   * descendant atomically via `renewSessionLeaseSubtree`, then mark the root and
   * currently-live descendants to the new expiry. On lease conflict / split or a
   * missing root, fence the entire live subtree as lease_lost.
   */
  private async _renewLiveSessionLeaseSubtree(storage: HarnessStorage, root: Session): Promise<void> {
    if (root.lifecycleState !== 'live' && root.lifecycleState !== 'closing') return;
    if (this._leaseRenewingSessionIds.has(root.id)) return;
    this._leaseRenewingSessionIds.add(root.id);
    try {
      await root._enqueueLeaseRenewal(async () => {
        const effectiveTtl = root._getEffectiveLeaseTtlMs(this._leaseTtlMs);
        await this._renewSubtreeFromRoot(storage, root, effectiveTtl);
      });
    } catch (err) {
      if (err instanceof HarnessStorageLeaseConflictError || err instanceof HarnessStorageSessionNotFoundError) {
        // §5.8: subtree renewal failed (lost ownership or subtree split) — fence
        // the whole live subtree. Recompute live descendants at failure time so
        // a descendant created during the renewal is not left under a lost root.
        const subtree = [root, ...this._liveDescendantsOf(root.id)];
        for (const session of subtree) {
          if (this._liveSessions.get(session.id) === session) await this._evictLiveSession(session, 'lease_lost');
        }
        return;
      }
      // §10.2: a background lease-renewal storage failure never reaches a
      // caller — surface it as a storage_error observer event.
      if (err instanceof HarnessStorageError) this._emitStorageError(err);
      console.error('[harness/v1] session subtree lease renewal failed:', err);
    } finally {
      this._leaseRenewingSessionIds.delete(root.id);
    }
  }

  /**
   * §5.8 shared subtree renew+mark. Atomically renews the root + active
   * descendant lease entries in storage, then advances the in-memory expiry on
   * the root and every currently-live descendant (recomputed post-call so a
   * descendant created during the renewal is covered). Caller owns reentrancy,
   * the renewal chain, and the failure policy (evict vs throw).
   */
  private async _renewSubtreeFromRoot(
    storage: HarnessStorage,
    root: Session,
    ttlMs: number,
  ): Promise<SubtreeSessionLeaseResult> {
    const record = root.getRecord();
    const lease = await storage.renewSessionLeaseSubtree({
      harnessName: record.harnessName,
      rootSessionId: root.id,
      ownerId: this.ownerId,
      ttlMs,
    });
    root._markLeaseRenewed(lease.expiresAt);
    for (const descendant of this._liveDescendantsOf(root.id)) descendant._markLeaseRenewed(lease.expiresAt);
    return lease;
  }

  /**
   * @internal §5.8 — prove + renew the lease subtree that owns `session`, used by
   * the foreground paths (`extendLease`, CAS write-recovery). Resolves the live
   * root, renews the whole subtree atomically, and returns the new expiry. When
   * `propagateRootDeadline` is set (a caller-requested `extendLease`), the root's
   * effective-TTL deadline is advanced so the next background sweep does not
   * shorten the extension back to the default TTL. Throws
   * `HarnessSessionLockedError` if the root cannot be proven live in this process
   * — a child is never renewed on its own row.
   */
  async _internalRenewProveSubtree(
    session: Session,
    ttlMs: number,
    opts?: { propagateRootDeadline?: boolean },
  ): Promise<number> {
    const storage = this._requireStorage('session lease renewal');
    const root = this._resolveLiveRoot(session);
    if (root === undefined) {
      // Orphan: a live session whose root ownership cannot be proven here. Fence
      // it rather than renewing a child on its own row.
      await this._evictLiveSession(session, 'lease_lost');
      throw new HarnessSessionLockedError(session.id, 'unknown', session.getRecord().leaseExpiresAt ?? 0);
    }
    // Serialize on the ROOT's lease-renewal chain — the same chain the
    // background sweep uses — so a foreground extend / CAS proof can't run
    // concurrently with a root sweep and let the later write shrink the subtree
    // lease. The floor below (root effective TTL) is only sufficient under this
    // serialization. `_enqueueLeaseRenewal` skips when the root is no longer
    // live, leaving `expiresAt` at the 0 sentinel — treated as ownership lost.
    let expiresAt = 0;
    try {
      await root._enqueueLeaseRenewal(async () => {
        const effectiveTtl = Math.max(ttlMs, root._getEffectiveLeaseTtlMs(this._leaseTtlMs));
        const lease = await this._renewSubtreeFromRoot(storage, root, effectiveTtl);
        if (opts?.propagateRootDeadline === true) root._setLeaseExtensionDeadline(lease.expiresAt);
        expiresAt = lease.expiresAt;
      });
      if (expiresAt === 0) {
        await this._evictLiveSession(session, 'lease_lost');
        throw new HarnessSessionLockedError(session.id, 'unknown', session.getRecord().leaseExpiresAt ?? 0);
      }
      return expiresAt;
    } catch (err) {
      if (err instanceof HarnessStorageLeaseConflictError || err instanceof HarnessStorageSessionNotFoundError) {
        // Lost ownership or subtree split — fence the whole live subtree.
        const subtree = [root, ...this._liveDescendantsOf(root.id)];
        for (const member of subtree) {
          if (this._liveSessions.get(member.id) === member) await this._evictLiveSession(member, 'lease_lost');
        }
        if (err instanceof HarnessStorageSessionNotFoundError) throw new HarnessSessionNotFoundError(session.id);
        throw new HarnessSessionLockedError(session.id, err.heldBy, err.expiresAt);
      }
      throw err;
    }
  }

  /**
   * §10.2: project a background/best-effort `HarnessStorageError` (one that does
   * not reach a caller's awaited promise) into a `storage_error` observer event.
   * Routed to the owning live session when known (session-scoped, fanned to
   * `harness.subscribe`), else harness-scoped.
   */
  private _emitStorageError(err: HarnessStorageError): void {
    const payload = {
      type: 'storage_error' as const,
      operation: err.operation,
      retryable: err.retryable,
      error: { code: 'harness.storage' as const, message: err.message },
      ...(err.resourceId !== undefined ? { resourceId: err.resourceId } : {}),
      ...(err.threadId !== undefined ? { threadId: err.threadId } : {}),
      ...(err.harnessName !== undefined ? { harnessName: err.harnessName } : {}),
      ...(err.channelId !== undefined ? { channelId: err.channelId } : {}),
      ...(err.subject !== undefined ? { subject: err.subject } : {}),
    };
    const session = err.sessionId ? this._liveSessions.get(err.sessionId) : undefined;
    if (session) {
      session._emit(payload);
    } else {
      this._emitter.emit({ ...payload, ...(err.sessionId !== undefined ? { sessionId: err.sessionId } : {}) });
    }
  }

  /**
   * §5.4 pressure eviction: before adding a new live session that would exceed
   * `sessions.maxLive`, evict the least-recently-active unpinned session
   * (flushing its dirty state first). If every live session is pinned (parked on
   * a pending interaction), reject with `HarnessLiveSessionLimitError` rather
   * than dropping a pending prompt. No-op when `maxLive` is `Infinity`.
   */
  private async _enforceMaxLiveCap(): Promise<void> {
    if (!Number.isFinite(this._maxLive)) return;
    while (this._liveSessions.size >= this._maxLive) {
      const victim = this._selectPressureEvictionVictim();
      if (!victim) {
        throw new HarnessLiveSessionLimitError(this._maxLive, this._liveSessions.size);
      }
      await this._evictLiveSession(victim, 'pressure');
    }
  }

  /** Least-recently-active unpinned live session, or undefined if all are pinned (§5.4). */
  private _selectPressureEvictionVictim(): Session | undefined {
    let victim: Session | undefined;
    for (const candidate of this._liveSessions.values()) {
      if (this._isPinnedSubtree(candidate)) continue;
      if (!victim || candidate.lastActivityAt < victim.lastActivityAt) victim = candidate;
    }
    return victim;
  }

  /**
   * A session is pinned while it parks on a pending interaction; a live
   * descendant that is pinned also pins its parent/root owner subtree, because
   * descendant writes share the parent/root lease (§5.4 / §5.8).
   */
  private _isPinnedSubtree(session: Session): boolean {
    if (session.isPinned()) return true;
    for (const candidate of this._liveSessions.values()) {
      if (candidate.parentSessionId === session.id && this._isPinnedSubtree(candidate)) return true;
    }
    return false;
  }

  /** Live descendants of `rootId` (BFS over `_liveSessions` by parentSessionId). */
  private _liveDescendantsOf(rootId: string): Session[] {
    const out: Session[] = [];
    const stack: string[] = [rootId];
    while (stack.length > 0) {
      const parentId = stack.pop()!;
      for (const candidate of this._liveSessions.values()) {
        if (candidate.parentSessionId === parentId) {
          out.push(candidate);
          stack.push(candidate.id);
        }
      }
    }
    return out;
  }

  /**
   * §5.8: walk a live session's parent chain to its top-level root via
   * `_liveSessions`. Returns the session itself when it is already a root, or
   * `undefined` when any ancestor is not live — i.e. the session is an orphan
   * whose root ownership cannot be proven in this process.
   */
  private _resolveLiveRoot(session: Session): Session | undefined {
    let current: Session | undefined = session;
    const seen = new Set<string>();
    while (current !== undefined && current.parentSessionId !== undefined) {
      if (seen.has(current.id)) return undefined;
      seen.add(current.id);
      current = this._liveSessions.get(current.parentSessionId);
    }
    return current;
  }

  private async _evictLiveSession(session: Session, reason: 'lease_lost' | 'pressure' | 'idle'): Promise<void> {
    if (this._liveSessions.get(session.id) !== session) return;
    // §5.4 `_enforceMaxLiveCap` flushes the victim first. Previously only event
    // persistence was drained, leaving the session `_flushChain` (buffered
    // session-record / token-usage writes) un-drained — `_markEvicted` then flips
    // the session out of the live/closing state, after which `_persistTokenUsageDelta`
    // short-circuits and any still-buffered write is dropped. Drain the flush chain
    // first so those writes settle while the session is still live.
    //
    // Scope: ONLY the `pressure`/`idle` evictions, which run from the cap enforcer
    // and the idle reaper — paths NOT re-entrant to a flush. The `lease_lost`
    // eviction is reached from INSIDE `_flushUpdate` (a lease conflict fences the
    // subtree mid-flush), so awaiting the flush chain there would await the very
    // chain we are a link in → deadlock. Lease-lost writes are also already doomed
    // (the lease is gone; the flush is failing), so there is nothing useful to drain.
    //
    // Best-effort + bounded: wrapped like the event flush below (a victim under
    // memory pressure may still have a failing/slow write), and capped by a short
    // deadline so a stuck storage write can never block this hot path. Latched
    // durability errors surface through `shutdown()`, not eviction.
    if (reason === 'pressure' || reason === 'idle') {
      try {
        await session._internalAwaitFlushChain({ deadlineAt: Date.now() + EVICTION_FLUSH_DRAIN_BUDGET_MS });
      } catch {
        // Best-effort: do not block or fail eviction on a drained-but-failed/slow flush.
      }
      // Re-check the live-map guard after the drain `await`. The drained flush
      // chain can hit a lease conflict mid-drain and self-evict this very session
      // via `_internalEvictSubtreeLeaseLost` -> `_evictLiveSession(this,'lease_lost')`,
      // which already ran `_markEvicted` + emitted `session_evicted` + deleted the
      // live entry + tore down the bridge. Bailing here avoids a duplicate
      // `session_evicted` emit and redundant teardown when this pressure/idle path
      // resumes after its drain. (The lease_lost path skips this drain entirely.)
      if (this._liveSessions.get(session.id) !== session) return;
    }
    const record = session.getRecord() as SessionRecord;
    session._markEvicted(record);
    session._emit({ type: 'session_evicted', reason });
    try {
      await session._flushEventPersistence();
    } catch {
      // Best-effort: lease loss may also mean event persistence is unavailable.
    }
    const bridge = this._sessionEventBridges.get(session.id);
    if (bridge) {
      bridge();
      this._sessionEventBridges.delete(session.id);
    }
    this._liveSessions.delete(session.id);
    this._stopLeaseRenewalLoopIfIdle();

    try {
      if (this._workspaceKind === 'per-session') {
        await this._workspaceRegistry.releasePerSession({ sessionId: session.id });
      } else if (this._workspaceKind === 'per-resource') {
        await this._workspaceRegistry.releasePerResource({ resourceId: session.resourceId });
      }
    } catch {
      // Best-effort; workspace registry errors are surfaced through events elsewhere.
    }
  }

  private async _eventReplaySeedFor(
    storage: HarnessStorage,
    record: SessionRecord,
  ): Promise<{ epoch: string; nextSequence: number } | undefined> {
    // §10.5: when transient streaming deltas are NOT persisted, the persisted
    // `newestSequence` undercounts the true emitted seq (skipped deltas advanced
    // the live counter), so seeding `newestSequence + 1` would REUSE seq numbers a
    // reconnecting client already saw on the prior epoch → silent event-id collision.
    // Don't reuse the cursor: each rehydrate mints a fresh epoch, so a reconnect with
    // a prior-epoch Last-Event-ID gets a 412 `stale_epoch` and recovers via the
    // snapshot/message path (durable SSE replay across restarts is not a v1 goal).
    if (!this._persistTransientStreamingEvents) return undefined;
    try {
      const state = await storage.getSessionEventReplayState({
        harnessName: record.harnessName,
        sessionId: record.id,
        resourceId: record.resourceId,
        threadId: record.threadId,
      });
      if (!state) return undefined;
      return { epoch: state.epoch, nextSequence: state.newestSequence + 1 };
    } catch (err) {
      if (err instanceof HarnessStorageSessionEventReplayUnsupportedError) return undefined;
      throw new HarnessStorageError({ operation: 'session_load', sessionId: record.id, cause: err });
    }
  }

  private async _acquireLease(storage: HarnessStorage, harnessName: string, sessionId: string) {
    // §5.8 lock acquisition under contention. `lockMode: 'wait'` blocks up to
    // `lockWaitMs`, re-acquiring once the held lease expires (never authorizing
    // from a cached `expiresAt` — always re-attempt). `'fail'` rejects
    // immediately with the current owner. (`'steal'` is reserved/rejected at
    // construction until the operator fence is implemented.)
    const waitDeadline = Date.now() + this._lockWaitMs;
    for (;;) {
      try {
        return await storage.acquireSessionLease({
          harnessName,
          sessionId,
          ownerId: this.ownerId,
          ttlMs: this._leaseTtlMs,
        });
      } catch (err) {
        if (err instanceof HarnessStorageLeaseConflictError) {
          const now = Date.now();
          if (this._lockMode !== 'wait' || now >= waitDeadline) {
            throw new HarnessSessionLockedError(sessionId, err.heldBy, err.expiresAt);
          }
          // Wait until the held lease expires (bounded by the wait deadline),
          // then re-attempt acquisition against authoritative storage state.
          const wakeAt = Math.min(err.expiresAt, waitDeadline);
          await new Promise<void>(resolve => setTimeout(resolve, Math.max(1, wakeAt - now)));
          continue;
        }
        if (err instanceof HarnessStorageSessionNotFoundError) {
          throw new HarnessSessionNotFoundError(sessionId);
        }
        throw new HarnessStorageError({ operation: 'session_lease_acquire', sessionId, cause: err });
      }
    }
  }

  // -------------------------------------------------------------------------
  // Lifecycle.
  // -------------------------------------------------------------------------

  /**
   * Soft-close: persist closingAt, reject new work, drain admitted turns until
   * each close deadline, terminalize descendants bottom-up, set closedAt,
   * release leases, and drop live instances. Idempotent. See §5.5.
   */
  async closeSession(opts: { sessionId: string; resourceId?: string }): Promise<void> {
    if (this._shutdown) return;
    const storage = this._requireStorage('closeSession()');
    const live = this._liveSessions.get(opts.sessionId);
    if (live) {
      if (opts.resourceId !== undefined && live.resourceId !== opts.resourceId) {
        throw new HarnessSessionNotFoundError(opts.sessionId);
      }
      await this._closeSession(live);
      return;
    }
    const stored = await storage.loadSession({ harnessName: this._harnessName, sessionId: opts.sessionId });
    if (!stored) throw new HarnessSessionNotFoundError(opts.sessionId);
    if (opts.resourceId !== undefined && stored.resourceId !== opts.resourceId) {
      throw new HarnessSessionNotFoundError(opts.sessionId);
    }
    if (stored.closedAt !== undefined) return; // already closed → idempotent.
    await this._closeSessionRecord(storage, stored, undefined, { resourceId: opts.resourceId });
  }

  /**
   * Hard-delete a session subtree. Non-force delete is a closed-record cleanup
   * path; force delete first reuses the close cascade so pending local work is
   * terminalized before storage rows and owned attachments are removed.
   */
  async deleteSession(opts: SessionDeleteOptions): Promise<void> {
    if (this._shutdown) return;
    const storage = this._requireStorage('deleteSession()');
    const stored = await storage.loadSession({ harnessName: this._harnessName, sessionId: opts.sessionId });
    if (!stored) throw new HarnessSessionNotFoundError(opts.sessionId);
    if (stored.resourceId !== opts.resourceId) {
      throw new HarnessSessionNotFoundError(opts.sessionId);
    }

    if (opts.force) {
      await this._forceDeleteSessionRecord(storage, stored, () => !this._shutdown, { resourceId: opts.resourceId });
      return;
    }

    const tree = await this._collectDeleteTree(storage, stored);
    const blockers = this._collectDeleteBlockers(tree);
    if (blockers.length > 0) {
      throw new HarnessSessionDeleteBlockedError(stored.id, blockers);
    }
    if (this._shutdown) return;
    await this._deleteClosedTree(storage, tree);
  }

  /**
   * @internal — backs `Session.clone()` (§4.1 / §2.2). Clones the source's
   * backing thread (copying committed message history + `ThreadCloneMetadata`
   * provenance), then mints a new session that OWNS the cloned thread. Per §5.2a
   * the clone copies NO SessionRecord: mode/model reset to the harness default
   * (legacy mode/model defaults are explicitly NOT copied from the source), with
   * empty grants/state/pending. Lease, live handles, pending queue/inbox, and
   * in-flight tool execution are never copied — the clone is a fresh runtime room
   * over the copied transcript.
   */
  async _cloneSession(source: Session, opts?: { title?: string; copyAppMetadata?: boolean }): Promise<Session> {
    const storage = this._requireStorage('clone()');
    const clonedThread = await this._threadOps.clone({
      resourceId: source.resourceId,
      threadId: source.threadId,
      ...(opts?.title ? { title: opts.title } : {}),
    });
    // §5.2a: the clone copies NO SessionRecord — the new session is a fresh
    // runtime room (harness-default mode/model, empty grants/state/pending) over
    // the copied committed history, and owns the cloned thread. `copyAppMetadata`
    // selects whether app-owned thread metadata carries over; its strict
    // `metadata.app`-only gating lands with the metadata.app slice (the
    // underlying memory.cloneThread currently copies thread metadata regardless).
    void opts?.copyAppMetadata;
    return this._createFresh(storage, {
      resourceId: source.resourceId,
      threadId: clonedThread.id,
      ownsThread: true,
      origin: 'top-level',
    });
  }

  /**
   * @internal — used by `Session.close()` and `Harness.closeSession()`.
   */
  async _closeSession(session: Session): Promise<void> {
    if (this._shutdown) return;
    if (session.isClosed) return;

    const storage = this._requireStorage('closeSession()');
    await this._closeSessionRecord(storage, session.getRecord());
  }

  private async _closeSessionRecord(
    storage: HarnessStorage,
    rootRecord: SessionRecord,
    closedLiveSessions?: Map<string, Session>,
    scope: { resourceId?: string } = {},
  ): Promise<void> {
    const existing = this._closePromises.get(rootRecord.id);
    if (existing) return existing;

    const closeIds = new Set<string>([rootRecord.id]);
    const persistedCloseIds = new Set<string>();
    this._liveSessions.get(rootRecord.id)?._beginClosing();
    let closePromise!: Promise<void>;
    closePromise = Promise.resolve()
      .then(() =>
        this._closeSessionRecordOnce(
          storage,
          rootRecord,
          closeIds,
          persistedCloseIds,
          closePromise,
          closedLiveSessions,
          scope,
        ),
      )
      .catch(err => {
        for (const id of closeIds) {
          if (!persistedCloseIds.has(id)) {
            this._liveSessions.get(id)?._restoreLiveAfterFailedClose();
          }
        }
        throw err;
      })
      .finally(() => {
        for (const id of closeIds) {
          if (this._closePromises.get(id) === closePromise) {
            this._closePromises.delete(id);
          }
        }
      });
    this._closePromises.set(rootRecord.id, closePromise);
    return closePromise;
  }

  private async _closeSessionRecordOnce(
    storage: HarnessStorage,
    rootRecord: SessionRecord,
    closeIds: Set<string>,
    persistedCloseIds: Set<string>,
    closePromise: Promise<void>,
    closedLiveSessions?: Map<string, Session>,
    scope: { resourceId?: string } = {},
  ): Promise<void> {
    const tree: CloseTreeNode[] = [];
    try {
      const root = await this._prepareCloseNode(storage, rootRecord, 0, scope);
      if (root.record.closedAt !== undefined) {
        await this._releaseCloseTreeLeases(storage, [root]);
        return;
      }
      tree.push(root);
      tree[0] = await this._markCloseNodeClosing(storage, root, persistedCloseIds);
      // §5.5: the close deadline is ONE fixed value for the whole subtree. The
      // root computed/preserved its `closeDeadlineAt` above; capture it once and
      // propagate that single deadline to every descendant marked during the BFS
      // walk and to the drain wait, so a fresh descendant never stamps its own
      // (later) wall-clock `now + closeTimeoutMs` and inflate the total wait.
      const subtreeCloseDeadlineAt = tree[0]!.record.closeDeadlineAt ?? Date.now() + this._closeTimeoutMs;

      for (let index = 0; index < tree.length; index++) {
        const node = tree[index]!;
        const children = await storage.listSessions({
          harnessName: node.record.harnessName,
          resourceId: root.record.resourceId,
          includeClosed: false,
          parentSessionId: node.record.id,
        });
        for (const child of children) {
          const stored = await storage.loadSession({ harnessName: node.record.harnessName, sessionId: child.id });
          if (!stored || stored.closedAt !== undefined) continue;
          const existingClose = this._closePromises.get(stored.id);
          if (existingClose && existingClose !== closePromise) {
            await existingClose;
            continue;
          }
          closeIds.add(stored.id);
          this._closePromises.set(stored.id, closePromise);
          const childNode = await this._prepareCloseNode(storage, stored, node.depth + 1, {
            resourceId: root.record.resourceId,
          });
          if (childNode.record.closedAt !== undefined) {
            closeIds.delete(stored.id);
            if (this._closePromises.get(stored.id) === closePromise) {
              this._closePromises.delete(stored.id);
            }
            await this._releaseCloseTreeLeases(storage, [childNode]);
            continue;
          }
          childNode.live?._beginClosing();
          tree.push(childNode);
          tree[tree.length - 1] = await this._markCloseNodeClosing(
            storage,
            childNode,
            persistedCloseIds,
            subtreeCloseDeadlineAt,
          );
        }
      }

      await this._drainCloseTree(tree, subtreeCloseDeadlineAt);
      await this._terminalizeCloseTree(storage, tree, closedLiveSessions);
    } catch (err) {
      await this._releaseCloseTreeLeases(storage, tree);
      throw err;
    }
  }

  private async _prepareCloseNode(
    storage: HarnessStorage,
    record: SessionRecord,
    depth: number,
    scope: { resourceId?: string } = {},
  ): Promise<CloseTreeNode> {
    const live = this._liveSessions.get(record.id);
    if (live) {
      if (scope.resourceId !== undefined && live.resourceId !== scope.resourceId) {
        throw new HarnessSessionNotFoundError(record.id);
      }
      return {
        record: live.getRecord(),
        depth,
        live,
        leaseAcquired: false,
      };
    }

    const lease = await this._acquireLease(storage, record.harnessName, record.id);
    let latest: SessionRecord | null;
    try {
      latest = await storage.loadSession({ harnessName: record.harnessName, sessionId: record.id });
      if (!latest) throw new HarnessSessionNotFoundError(record.id);
      if (scope.resourceId !== undefined && latest.resourceId !== scope.resourceId) {
        throw new HarnessSessionNotFoundError(record.id);
      }
    } catch (err) {
      try {
        await storage.releaseSessionLease({
          harnessName: record.harnessName,
          sessionId: record.id,
          ownerId: this.ownerId,
        });
      } catch {
        // Preserve the original close failure. The lease release is best-effort
        // because the row may have disappeared or another owner may have won.
      }
      throw err;
    }
    const leasedRecord = {
      ...latest,
      ownerId: this.ownerId,
      leaseExpiresAt: lease.expiresAt,
      version: lease.version,
    };
    if ((leasedRecord.pendingQueue?.length ?? 0) > 0) {
      const recovered = this._adoptSession(storage, leasedRecord, {
        emitCreated: false,
        kickQueueDrain: false,
        eventReplaySeed: await this._eventReplaySeedFor(storage, leasedRecord),
      });
      return {
        record: recovered.getRecord(),
        depth,
        live: recovered,
        leaseAcquired: true,
      };
    }
    return {
      record: leasedRecord,
      depth,
      leaseAcquired: true,
    };
  }

  private async _markCloseNodeClosing(
    storage: HarnessStorage,
    node: CloseTreeNode,
    persistedCloseIds: Set<string>,
    // §5.5: the ONE fixed deadline for the whole close subtree, stamped once at
    // the root and propagated to every descendant. `undefined` only for the root
    // node, which derives its own deadline from `closingAt + closeTimeoutMs` (or
    // a deadline already persisted on a resumed close).
    subtreeCloseDeadlineAt?: number,
  ): Promise<CloseTreeNode> {
    const closingAt = node.record.closingAt ?? Date.now();
    const closeDeadlineAt =
      node.record.closeDeadlineAt ?? subtreeCloseDeadlineAt ?? closingAt + this._closeTimeoutMs;
    if (node.live) {
      let record: SessionRecord;
      try {
        record = await node.live._flushClosingMarker({
          closeTimeoutMs: this._closeTimeoutMs,
          closeDeadlineAt: subtreeCloseDeadlineAt,
        });
      } catch (err) {
        throw new HarnessStorageError({ operation: 'session_close', sessionId: node.record.id, cause: err });
      }
      persistedCloseIds.add(record.id);
      this._emitSessionClosing(node.live, record);
      return { ...node, record };
    }

    const next: SessionRecord = {
      ...node.record,
      closingAt,
      closeDeadlineAt,
      lastActivityAt: Date.now(),
    };
    try {
      const saved = await storage.saveSession(next, {
        harnessName: next.harnessName,
        ownerId: this.ownerId,
        ifVersion: node.record.version,
      });
      next.version = saved.version;
    } catch (err) {
      throw new HarnessStorageError({ operation: 'session_close', sessionId: next.id, cause: err });
    }

    persistedCloseIds.add(next.id);
    this._emitSessionClosing(undefined, next);
    return { ...node, record: next };
  }

  private async _drainCloseTree(tree: CloseTreeNode[], subtreeCloseDeadlineAt: number): Promise<void> {
    // §5.5: every node drains against the SINGLE subtree deadline, not its own
    // per-node `closeDeadlineAt`. A descendant marked mid-walk carries the same
    // root deadline, so the total close wait is bounded by `closeTimeoutMs` and
    // does not grow with subtree depth.
    await Promise.all(
      tree.map(node => {
        if (!node.live) return undefined;
        return node.live._waitForCloseDrain(subtreeCloseDeadlineAt);
      }),
    );
  }

  private async _terminalizeCloseTree(
    storage: HarnessStorage,
    tree: CloseTreeNode[],
    closedLiveSessions?: Map<string, Session>,
  ): Promise<void> {
    const bottomUp = [...tree].sort((a, b) => b.depth - a.depth);
    for (const node of bottomUp) {
      if (node.record.closedAt !== undefined) {
        await this._releaseClosedSessionResources(storage, node.record, node.live, closedLiveSessions);
        continue;
      }
      const closedAt = Date.now();
      let closed: SessionRecord;
      if (node.live) {
        try {
          closed = await node.live._flushClosedMarker(closedAt);
        } catch (err) {
          throw new HarnessStorageError({ operation: 'session_close', sessionId: node.record.id, cause: err });
        }
      } else {
        closed = {
          ...node.record,
          lastActivityAt: closedAt,
          closedAt,
        };
        try {
          const saved = await storage.saveSession(closed, {
            harnessName: closed.harnessName,
            ownerId: this.ownerId,
            ifVersion: node.record.version,
          });
          closed.version = saved.version;
        } catch (err) {
          throw new HarnessStorageError({ operation: 'session_close', sessionId: closed.id, cause: err });
        }
      }
      await this._releaseClosedSessionResources(storage, closed, node.live, closedLiveSessions);
    }
  }

  private async _releaseCloseTreeLeases(storage: HarnessStorage, tree: CloseTreeNode[]): Promise<void> {
    for (const node of tree) {
      if (!node.leaseAcquired) continue;
      try {
        await storage.releaseSessionLease({
          harnessName: node.record.harnessName,
          sessionId: node.record.id,
          ownerId: this.ownerId,
        });
      } catch {
        // Best effort; the lease still expires by TTL.
      }
      if (node.live && this._liveSessions.get(node.record.id) === node.live) {
        node.live._markEvicted(node.record);
        const bridge = this._sessionEventBridges.get(node.record.id);
        if (bridge) {
          bridge();
          this._sessionEventBridges.delete(node.record.id);
        }
        this._liveSessions.delete(node.record.id);
      }
    }
    this._stopLeaseRenewalLoopIfIdle();
  }

  private _emitSessionClosing(session: Session | undefined, record: SessionRecord): void {
    const event = {
      type: 'session_closing' as const,
      reason: 'requested' as const,
      closingAt: record.closingAt!,
      closeDeadlineAt: record.closeDeadlineAt!,
    };
    if (session) {
      session._emit(event);
      return;
    }
    this._emitter.emit(event, { sessionId: record.id });
  }

  private async _releaseClosedSessionResources(
    storage: HarnessStorage,
    record: SessionRecord,
    session: Session | undefined,
    closedLiveSessions?: Map<string, Session>,
  ): Promise<void> {
    let eventPersistenceError: unknown;
    if (session) {
      closedLiveSessions?.set(record.id, session);
      session._markClosed(record);
      // Emit session_closed BEFORE we tear down the per-session bridge so
      // harness-level subscribers see the lifecycle event for this session.
      // The session's own emitter is still wired and will publish to the
      // bridge before the unsubscribe lands.
      session._emit({ type: 'session_closed', reason: 'requested' });
      try {
        await session._flushEventPersistence();
      } catch (err) {
        eventPersistenceError = err;
      }
    } else {
      this._emitter.emit({ type: 'session_closed', reason: 'requested' }, { sessionId: record.id });
    }

    try {
      await storage.releaseSessionLease({
        harnessName: record.harnessName,
        sessionId: record.id,
        ownerId: this.ownerId,
      });
    } catch {
      // Release is best-effort — record is already closed and the lease
      // will TTL out either way.
    }

    // Release the session's workspace under the configured ownership model.
    // `shared` is owned by the harness; nothing to release here.
    try {
      if (this._workspaceKind === 'per-session') {
        await this._workspaceRegistry.releasePerSession({ sessionId: record.id });
      } else if (this._workspaceKind === 'per-resource') {
        await this._workspaceRegistry.releasePerResource({ resourceId: record.resourceId });
      }
    } catch {
      // Best-effort — registry surfaces errors via workspace_error event.
    }

    const bridge = this._sessionEventBridges.get(record.id);
    if (bridge) {
      bridge();
      this._sessionEventBridges.delete(record.id);
    }
    this._liveSessions.delete(record.id);
    this._stopLeaseRenewalLoopIfIdle();

    if (eventPersistenceError !== undefined) {
      throw new HarnessStorageError({ operation: 'session_save', sessionId: record.id, cause: eventPersistenceError });
    }
  }

  private async _forceDeleteSessionRecord(
    storage: HarnessStorage,
    rootRecord: SessionRecord,
    shouldContinue: () => boolean = () => true,
    scope: { resourceId?: string } = {},
  ): Promise<SessionRecord[]> {
    const latest = await storage.loadSession({ harnessName: rootRecord.harnessName, sessionId: rootRecord.id });
    if (!latest) return [];
    if (scope.resourceId !== undefined && latest.resourceId !== scope.resourceId) {
      throw new HarnessSessionNotFoundError(rootRecord.id);
    }
    const preCloseTree = await this._collectDeleteTree(storage, latest);
    const liveDeleteHandles = new Map<string, Session>();
    for (const node of preCloseTree) {
      const live = this._liveSessions.get(node.record.id);
      if (live) liveDeleteHandles.set(node.record.id, live);
    }
    if (latest.closedAt === undefined) {
      await this._closeSessionRecord(storage, latest, liveDeleteHandles, scope);
      if (!shouldContinue()) return [];
    }
    const closed = await storage.loadSession({ harnessName: rootRecord.harnessName, sessionId: rootRecord.id });
    if (!closed) return [];
    if (scope.resourceId !== undefined && closed.resourceId !== scope.resourceId) {
      throw new HarnessSessionNotFoundError(rootRecord.id);
    }
    const tree = await this._collectDeleteTree(storage, closed);
    if (!shouldContinue()) return [];
    const deleted = tree.map(node => node.record);
    await this._deleteClosedTree(storage, tree, liveDeleteHandles);
    return deleted;
  }

  private async _collectDeleteTree(storage: HarnessStorage, rootRecord: SessionRecord): Promise<CloseTreeNode[]> {
    const tree: CloseTreeNode[] = [{ record: rootRecord, depth: 0, leaseAcquired: false }];
    const seen = new Set<string>([rootRecord.id]);
    for (let index = 0; index < tree.length; index++) {
      const node = tree[index]!;
      const children = await storage.listSessions({
        harnessName: node.record.harnessName,
        resourceId: rootRecord.resourceId,
        includeClosed: true,
        parentSessionId: node.record.id,
      });
      for (const child of children) {
        if (seen.has(child.id)) continue;
        const stored = await storage.loadSession({ harnessName: node.record.harnessName, sessionId: child.id });
        if (!stored) continue;
        if (stored.resourceId !== rootRecord.resourceId) continue;
        seen.add(stored.id);
        tree.push({ record: stored, depth: node.depth + 1, leaseAcquired: false });
      }
    }
    return tree;
  }

  private _collectDeleteBlockers(tree: CloseTreeNode[]): HarnessSessionDeleteBlocker[] {
    const blockers: HarnessSessionDeleteBlocker[] = [];
    for (const node of tree) {
      const record = node.record;
      // §4.5b structured blockers: the root of the delete tree is the `session`;
      // descendants are `child_session`. Queue/inbox dependents carry the
      // owning row id + its status.
      const sessionSource: HarnessSessionDeleteBlocker['source'] = node.depth === 0 ? 'session' : 'child_session';
      if (record.closedAt === undefined) blockers.push({ source: sessionSource, id: record.id, status: 'not_closed' });
      if ((record.pendingQueue?.length ?? 0) > 0) {
        blockers.push({ source: 'queue', id: record.id, status: 'pending_queue' });
      }
      if (record.pendingResume !== undefined) {
        blockers.push({ source: 'inbox_response', id: record.id, status: 'pending_resume' });
      }
      for (const receipt of Object.values(record.queueAdmissionReceipts ?? {})) {
        if (
          receipt.status === 'queued' ||
          receipt.status === 'admitting' ||
          receipt.status === 'accepted' ||
          (receipt.status === 'completed' && receipt.postRunFinalizedAt === undefined)
        ) {
          blockers.push({ source: 'queue', id: receipt.queuedItemId, status: receipt.status });
        }
      }
      for (const receipt of Object.values(record.inboxResponseReceipts ?? {})) {
        if (receipt.status === 'accepted' || receipt.retryable === true) {
          blockers.push({ source: 'inbox_response', id: receipt.responseId, status: receipt.status });
        }
      }
    }
    return blockers;
  }

  private async _deleteClosedTree(
    storage: HarnessStorage,
    tree: CloseTreeNode[],
    deletedLiveSessions = new Map<string, Session>(),
  ): Promise<void> {
    const bottomUp = [...tree].sort((a, b) => b.depth - a.depth);
    const sessions = bottomUp.map(node => ({
      harnessName: node.record.harnessName,
      sessionId: node.record.id,
      ifVersion: node.record.version,
      expectedResourceId: node.record.resourceId,
      expectedThreadId: node.record.threadId,
      expectedParentSessionId: node.record.parentSessionId ?? null,
      expectedCreatedAt: node.record.createdAt,
      requireClosed: true,
    }));
    if (storage.supportsAtomicDeleteSessions) {
      try {
        await storage.deleteSessions({ sessions });
      } catch (err) {
        for (const node of bottomUp) {
          let stillExists: SessionRecord | null;
          try {
            stillExists = await storage.loadSession({
              harnessName: node.record.harnessName,
              sessionId: node.record.id,
            });
          } catch {
            continue;
          }
          if (stillExists) continue;
          const live = this._markDeletedSession(node, deletedLiveSessions);
          // Preserve the original guarded-delete error; the caller already sees
          // this delete attempt as failed, and retry/reconciliation can clean up
          // any remaining operation evidence from this live session's active turn.
          await this._cleanupDeletedOperationEvidence(storage, node.record, live).catch(() => {});
        }
        throw err;
      }
      for (const node of bottomUp) {
        const live = this._markDeletedSession(node, deletedLiveSessions);
        await this._cleanupDeletedOperationEvidence(storage, node.record, live).catch(() => {});
      }
      return;
    }
    for (let index = 0; index < bottomUp.length; index++) {
      await storage.deleteSession(sessions[index]!);
      const live = this._markDeletedSession(bottomUp[index]!, deletedLiveSessions);
      await this._cleanupDeletedOperationEvidence(storage, bottomUp[index]!.record, live).catch(() => {});
    }
  }

  private async _cleanupDeletedOperationEvidence(
    storage: HarnessStorage,
    record: SessionRecord,
    live: Session | undefined,
  ): Promise<void> {
    for (const signalId of live?._deletedOperationEvidenceSignalIds() ?? []) {
      await storage.deleteOperationAdmissionTombstonesForSession({
        harnessName: record.harnessName,
        sessionId: record.id,
        resourceId: record.resourceId,
        threadId: record.threadId,
        signalId,
      });
    }
  }

  private _markDeletedSession(node: CloseTreeNode, deletedLiveSessions: Map<string, Session>): Session | undefined {
    const live = this._liveSessions.get(node.record.id) ?? deletedLiveSessions.get(node.record.id);
    live?._markDeleted();
    const bridge = this._sessionEventBridges.get(node.record.id);
    if (bridge) {
      bridge();
      this._sessionEventBridges.delete(node.record.id);
    }
    this._liveSessions.delete(node.record.id);
    this._stopLeaseRenewalLoopIfIdle();
    return live;
  }

  /**
   * Read-only listing of session records for a resource. Closed records are
   * excluded unless `includeClosed: true`.
   */
  async listSessions(opts: SessionListOptions & { parentSessionId?: string }): Promise<SessionSummary[]> {
    const storage = this._requireStorage('listSessions()');
    return storage.listSessions({
      harnessName: this._harnessName,
      resourceId: opts.resourceId,
      includeClosed: opts.includeClosed,
      parentSessionId: opts.parentSessionId,
    });
  }

  /**
   * Inspect a single record by id. Returns `null` if no record exists; does
   * not throw on closed records (this is the inspection path). The active
   * resolver throws for closed; this method returns them when requested.
   */
  async loadSession(opts: SessionLoadByIdOptions): Promise<SessionRecord | null> {
    const storage = this._requireStorage('loadSession()');
    const stored = await storage.loadSession({ harnessName: this._harnessName, sessionId: opts.sessionId });
    if (!stored) return null;
    if (stored.closedAt !== undefined && !opts.includeClosed) return null;
    return stored;
  }

  async lookupMessageResult(opts: {
    sessionId: string;
    resourceId: string;
    signalId: string;
  }): Promise<AgentSignalResultStatus | OperationAdmissionTombstone | null> {
    const storage = this._requireStorage('lookupMessageResult()');
    const stored = await storage.loadSession({ harnessName: this._harnessName, sessionId: opts.sessionId });
    if (!stored || stored.resourceId !== opts.resourceId) return null;
    return storage.loadMessageResultEvidence({
      harnessName: stored.harnessName,
      sessionId: stored.id,
      resourceId: stored.resourceId,
      threadId: stored.threadId,
      signalId: opts.signalId,
    });
  }

  async lookupQueueResult(opts: {
    sessionId: string;
    resourceId: string;
    queuedItemId: string;
  }): Promise<QueueAdmissionReceipt | OperationAdmissionTombstone | null> {
    const storage = this._requireStorage('lookupQueueResult()');
    const stored = await storage.loadSession({ harnessName: this._harnessName, sessionId: opts.sessionId });
    if (!stored || stored.resourceId !== opts.resourceId) return null;
    return storage.loadQueueResultEvidence({
      harnessName: stored.harnessName,
      sessionId: stored.id,
      resourceId: stored.resourceId,
      queuedItemId: opts.queuedItemId,
    });
  }

  /**
   * Drain in-flight work and release every held lease. After `shutdown`,
   * `session()` rejects. Idempotent.
   */
  async shutdown(opts?: ShutdownOptions): Promise<void> {
    if (this._shutdown) return;
    this._shutdown = true;
    this._stopLeaseRenewalLoop();
    this._stopChannelInboxRecoveryLoop();

    let storage: HarnessStorage;
    try {
      storage = this._requireStorage('shutdown()');
    } catch {
      // No storage bound — nothing to release. Idempotent.
      this._liveSessions.clear();
      try {
        await this._workspaceRegistry.shutdown();
      } catch {
        // Best-effort: errors surface through the workspace_error event.
      }
      this._untrackBoundStorage();
      return;
    }

    const pendingCloses = new Set(this._closePromises.values());
    if (pendingCloses.size > 0) {
      await Promise.allSettled(pendingCloses);
    }

    // Release every held lease. We keep the records active in storage —
    // shutdown is not a close.
    const sessions = Array.from(this._liveSessions.values());
    for (const session of sessions) {
      session._beginClosing();
    }
    const drainTimeoutMs = opts?.drainTimeoutMs ?? this._closeTimeoutMs;
    const drainDeadlineAt = Date.now() + drainTimeoutMs;
    let eventPersistenceError: { sessionId: string; error: unknown } | undefined;
    for (const session of sessions) {
      try {
        await session._waitForShutdownDrain(drainDeadlineAt);
      } catch (err) {
        eventPersistenceError ??= { sessionId: session.id, error: err };
        continue;
      }
      try {
        await session._internalPersistTokenUsageForShutdown({ deadlineAt: drainDeadlineAt });
        await session._internalAwaitFlushChain({ deadlineAt: drainDeadlineAt });
      } catch (err) {
        eventPersistenceError ??= { sessionId: session.id, error: err };
        continue;
      }
      try {
        await session._flushEventPersistence();
      } catch (err) {
        eventPersistenceError ??= { sessionId: session.id, error: err };
        continue;
      }
    }
    if (eventPersistenceError !== undefined) {
      for (const session of sessions) {
        session._restoreLiveAfterFailedClose();
      }
      this._shutdown = false;
      if (this._liveSessions.size > 0) {
        this._ensureLeaseRenewalLoop();
      }
      // §14.2: the recovery worker starts on BIND (independent of live sessions),
      // so a rolled-back shutdown must resume it or channel recovery stays dead
      // and channelWorkerReadiness() reports worker_not_started. Self-gates on
      // no-channels / already-running.
      this._ensureChannelInboxRecoveryLoop();
      throw new HarnessStorageError({ operation: 'session_save', sessionId: eventPersistenceError.sessionId, cause: eventPersistenceError.error });
    }

    for (const session of sessions) {
      // Surface eviction to harness-level subscribers after admitted turn work
      // drains so replay observes terminal turn events before the handoff marker.
      if (!this._shutdownEvictedSessionIds.has(session.id)) {
        session._emit({ type: 'session_evicted', reason: 'shutdown' });
        this._shutdownEvictedSessionIds.add(session.id);
      }
      try {
        await session._flushEventPersistence();
      } catch (err) {
        eventPersistenceError ??= { sessionId: session.id, error: err };
        continue;
      }
    }
    if (eventPersistenceError !== undefined) {
      for (const session of sessions) {
        session._restoreLiveAfterFailedClose();
      }
      this._shutdown = false;
      if (this._liveSessions.size > 0) {
        this._ensureLeaseRenewalLoop();
      }
      // §14.2: the recovery worker starts on BIND (independent of live sessions),
      // so a rolled-back shutdown must resume it or channel recovery stays dead
      // and channelWorkerReadiness() reports worker_not_started. Self-gates on
      // no-channels / already-running.
      this._ensureChannelInboxRecoveryLoop();
      throw new HarnessStorageError({ operation: 'session_save', sessionId: eventPersistenceError.sessionId, cause: eventPersistenceError.error });
    }

    for (const session of sessions) {
      try {
        await storage.releaseSessionLease({
          harnessName: session.getRecord().harnessName,
          sessionId: session.id,
          ownerId: this.ownerId,
        });
      } catch {
        // Best-effort: leases TTL out anyway.
      }

      const bridge = this._sessionEventBridges.get(session.id);
      if (bridge) {
        bridge();
        this._sessionEventBridges.delete(session.id);
      }
      this._liveSessions.delete(session.id);
    }
    this._liveSessions.clear();
    this._shutdownEvictedSessionIds.clear();

    // Tear down every provisioned workspace (shared + per-resource + per-session).
    try {
      await this._workspaceRegistry.shutdown();
    } catch {
      // Best-effort: workspace teardown errors are swallowed (provider-owned;
      // §2.7/§10.2 define no public workspace event).
    }
    this._untrackBoundStorage();
    // §10.2: harness-scoped process-shutdown marker. No sessionId → delivered to
    // `harness.subscribe(...)` only, never per-session SSE replay. Sessions persist.
    this._emitter.emit({ type: 'harness_shutdown' });
  }

  // -------------------------------------------------------------------------
  // Thread API (sidebar surface). See HARNESS_V1_SPEC.md §4.4 + §5.2.
  //
  // Threads are the durable artifact (message log + title), distinct from
  // the runtime Session. Every operation is resource-scoped — cross-resource
  // existence is never leaked. `delete` cascades through closeSession logic so
  // leases are released and child sessions are torn
  // down before the thread + messages are removed.
  // -------------------------------------------------------------------------
  // §0/§4.1/§11.6e/§13: thread CRUD is NOT a public app-facing surface — there
  // is no `harness.threads.*` lifecycle namespace. These operations live behind
  // an internal/operator boundary: internal callers use them directly, and
  // server/operator routes reach them through the `@internal`
  // `createHarnessOperatorThreadController(harness)` factory. Normal product
  // lifecycle is `Session.close()` / `Session.delete()` / `Session.rename()`.
  // @internal
  _threadOps = {
    create: async (opts: ThreadCreateOptions): Promise<ThreadRecord> => {
      const memory = await this._requireMemoryStorage('threads.create()');
      assertNoHarnessInternalThreadMetadata(opts.metadata, 'threads.create().metadata');
      const now = new Date();
      const threadId = opts.threadId ?? this._mintThreadId();
      const saveThread = (existing?: ThreadRecord | null) => {
        const metadata = {
          ...((existing?.metadata as Record<string, unknown> | undefined) ?? {}),
          ...((opts.metadata as Record<string, unknown> | undefined) ?? {}),
        };
        return memory.saveThread({
          thread: {
            id: threadId,
            resourceId: opts.resourceId,
            title: opts.title,
            createdAt: existing?.createdAt ?? now,
            updatedAt: now,
            metadata: Object.keys(metadata).length > 0 ? metadata : undefined,
          },
        });
      };
      const saveThreadIfVisibleOwner = async (fence?: { assertActive(): Promise<void> }) => {
        const existing = await memory.getThreadById({ threadId });
        if (existing && existing.resourceId !== opts.resourceId) {
          throw new HarnessThreadNotFoundError(opts.resourceId, threadId);
        }
        await fence?.assertActive();
        return saveThread(existing);
      };
      let thread;
      if (opts.threadId === undefined) {
        thread = await saveThread();
      } else {
        try {
          thread = await this._requireStorage('threads.create()').withThreadDeleteFence(
            {
              threadId,
              ownerId: `${this.ownerId}:thread-create:${randomUUID()}`,
              ttlMs: Math.max(this._closeTimeoutMs, this._leaseTtlMs),
            },
            saveThreadIfVisibleOwner,
          );
        } catch (err) {
          if (err instanceof HarnessConfigError) {
            thread = await saveThreadIfVisibleOwner();
          } else {
            if (!isMissingThreadDeleteFenceImplementation(err)) throw err;
            thread = await saveThreadIfVisibleOwner();
          }
        }
      }
      // §4.1/§10.2: thread CRUD is an internal/operator op — it emits no
      // public HarnessEventV1 event (there is no thread_* family in the union).
      const record = toThreadRecord(thread);
      return record;
    },

    list: async (opts: ThreadListOptions): Promise<ThreadListResult> => {
      const memory = await this._requireMemoryStorage('threads.list()');
      assertNoHarnessInternalThreadMetadata(opts.metadata, 'threads.list().metadata');
      const out = await memory.listThreads({
        perPage: opts.perPage ?? 100,
        page: opts.page ?? 0,
        orderBy: opts.orderBy,
        filter: {
          resourceId: opts.resourceId,
          metadata: opts.metadata as Record<string, unknown> | undefined,
        },
      });
      return {
        threads: out.threads.map(toThreadRecord),
        total: out.total,
        perPage: out.perPage,
        page: out.page,
        hasMore: out.hasMore,
      };
    },

    get: async (opts: ThreadGetOptions): Promise<ThreadRecord | null> => {
      const memory = await this._requireMemoryStorage('threads.get()');
      const thread = await memory.getThreadById({ threadId: opts.threadId });
      if (!thread || thread.resourceId !== opts.resourceId) return null;
      return toThreadRecord(thread);
    },

    rename: async (opts: ThreadRenameOptions): Promise<ThreadRecord> => {
      const memory = await this._requireMemoryStorage('threads.rename()');
      assertNoHarnessInternalThreadMetadata(opts.metadata, 'threads.rename().metadata');
      const existing = await memory.getThreadById({ threadId: opts.threadId });
      if (!existing || existing.resourceId !== opts.resourceId) {
        throw new HarnessThreadNotFoundError(opts.resourceId, opts.threadId);
      }
      const merged: Record<string, unknown> = {
        ...((existing.metadata as Record<string, unknown> | undefined) ?? {}),
        ...((opts.metadata as Record<string, unknown> | undefined) ?? {}),
      };
      const updated = await memory.updateThread({
        id: opts.threadId,
        title: opts.title,
        metadata: merged,
      });
      const record = toThreadRecord(updated);
      return record;
    },

    clone: async (opts: ThreadCloneOptions): Promise<ThreadRecord> => {
      const memory = await this._requireMemoryStorage('threads.clone()');
      assertNoHarnessInternalThreadMetadata(opts.metadata, 'threads.clone().metadata');
      const source = await memory.getThreadById({ threadId: opts.threadId });
      if (!source || source.resourceId !== opts.resourceId) {
        throw new HarnessThreadNotFoundError(opts.resourceId, opts.threadId);
      }
      const cloned = await memory.cloneThread({
        sourceThreadId: opts.threadId,
        newThreadId: opts.newThreadId,
        resourceId: opts.resourceId,
        title: opts.title,
        metadata: opts.metadata as Record<string, unknown> | undefined,
        options: opts.messageLimit !== undefined ? { messageLimit: opts.messageLimit } : undefined,
      });
      const record = toThreadRecord(cloned.thread);
      return record;
    },

    delete: async (opts: ThreadDeleteOptions): Promise<void> => {
      if (this._shutdown) return;
      const memory = await this._requireMemoryStorage('threads.delete()');
      if (this._shutdown) return;
      const existing = await memory.getThreadById({ threadId: opts.threadId });
      if (this._shutdown) return;
      if (!existing || existing.resourceId !== opts.resourceId) {
        // Idempotent: deleting a missing or foreign-owned thread is a no-op
        // from the caller's perspective. Cross-resource existence is never
        // leaked.
        return;
      }
      if (hasExternalSessionStorageOwner(existing.metadata)) {
        throw new HarnessConfigError(
          'sessions.storage',
          'threads.delete() cannot delete global memory thread rows after a separate Harness session storage has attached to this thread',
        );
      }

      // Cascade: force-delete every session rooted on this thread before
      // deleting the thread so descendants, leases, and owned attachments are
      // cleaned through the same session lifecycle path. Without Harness
      // session storage we cannot prove cross-process ownership, so deletion
      // fails closed before mutating global memory rows.
      let storage: HarnessStorage;
      try {
        storage = this._requireStorage('threads.delete()');
      } catch (err) {
        if (!(err instanceof HarnessConfigError)) throw err;
        throw new HarnessConfigError(
          'sessions.storage',
          'threads.delete() requires Harness session storage so it can prove thread ownership before deleting global memory thread rows',
        );
      }
      if (!this._canDeleteGlobalMemoryThreadWithStorage(storage, memory)) {
        throw new HarnessConfigError(
          'sessions.storage',
          'threads.delete() cannot cascade with a separate session storage override because MemoryStorage.deleteThread deletes global thread rows',
        );
      }
      let deletedRootThread = false;
      const rootDeleteMarked = await this._setThreadDeleteInProgress(memory, opts.threadId, true, opts.resourceId);
      try {
        while (!this._shutdown) {
          try {
            await storage.withThreadDeleteFence(
              {
                threadId: opts.threadId,
                ownerId: `${this.ownerId}:thread-delete:${randomUUID()}`,
                ttlMs: Math.max(this._closeTimeoutMs, this._leaseTtlMs),
              },
              async rootFence => {
                if (this._shutdown) return;
                // Preflight before deleting session rows: custom adapters must
                // prove they can see active thread owners across their visible
                // namespaces before we mutate storage or global memory rows.
                await storage.listActiveSessionsByThread({ threadId: opts.threadId });
                const candidates = await storage.listSessionsByThread({
                  harnessName: this._harnessName,
                  resourceId: opts.resourceId,
                  threadId: opts.threadId,
                  includeClosed: true,
                });
                if (this._shutdown) return;
                for (const candidate of candidates) {
                  const stored = await storage.loadSession({
                    harnessName: this._harnessName,
                    sessionId: candidate.id,
                  });
                  if (!stored || stored.threadId !== opts.threadId || stored.resourceId !== opts.resourceId) continue;
                  const deletedRecords = await this._forceDeleteSessionRecord(storage, stored, () => !this._shutdown, {
                    resourceId: opts.resourceId,
                  });
                  for (const deleted of deletedRecords) {
                    if (this._shutdown) return;
                    if (!deleted.ownsThread || deleted.threadId === opts.threadId) continue;
                    try {
                      await storage.withThreadDeleteFence(
                        {
                          threadId: deleted.threadId,
                          ownerId: `${this.ownerId}:thread-delete:${randomUUID()}`,
                          ttlMs: Math.max(this._closeTimeoutMs, this._leaseTtlMs),
                        },
                        async descendantFence => {
                          const descendantDeleteMarked = await this._setThreadDeleteInProgress(
                            memory,
                            deleted.threadId,
                            true,
                            deleted.resourceId,
                          );
                          let deletedDescendantThread = false;
                          try {
                            const deletedThread = await memory.getThreadById({ threadId: deleted.threadId });
                            if (!deletedThread || deletedThread.resourceId !== deleted.resourceId) return;
                            if (hasExternalSessionStorageOwner(deletedThread.metadata)) return;
                            const activeThreadSessions = await storage.listActiveSessionsByThread({
                              threadId: deleted.threadId,
                            });
                            if (activeThreadSessions.length > 0) return;
                            const remainingThreadSessions = await storage.listSessionsByThread({
                              threadId: deleted.threadId,
                              includeClosed: true,
                            });
                            if (remainingThreadSessions.length > 0) return;
                            if (!this._canDeleteGlobalMemoryThreadWithStorage(storage, memory)) return;
                            if (await this._hasVisibleHarnessSessionsForThread(storage, deleted.threadId)) return;
                            await descendantFence.assertActive();
                            await memory.deleteThread({ threadId: deleted.threadId });
                            deletedDescendantThread = true;
                            if (this._shutdown) return;
                            if (memory.supportsObservationalMemory) {
                              await memory.clearObservationalMemory(deleted.threadId, deleted.resourceId);
                            }
                          } finally {
                            if (descendantDeleteMarked && !deletedDescendantThread) {
                              await this._setThreadDeleteInProgress(
                                memory,
                                deleted.threadId,
                                false,
                                deleted.resourceId,
                              );
                            }
                          }
                        },
                      );
                    } catch (err) {
                      if (err instanceof HarnessStorageThreadDeleteFenceConflictError) continue;
                      throw err;
                    }
                  }
                  if (this._shutdown) return;
                }

                const activeRootThreadSessions = await storage.listActiveSessionsByThread({
                  threadId: opts.threadId,
                });
                if (activeRootThreadSessions.length > 0) {
                  return;
                }
                const remainingRootThreadSessions = await storage.listSessionsByThread({
                  threadId: opts.threadId,
                  includeClosed: true,
                });
                if (remainingRootThreadSessions.length > 0) {
                  return;
                }
                const rootThread = await memory.getThreadById({ threadId: opts.threadId });
                if (!rootThread || rootThread.resourceId !== opts.resourceId) {
                  return;
                }
                if (hasExternalSessionStorageOwner(rootThread.metadata)) {
                  return;
                }
                if (!this._canDeleteGlobalMemoryThreadWithStorage(storage, memory)) {
                  return;
                }
                if (await this._hasVisibleHarnessSessionsForThread(storage, opts.threadId)) {
                  return;
                }
                await rootFence.assertActive();
                await memory.deleteThread({ threadId: opts.threadId });
                deletedRootThread = true;
                if (memory.supportsObservationalMemory) {
                  await memory.clearObservationalMemory(opts.threadId, opts.resourceId);
                }
              },
            );
            break;
          } catch (err) {
            if (
              err instanceof HarnessStorageThreadDeleteFenceConflictError &&
              err.ownerId?.includes(':thread-delete:')
            ) {
              await waitForThreadDeleteFenceRetry();
              continue;
            }
            throw err;
          }
        }
      } finally {
        if (rootDeleteMarked && !deletedRootThread) {
          await this._setThreadDeleteInProgress(memory, opts.threadId, false, opts.resourceId);
        }
      }
      if (!deletedRootThread) return;
    },

    /**
     * Shallow-merges `patch` into the thread's `metadata`. Keys whose values
     * are `undefined` in the patch are removed from the stored metadata. The
     * patch is otherwise a verbatim overwrite — nested objects are replaced,
     * not deep-merged, matching `Session.setState()` semantics.
     *
     * Emits `thread_settings_changed` only when the on-disk metadata actually
     * differs from the prior state, so subscribers can treat the event as a
     * real change signal rather than a write-acknowledgement.
     *
     * Throws `HarnessThreadNotFoundError` if the thread does not exist or
     * is owned by a different resource — cross-resource existence is never
     * leaked.
     */
    setSettings: async (opts: ThreadSetSettingsOptions): Promise<void> => {
      const memory = await this._requireMemoryStorage('threads.setSettings()');
      const existing = await memory.getThreadById({ threadId: opts.threadId });
      if (!existing || existing.resourceId !== opts.resourceId) {
        throw new HarnessThreadNotFoundError(opts.resourceId, opts.threadId);
      }

      const before = (existing.metadata as Record<string, unknown> | undefined) ?? {};
      const next: Record<string, unknown> = { ...before };
      const effectivePatch: Record<string, unknown> = {};
      const removedKeys: string[] = [];

      for (const [key, value] of Object.entries(opts.patch)) {
        if (HARNESS_INTERNAL_THREAD_METADATA_KEYS.has(key)) {
          throw new HarnessConfigError(
            'threads.setSettings().patch',
            `metadata key "${key}" is reserved for Harness internals`,
          );
        }
        if (value === undefined) {
          if (key in next) {
            delete next[key];
            removedKeys.push(key);
          }
          continue;
        }
        // Only record real diffs so the event reflects actual change.
        if (!Object.is(before[key], value)) {
          next[key] = value;
          effectivePatch[key] = value;
        }
      }

      if (Object.keys(effectivePatch).length === 0 && removedKeys.length === 0) {
        // No-op write — skip the storage round trip and the event.
        return;
      }

      await memory.saveThread({
        thread: {
          ...existing,
          metadata: Object.keys(next).length > 0 ? next : undefined,
          updatedAt: new Date(),
        },
      });

    },

    /**
     * Returns a frozen snapshot of the thread's metadata. An empty object is
     * returned when the thread has no metadata. Throws
     * `HarnessThreadNotFoundError` if the thread does not exist or is owned
     * by a different resource.
     */
    getSettings: async (opts: ThreadGetSettingsOptions): Promise<Readonly<Record<string, unknown>>> => {
      const memory = await this._requireMemoryStorage('threads.getSettings()');
      const existing = await memory.getThreadById({ threadId: opts.threadId });
      if (!existing || existing.resourceId !== opts.resourceId) {
        throw new HarnessThreadNotFoundError(opts.resourceId, opts.threadId);
      }
      const metadata = (existing.metadata as Record<string, unknown> | undefined) ?? {};
      return Object.freeze(stripHarnessInternalThreadMetadata(metadata) ?? {});
    },

    /**
     * Convenience accessor for a single setting. Returns `undefined` when the
     * key is absent. Throws `HarnessThreadNotFoundError` if the thread does
     * not exist or is owned by a different resource.
     */
    getSetting: async (opts: ThreadGetSettingOptions): Promise<unknown> => {
      const settings = await this._threadOps.getSettings({
        resourceId: opts.resourceId,
        threadId: opts.threadId,
      });
      return settings[opts.key];
    },
  };

  // -------------------------------------------------------------------------
  // §9 — `harness.models.*` (catalog + auth status). The catalog is static,
  // declared at construction. Auth status is resolved on demand because it
  // changes out-of-band (login flows, expiring tokens) and the harness has
  // no signal to invalidate a cache on.
  // -------------------------------------------------------------------------

  models = {
    /**
     * Returns a frozen snapshot of every catalog entry in declaration order.
     * The catalog is intentionally a pure UX surface — callers can render a
     * picker without reaching into provider plumbing. Empty array when the
     * harness was configured without a `models` list.
     */
    list: async (): Promise<readonly ModelInfo[]> => {
      return Object.freeze(Array.from(this._modelCatalog.values()));
    },

    /**
     * Returns the catalog entry for `modelId`, or `null` if no such entry
     * exists. Async to match the rest of `harness.models.*` and leave room
     * for backend-backed catalogs without a breaking change.
     */
    get: async (modelId: string): Promise<ModelInfo | null> => {
      return this._modelCatalog.get(modelId) ?? null;
    },

    /**
     * Resolves the current auth status for a catalog `modelId`. Calls the
     * configured {@link HarnessConfigCommon.modelAuthStatusResolver}; if
     * none was supplied, returns `'unknown'`.
     *
     * Throws `HarnessModelNotFoundError` when `modelId` is not in the
     * catalog. Typos surface immediately rather than collapsing into a
     * spurious `'unknown'` reading.
     */
    getAuthStatus: async (modelId: string): Promise<ModelAuthStatus> => {
      if (!this._modelCatalog.has(modelId)) {
        throw new HarnessModelNotFoundError(modelId);
      }
      if (!this._modelAuthStatusResolver) return 'unknown';
      return await this._modelAuthStatusResolver(modelId);
    },
  };

  getFileConfig(): Readonly<HarnessFileConfig> {
    return Object.freeze({
      ...this._fileConfig,
      ...(this._fileConfig.allowedUrlMimeTypes
        ? { allowedUrlMimeTypes: Object.freeze([...this._fileConfig.allowedUrlMimeTypes]) }
        : {}),
    });
  }

  attachments = {
    upload: async (opts: AttachmentUploadOptions): Promise<AttachmentRef> => {
      const storage = this._requireStorage('attachments.upload()');
      const session = await this.session(
        opts.resourceId ? { sessionId: opts.sessionId, resourceId: opts.resourceId } : { sessionId: opts.sessionId },
      );
      const metadata =
        opts.metadata === undefined
          ? undefined
          : assertAttachmentJsonRecord(opts.metadata, 'attachments.upload().metadata');
      let upload: {
        name: string;
        mimeType: string;
        data: Uint8Array;
        semantic: AttachmentSemanticMetadata;
      };
      if (opts.kind === 'primitive') {
        upload = {
          name: opts.name,
          mimeType: opts.mimeType ?? 'application/json',
          data: encodeAttachmentJson(assertAttachmentJsonValue(opts.value, 'attachments.upload().value')),
          semantic: {
            kind: 'primitive',
            primitiveType: opts.primitiveType,
            ...(metadata ? { metadata } : {}),
          },
        };
      } else if (opts.kind === 'element') {
        upload = {
          name: opts.name,
          mimeType: opts.mimeType ?? 'application/vnd.mastra.harness.element+json',
          data: encodeAttachmentJson(assertAttachmentJsonValue(opts.payload, 'attachments.upload().payload')),
          semantic: {
            kind: 'element',
            elementType: opts.elementType,
            ...(opts.renderer ? { renderer: { ...opts.renderer } } : {}),
            ...(opts.schemaId ? { schemaId: opts.schemaId } : {}),
            ...(metadata ? { metadata } : {}),
          },
        };
      } else {
        upload = {
          name: opts.filename,
          mimeType: opts.contentType,
          data:
            opts.data instanceof Uint8Array
              ? new Uint8Array(opts.data)
              : new Uint8Array(await new Response(opts.data).arrayBuffer()),
          semantic: {
            kind: 'file',
            ...(metadata ? { metadata } : {}),
          },
        };
      }
      const internalOpts = opts as { attachmentId?: unknown; source?: AttachmentSource };
      const source = internalOpts.source === 'url' ? 'url' : 'preupload';
      if (
        internalOpts.attachmentId !== undefined &&
        (typeof internalOpts.attachmentId !== 'string' || internalOpts.attachmentId.length === 0)
      ) {
        throw new HarnessValidationError('attachments.upload().attachmentId', 'must be a non-empty string');
      }
      const attachmentId = internalOpts.attachmentId ?? `attachment-${randomUUID()}`;
      const sha256 = createHash('sha256').update(upload.data).digest('hex');
      const existing = await storage.getAttachmentRecord({
        harnessName: session.getRecord().harnessName,
        sessionId: session.id,
        attachmentId,
      });
      if (existing) {
        const existingSemantic = attachmentSemanticFromRecord(existing);
        if (
          existing.name !== upload.name ||
          existing.mimeType !== upload.mimeType ||
          existing.bytes !== upload.data.byteLength ||
          existing.sha256 !== sha256 ||
          existing.source !== source ||
          !attachmentSemanticMatches(existingSemantic, upload.semantic)
        ) {
          throw new HarnessAttachmentUnavailableError(session.id, 'digest_mismatch', attachmentId);
        }
        return {
          attachmentId: existing.attachmentId,
          resourceId: session.resourceId,
          ownerSessionId: session.id,
          bytes: existing.bytes,
          sha256: existing.sha256,
          source: existing.source,
          name: existing.name,
          mimeType: existing.mimeType,
          ...existingSemantic,
        };
      }
      const saved = await storage.saveAttachment({
        harnessName: session.getRecord().harnessName,
        sessionId: session.id,
        attachmentId,
        name: upload.name,
        mimeType: upload.mimeType,
        source,
        data: upload.data,
        semantic: upload.semantic,
      });
      const savedRecord = await storage.getAttachmentRecord({
        harnessName: session.getRecord().harnessName,
        sessionId: session.id,
        attachmentId,
      });
      const savedSemantic = savedRecord ? attachmentSemanticFromRecord(savedRecord) : undefined;
      if (
        !savedRecord ||
        savedRecord.name !== upload.name ||
        savedRecord.mimeType !== upload.mimeType ||
        saved.bytes !== upload.data.byteLength ||
        saved.sha256 !== sha256 ||
        savedRecord.source !== source ||
        savedRecord.bytes !== upload.data.byteLength ||
        savedRecord.sha256 !== sha256 ||
        !savedSemantic ||
        !attachmentSemanticMatches(savedSemantic, upload.semantic)
      ) {
        throw new HarnessAttachmentUnavailableError(session.id, 'digest_mismatch', attachmentId);
      }
      // §10.2: session-scoped attachment_uploaded fires only on a fresh save
      // (the idempotent existing-digest path above does not re-emit).
      session._emit({
        type: 'attachment_uploaded',
        attachmentId: savedRecord.attachmentId,
        name: savedRecord.name,
        mimeType: savedRecord.mimeType,
        bytes: savedRecord.bytes,
      });
      return {
        attachmentId: savedRecord.attachmentId,
        resourceId: session.resourceId,
        ownerSessionId: session.id,
        bytes: savedRecord.bytes,
        sha256: savedRecord.sha256,
        source,
        name: upload.name,
        mimeType: upload.mimeType,
        ...savedSemantic,
      };
    },
    delete: async (opts: AttachmentDeleteOptions): Promise<void> => {
      const storage = this._requireStorage('attachments.delete()');
      const session = await this.session(
        opts.resourceId ? { sessionId: opts.sessionId, resourceId: opts.resourceId } : { sessionId: opts.sessionId },
      );
      try {
        await storage.deleteAttachment({
          harnessName: session.getRecord().harnessName,
          sessionId: session.id,
          attachmentId: opts.attachmentId,
        });
      } catch (err) {
        if (err instanceof HarnessStorageAttachmentInUseError) {
          throw new HarnessAttachmentInUseError(err.sessionId, err.attachmentId, err.references);
        }
        throw err;
      }
      // §10.2: session-scoped attachment_deleted after the durable delete commits.
      session._emit({ type: 'attachment_deleted', attachmentId: opts.attachmentId });
    },
  };

  // -------------------------------------------------------------------------
  // Internals.
  // -------------------------------------------------------------------------

  private _requireStorage(callsite: string): HarnessStorage {
    if (this._storageOverride) return this._storageOverride;
    if (this._mastra) {
      const composite = this._mastra.getStorage();
      // Domain access goes through getStore() everywhere else in the codebase
      // — keep this consistent so adapters that override the accessor (e.g.
      // to add caching or lazy init) plug in transparently. Synchronously
      // available because all current adapters resolve domains eagerly, but
      // we still resolve via the accessor rather than poking `.stores.harness`
      // directly.
      const harness = composite?.stores?.harness;
      if (harness) return harness;
    }
    throw new HarnessConfigError(
      'sessions.storage',
      `required for ${callsite} — pass storage in HarnessConfig.storage, HarnessConfig.sessions.storage, or via the Mastra instance backing this harness`,
    );
  }

  private _canDeleteGlobalMemoryThreadWithStorage(storage: HarnessStorage, memory: object): boolean {
    const mastra = this._mastra;
    if (!mastra || mastra.getStorage()?.stores?.harness !== storage) return false;
    const boundHarnesses = boundHarnessesByMemory.get(memory);
    if (!boundHarnesses) return false;
    for (const harness of boundHarnesses) {
      if (harness._getEffectiveSessionStorage() !== storage) return false;
    }
    return true;
  }

  private _trackMemoryStorage(memory: unknown): void {
    if (!memory || typeof memory !== 'object') return;
    let boundHarnesses = boundHarnessesByMemory.get(memory);
    if (!boundHarnesses) {
      boundHarnesses = new Set();
      boundHarnessesByMemory.set(memory, boundHarnesses);
    }
    boundHarnesses.add(this);
  }

  private _untrackBoundStorage(): void {
    const mastra = this._mastra;
    if (mastra) {
      boundHarnessesByMastra.get(mastra)?.delete(this);
      this._untrackMemoryStorage(mastra.getStorage()?.stores?.memory);
    }
  }

  private _untrackMemoryStorage(memory: unknown): void {
    if (!memory || typeof memory !== 'object') return;
    boundHarnessesByMemory.get(memory)?.delete(this);
  }

  private _getEffectiveSessionStorage(): HarnessStorage | undefined {
    return this._storageOverride ?? this._mastra?.getStorage()?.stores?.harness;
  }

  /** @internal — used by Harness wakeup workers to honor session storage overrides. */
  _internalGetSessionStorage(): HarnessStorage | undefined {
    return this._getEffectiveSessionStorage();
  }

  private _usesSeparateSessionStorage(): boolean {
    if (!this._storageOverride) return false;
    return this._storageOverride !== this._mastra?.getStorage()?.stores?.harness;
  }

  private async _markExternalSessionStorageOwner(
    threadId: string,
    opts: { requireExisting?: boolean } = {},
  ): Promise<void> {
    if (!this._usesSeparateSessionStorage()) return;
    const memory = await this._internalTryGetMemoryStorage();
    if (!memory) return;
    const thread = await memory.getThreadById({ threadId });
    if (!thread) {
      if (opts.requireExisting === false) return;
      throw new HarnessConfigError(
        'sessions.storage',
        'session() cannot attach a separate session storage to a memory thread that does not exist',
      );
    }
    if (hasHarnessThreadDeleteInProgress(thread.metadata)) {
      throw new HarnessConfigError(
        'sessions.storage',
        'session() cannot attach a separate session storage to a memory thread while threads.delete() is in progress',
      );
    }
    const hadExternalOwner = hasExternalSessionStorageOwner(thread.metadata);
    if (hadExternalOwner) return;
    await memory.updateThread({
      id: threadId,
      title: thread.title ?? '',
      metadata: {
        ...(thread.metadata as Record<string, unknown> | undefined),
        [EXTERNAL_SESSION_STORAGE_OWNER_METADATA_KEY]: true,
      },
    });
    const marked = await memory.getThreadById({ threadId });
    if (hasHarnessThreadDeleteInProgress(marked?.metadata)) {
      await memory.updateThread({
        id: threadId,
        title: marked?.title ?? thread.title ?? '',
        metadata: {
          ...((marked?.metadata as Record<string, unknown> | undefined) ?? {}),
          [EXTERNAL_SESSION_STORAGE_OWNER_METADATA_KEY]: false,
        },
      });
      throw new HarnessConfigError(
        'sessions.storage',
        'session() cannot attach a separate session storage to a memory thread while threads.delete() is in progress',
      );
    }
  }

  private async _setThreadDeleteInProgress(
    memory: MemoryStorage,
    threadId: string,
    value: boolean,
    resourceId?: string,
  ): Promise<boolean> {
    const thread = await memory.getThreadById({ threadId });
    if (!thread || (resourceId !== undefined && thread.resourceId !== resourceId)) return false;
    await memory.updateThread({
      id: threadId,
      title: thread.title ?? '',
      metadata: {
        ...(thread.metadata as Record<string, unknown> | undefined),
        [HARNESS_THREAD_DELETE_IN_PROGRESS_METADATA_KEY]: value,
      },
    });
    return true;
  }

  private async _hasVisibleHarnessSessionsForThread(storage: HarnessStorage, threadId: string): Promise<boolean> {
    const sessions = await storage.listSessionsByThread({
      threadId,
      includeClosed: true,
    });
    return sessions.length > 0;
  }

  /**
   * Thread CRUD is owned by Mastra's memory storage domain, not by the
   * harness storage domain. We resolve it lazily through the bound Mastra
   * instance via `getStore('memory')` — the harness never persists threads
   * itself.
   */
  private async _requireMemoryStorage(callsite: string) {
    if (!this._mastra) {
      throw new HarnessConfigError(
        'mastra',
        `required for ${callsite} — thread CRUD needs a Mastra instance bound to this harness so we can access the memory storage domain`,
      );
    }
    const composite = this._mastra.getStorage();
    if (!composite) {
      throw new HarnessConfigError(
        'storage',
        `required for ${callsite} — the bound Mastra instance has no storage configured`,
      );
    }
    const memory = await composite.getStore('memory');
    if (!memory) {
      throw new HarnessConfigError(
        'storage.memory',
        `required for ${callsite} — the bound Mastra storage has no memory domain registered`,
      );
    }
    this._trackMemoryStorage(memory);
    return memory;
  }

  /**
   * @internal — Session-facing soft variant of `_requireMemoryStorage`. Returns
   * `null` when memory storage is not configured instead of throwing, so
   * read-only consumers (e.g. `Session.listMessages`) can gracefully return an
   * empty history for ad-hoc threads without crashing.
   */
  async _internalTryGetMemoryStorage() {
    if (!this._mastra) return null;
    const composite = this._mastra.getStorage();
    if (!composite) return null;
    const memory = await composite.getStore('memory');
    this._trackMemoryStorage(memory);
    return memory ?? null;
  }

  private _mintThreadId(): string {
    return `thread-${randomUUID()}`;
  }

  /** @internal — used by Session.cancel(...) to walk the live subagent tree. */
  _internalGetLiveSession(sessionId: string): Session | undefined {
    return this._liveSessions.get(sessionId);
  }

  async _internalEvictLiveSessionLeaseLost(session: Session): Promise<void> {
    await this._evictLiveSession(session, 'lease_lost');
  }

  /**
   * @internal §5.8 — fence the entire live subtree that owns `session` as
   * lease_lost. A save lease conflict means subtree ownership was lost (root row
   * conflict) or split (child row conflict), so the root and every live
   * descendant must be evicted, not just `session`. Resolves the live root to
   * cover siblings; falls back to `session` + its live descendants when the root
   * cannot be resolved (orphan).
   */
  async _internalEvictSubtreeLeaseLost(session: Session): Promise<void> {
    const root = this._resolveLiveRoot(session) ?? session;
    const subtree = [root, ...this._liveDescendantsOf(root.id)];
    for (const member of subtree) {
      if (this._liveSessions.get(member.id) === member) await this._evictLiveSession(member, 'lease_lost');
    }
  }

  /** @internal — exposed for inspection in tests. */
  _internalLiveSessionCount(): number {
    return this._liveSessions.size;
  }

  /** @internal — accessor for `Session.queue()` admission caps. */
  get _internalMaxQueueDepth(): number {
    return this._maxQueueDepth;
  }

  /** @internal — accessor for `Session.queue()` full-queue behavior. */
  get _internalQueueBackpressure(): HarnessQueueBackpressurePolicy {
    return this._queueBackpressure;
  }

  /** @internal — default lease TTL consumed by `Session.extendLease(...)`. */
  get _internalLeaseTtlMs(): number {
    return this._leaseTtlMs;
  }

  /** @internal — goal-loop defaults, consumed by `Session.setGoal()` (§4.7). */
  get _internalGoalDefaults(): Readonly<{ defaultJudgeModel?: string; defaultMaxTurns: number }> {
    return this._goalDefaults;
  }
}

function toThreadRecord(thread: {
  id: string;
  resourceId: string;
  title?: string;
  createdAt: Date;
  updatedAt: Date;
  metadata?: Record<string, unknown>;
}): ThreadRecord {
  return {
    id: thread.id,
    resourceId: thread.resourceId,
    title: thread.title,
    createdAt: thread.createdAt,
    updatedAt: thread.updatedAt,
    metadata: stripHarnessInternalThreadMetadata(thread.metadata),
  };
}

function isMissingThreadDeleteFenceImplementation(err: unknown): boolean {
  return (
    err instanceof HarnessStorageThreadDeleteFenceUnsupportedError ||
    (err instanceof Error &&
      err.message === 'HarnessStorage.withThreadDeleteFence must be implemented by this storage adapter')
  );
}

async function waitForThreadDeleteFenceRetry(): Promise<void> {
  await new Promise(resolve => setTimeout(resolve, 25));
}

function emptyPermissionRules(): PermissionRules {
  return { categories: {}, tools: {} };
}

function emptySessionGrants(): SessionGrants {
  return { categories: [], tools: [] };
}

function zeroTokenUsage(): TokenUsage {
  return { promptTokens: 0, completionTokens: 0, totalTokens: 0 };
}

// §14.1 channel-binding id derivation. Missing optional external IDs normalise to
// a sentinel (matching the storage tuple key) so derived ids are stable.
function normChannelExternalId(value: string | undefined): string {
  return value ?? ' ';
}

/** §14.1: a stable, namespaced threadId from the resolved resourceId + canonical platform tuple. */
function deriveChannelThreadId(
  ctx: {
    harnessName: string;
    channelId: string;
    platform: string;
    externalTenantId?: string;
    externalChannelId?: string;
    externalThreadId: string;
  },
  resourceId: string,
): string {
  const canonical = JSON.stringify([
    ctx.harnessName,
    ctx.channelId,
    ctx.platform,
    normChannelExternalId(ctx.externalTenantId),
    normChannelExternalId(ctx.externalChannelId),
    ctx.externalThreadId,
    resourceId,
  ]);
  return `ch:${createHash('sha256').update(canonical, 'utf8').digest('hex').slice(0, 32)}`;
}

/** §14.1: derive the owning session id from the resolved (resourceId, threadId). */
function deriveChannelSessionId(resourceId: string, threadId: string): string {
  const canonical = JSON.stringify([resourceId, threadId]);
  return `chs:${createHash('sha256').update(canonical, 'utf8').digest('hex').slice(0, 32)}`;
}

/**
 * §14.3: the trusted `requestContext.channel` projection, built ONLY from
 * verified envelope + (once resolved) binding evidence — never from caller input.
 * `bindingId` is absent at inbox-create (binding not yet resolved) and present
 * once the binding is committed.
 */
function buildChannelRequestContext(
  ctx: ChannelIngressContext,
  bindingId?: string,
): PersistedRequestContextInput {
  return {
    channel: {
      origin: 'inbound',
      harnessName: ctx.harnessName,
      channelId: ctx.channelId,
      providerId: ctx.providerId,
      platform: ctx.platform,
      conversationKind: ctx.conversationKind,
      trigger: ctx.trigger,
      externalThreadId: ctx.externalThreadId,
      externalMessageId: ctx.externalMessageId,
      ...(bindingId !== undefined ? { bindingId } : {}),
      ...(ctx.externalTenantId !== undefined ? { externalTenantId: ctx.externalTenantId } : {}),
      ...(ctx.externalChannelId !== undefined ? { externalChannelId: ctx.externalChannelId } : {}),
      // Map the provider-envelope actor (§14 ChannelActorContext: platformUserId) onto the §14.3
      // request-context actor (externalUserId). `linkedResourceId` is set only after app-level
      // identity linking, never at ingress; the envelope's `metadata` is not part of §14.3.
      ...(ctx.actor !== undefined
        ? {
            actor: {
              externalUserId: ctx.actor.platformUserId,
              ...(ctx.actor.displayName !== undefined ? { displayName: ctx.actor.displayName } : {}),
            },
          }
        : {}),
    },
  };
}

/**
 * §14.2 recovery: rebuild the `ChannelIngressContext` a recovery worker needs to
 * re-resolve a binding for a row that never resolved, from the durable inbox row
 * + its trusted `requestContext.channel`. `files` and `raw` are intentionally NOT
 * reconstructed — `ingress.resolveResource` must be deterministic from durable
 * fields (a policy that depends on raw provider bytes is invalid by design).
 * Throws if the row predates envelope persistence (missing channel context or
 * conversationKind/trigger) rather than guessing the policy input; the worker
 * surfaces that as a retryable failure (then dead-letters at maxAttempts, until
 * the deferred typed-error taxonomy classifies it as immediately terminal).
 */
function reconstructChannelIngressContext(row: ChannelInboxItem): ChannelIngressContext {
  const ch = row.requestContext.channel;
  if (ch === undefined || ch.conversationKind === undefined || ch.trigger === undefined) {
    throw new HarnessValidationError(
      'recoverChannelInboxOnce',
      'inbox row is missing the persisted channel envelope (conversationKind/trigger) required to re-resolve its binding',
    );
  }
  return {
    harnessName: ch.harnessName,
    channelId: ch.channelId,
    providerId: ch.providerId,
    platform: ch.platform,
    conversationKind: ch.conversationKind,
    trigger: ch.trigger,
    externalThreadId: ch.externalThreadId,
    externalMessageId: row.externalMessageId,
    content: row.content,
    receivedAt: row.receivedAt,
    ...(ch.externalTenantId !== undefined ? { externalTenantId: ch.externalTenantId } : {}),
    ...(ch.externalChannelId !== undefined ? { externalChannelId: ch.externalChannelId } : {}),
    // Reverse map: §14.3 request-context actor (externalUserId) → provider-envelope actor
    // (ChannelActorContext: platformUserId) for re-resolving the binding.
    ...(ch.actor !== undefined
      ? {
          actor: {
            platformUserId: ch.actor.externalUserId,
            ...(ch.actor.displayName !== undefined ? { displayName: ch.actor.displayName } : {}),
          },
        }
      : {}),
  };
}

/** §9 default inbox retry backoff: exponential (1s→64s) with bounded jitter. */
function defaultInboxRetryBackoffMs(attempt: number): number {
  const base = 1_000;
  const backoff = base * 2 ** Math.min(Math.max(attempt, 1), 6);
  const jitter = Math.floor(Math.random() * base);
  return Math.min(120_000, backoff + jitter);
}

/**
 * §14.2 channel-ingress recovery error taxonomy. Maps a thrown admission error to
 * the durable failure shape the worker persists. `attempts` is the already-bumped
 * attempt count for this row. Returns the target row status, the BARE
 * `HarnessRowErrorCode` for `lastError.code`, the retryable flag, and (for a
 * retryable `failed`) the next-attempt time.
 *
 * Terminal (operator-repair `dead`, ignore maxAttempts):
 *   - HarnessSessionClosedError → 'session_closed' (the row's resolved binding/
 *     session closed; §14.2 exempts only rows that durably resolved a replacement
 *     BEFORE admission — the worker has no such row, so it is terminal here).
 *   - HarnessSessionDeletedError → 'session_deleted' (never retarget).
 *   - HarnessOverrideConflictError → 'override_conflict' (the spec's "switch to
 *     queue" branch is for SIGNAL admission; this worker is queue-delivery only,
 *     so there is nothing to switch to → operator repair).
 * Deadline-bounded:
 *   - HarnessSessionClosingError → retryable 'session_closing' with nextAttemptAt
 *     clamped to closeDeadlineAt; once now ≥ closeDeadlineAt (or attempts exhausted)
 *     it is terminal 'dead'.
 * Retryable operational backpressure (→ 'dead' only at maxAttempts):
 *   - HarnessSessionLockedError → 'session_locked'
 *   - HarnessLiveSessionLimitError → 'live_session_limit'
 *   - HarnessQueueFullError → 'queue_full' (retryable because this worker forces
 *     queue delivery; revisit if signal delivery lands).
 *   - anything else → 'unknown'.
 */
function classifyChannelInboxFailure(
  error: unknown,
  opts: { attempts: number; maxAttempts: number; now: number; retryBackoffMs: (attempt: number) => number },
): { status: 'failed' | 'dead'; code: HarnessRowErrorCode; retryable: boolean; nextAttemptAt?: number } {
  const { attempts, maxAttempts, now, retryBackoffMs } = opts;
  const backoffRetry = (code: HarnessRowErrorCode): {
    status: 'failed' | 'dead';
    code: HarnessRowErrorCode;
    retryable: boolean;
    nextAttemptAt?: number;
  } =>
    attempts >= maxAttempts
      ? { status: 'dead', code, retryable: false }
      : { status: 'failed', code, retryable: true, nextAttemptAt: now + retryBackoffMs(attempts) };

  // Immediately terminal — operator repair, regardless of attempts.
  if (error instanceof HarnessSessionClosedError) return { status: 'dead', code: 'session_closed', retryable: false };
  if (error instanceof HarnessSessionDeletedError) return { status: 'dead', code: 'session_deleted', retryable: false };
  if (error instanceof HarnessOverrideConflictError) {
    return { status: 'dead', code: 'override_conflict', retryable: false };
  }
  // Unrecoverable row-shape problems (a row missing its persisted envelope or
  // resolved ids, or an ingress policy that returns an invalid resolution) cannot
  // be fixed by retrying — dead-letter immediately rather than burning attempts.
  if (error instanceof HarnessValidationError) return { status: 'dead', code: 'unknown', retryable: false };

  // Retryable only until the closing deadline.
  if (error instanceof HarnessSessionClosingError) {
    const deadline = error.closeDeadlineAt;
    if (now >= deadline || attempts >= maxAttempts) {
      return { status: 'dead', code: 'session_closing', retryable: false };
    }
    return {
      status: 'failed',
      code: 'session_closing',
      retryable: true,
      nextAttemptAt: Math.min(now + retryBackoffMs(attempts), deadline),
    };
  }

  if (error instanceof HarnessSessionLockedError) return backoffRetry('session_locked');
  if (error instanceof HarnessLiveSessionLimitError) return backoffRetry('live_session_limit');
  if (error instanceof HarnessQueueFullError) return backoffRetry('queue_full');
  return backoffRetry('unknown');
}

/**
 * @internal Operator/server boundary for thread CRUD (§13 auto-mounted routes).
 *
 * Thread create/list/get/rename/clone/delete/settings are intentionally NOT a
 * public app-facing `harness.threads.*` namespace (§0 mental-model, §4.1, §11.6e:
 * "there is no `harness.threads.*` lifecycle surface"; §13: "no matching
 * in-process thread method on Harness"). Server routes and operator tooling
 * reach these operations through this factory; product app code uses `Session`
 * lifecycle (`close()` / `delete()` / `rename()`) instead. The underlying
 * `_threadOps` member remains internal-only and is not part of the public type.
 */
export type HarnessOperatorThreadController = Harness['_threadOps'];

/** @internal See {@link HarnessOperatorThreadController}. */
export function createHarnessOperatorThreadController(harness: Harness): HarnessOperatorThreadController {
  return harness._threadOps;
}
