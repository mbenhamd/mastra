/**
 * Session mode, model and session-configuration operations (HARNESS_V1_SPEC.md §4.2 / §4.2a / §4.2d / §4.2e).
 *
 * The session is the local authority for the active mode/model id; the backing
 * agent is selected via Harness lookup. Setters CAS-write through storage so a
 * concurrent harness instance that holds the lease cannot observe a stale value.
 * This module also holds the flat attachment and thread-setting conveniences.
 *
 * It reaches the session only through `ModeModelConfigSessionPort`, which
 * `Session` builds in `_modeModelConfigPort()`.
 */

import type { SessionRecord } from '../../storage/domains/harness';
import type { JsonValue } from '../../storage/domains/harness/types';
import { assertJsonValue, sha256CanonicalJsonChecked as sha256CanonicalJson } from './canonical-json';
import { HarnessValidationError } from './errors';
import type { EventEmitter } from './events';
import type { Harness } from './harness';
import type { HarnessMode, ModelAuthStatus } from './types';

/**
 * The slice of `Session` the mode/model/config operations need. `record` is a
 * getter so the operations always see the current record; `modeSwitchEpoch`
 * reads and writes `Session._modeSwitchEpoch`, which the settlement-notice wake
 * grant reads.
 */
export interface ModeModelConfigSessionPort {
  readonly id: string;
  readonly resourceId: string;
  readonly threadId: string;
  readonly record: SessionRecord;
  readonly harness: Harness;
  readonly emitter: EventEmitter;
  modeSwitchEpoch: number;
  assertLive(method: string): void;
  flushUpdate(update: (prev: SessionRecord) => SessionRecord): Promise<void>;
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

export function currentModeId(port: ModeModelConfigSessionPort): string {
  port.assertLive('getCurrentModeId()');
  return port.record.modeId;
}

export function currentMode(port: ModeModelConfigSessionPort): HarnessMode {
  port.assertLive('getCurrentMode()');
  return port.harness._getMode(port.record.modeId);
}

export async function switchSessionMode(port: ModeModelConfigSessionPort, opts: { mode: string }): Promise<void> {
  port.assertLive('switchMode()');
  // Validates and throws on unknown id.
  port.harness._getMode(opts.mode);
  const previousModeId = port.record.modeId;
  if (previousModeId === opts.mode) return;
  port.modeSwitchEpoch += 1;
  // §4.2e — entering a mode re-establishes its base permission policy when it
  // declares one (the mode owns the base; runtime grants/setPolicy overlay it).
  // A mode that declares no `permissions` leaves the existing rules untouched.
  const seededRules = port.harness._modePermissionRules(opts.mode);
  await port.flushUpdate(prev => ({
    ...prev,
    modeId: opts.mode,
    ...(seededRules !== undefined
      ? { permissionRules: seededRules, permissionRulesSeedHash: sha256CanonicalJson(seededRules) }
      : {}),
  }));
  port.emitter.emit({ type: 'mode_changed', modeId: opts.mode, previousModeId });
}

/** Resolved model id for the next turn. Falls back to `''` when nothing has been selected. */
export function modelsCurrent(port: ModeModelConfigSessionPort): string {
  port.assertLive('models.current()');
  return port.record.modelId;
}

/**
 * True once any model has been chosen for this session — either an
 * explicit `models.switch()` call or a `models.setSubagent()` pin. Useful
 * for boot flows that want to gate UI on "has the user picked yet?"
 * without inspecting raw record fields.
 */
export function modelsHasSelected(port: ModeModelConfigSessionPort): boolean {
  port.assertLive('models.hasSelected()');
  if (port.record.modelId && port.record.modelId.length > 0) return true;
  if (Object.keys(port.record.subagentModelOverrides ?? {}).length > 0) return true;
  return false;
}

/**
 * Auth status for the currently resolved model. Routed through
 * `harness.models.getAuthStatus()` when the current model is in the
 * catalog; returns `'unknown'` when no model is selected or the model
 * isn't registered (we don't want the auth-status check to throw on a
 * free-form id the agent layer will accept anyway).
 */
export async function modelsCurrentAuthStatus(port: ModeModelConfigSessionPort): Promise<ModelAuthStatus> {
  port.assertLive('models.currentAuthStatus()');
  const modelId = port.record.modelId;
  if (!modelId) return 'unknown';
  const entry = await port.harness.models.get(modelId);
  if (!entry) return 'unknown';
  return port.harness.models.getAuthStatus(modelId);
}

/** Switch the session's default model id. Free-form string — validated by the agent layer. */
export async function modelsSwitch(port: ModeModelConfigSessionPort, opts: { model: string }): Promise<void> {
  port.assertLive('models.switch()');
  assertModelId('models.switch', opts.model);
  const previousModelId = port.record.modelId;
  if (previousModelId === opts.model) return;
  await port.flushUpdate(prev => ({ ...prev, modelId: opts.model }));
  port.emitter.emit({ type: 'model_changed', modelId: opts.model, previousModelId });
}

/**
 * Pin a model for spawned subagents of a given `agentType`. Override is
 * persisted in `SessionRecord.subagentModelOverrides` and read back by
 * the spawn machinery via `models.getSubagent()`. Emits
 * `model_override_set`. No-op when the same mapping is already set.
 */
export async function modelsSetSubagent(
  port: ModeModelConfigSessionPort,
  opts: { agentType: string; model: string },
): Promise<void> {
  port.assertLive('models.setSubagent()');
  assertAgentType('models.setSubagent', opts.agentType);
  assertModelId('models.setSubagent', opts.model);
  const previousModelId = port.record.subagentModelOverrides?.[opts.agentType] ?? null;
  if (previousModelId === opts.model) return;
  await port.flushUpdate(prev => ({
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
export function modelsGetSubagent(port: ModeModelConfigSessionPort, opts: { agentType: string }): string | null {
  port.assertLive('models.getSubagent()');
  assertAgentType('models.getSubagent', opts.agentType);
  return port.record.subagentModelOverrides?.[opts.agentType] ?? null;
}

export async function uploadSessionAttachment(
  port: ModeModelConfigSessionPort,
  opts: {
    name: string;
    mimeType: string;
    data: Uint8Array;
    onProgress?: (loaded: number, total: number) => void;
  },
): Promise<{ attachmentId: string }> {
  port.assertLive('uploadAttachment()');
  const ref = await port.harness.attachments.upload({
    sessionId: port.id,
    resourceId: port.resourceId,
    kind: 'file',
    filename: opts.name,
    contentType: opts.mimeType,
    data: opts.data,
  });
  opts.onProgress?.(opts.data.byteLength, opts.data.byteLength);
  return { attachmentId: ref.attachmentId };
}

export async function deleteSessionAttachment(
  port: ModeModelConfigSessionPort,
  opts: { attachmentId: string },
): Promise<void> {
  port.assertLive('deleteAttachment()');
  await port.harness.attachments.delete({
    sessionId: port.id,
    resourceId: port.resourceId,
    attachmentId: opts.attachmentId,
  });
}

export async function setSessionThreadSetting(
  port: ModeModelConfigSessionPort,
  opts: { key: string; value: JsonValue },
): Promise<void> {
  port.assertLive('setThreadSetting()');
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
  const settings = await port.harness._threadOps.getSettings({
    resourceId: port.resourceId,
    threadId: port.threadId,
  });
  const rawApp = (settings as { app?: unknown }).app;
  const currentApp =
    rawApp !== null && typeof rawApp === 'object' && !Array.isArray(rawApp)
      ? (rawApp as Record<string, JsonValue>)
      : {};
  await port.harness._threadOps.setSettings({
    resourceId: port.resourceId,
    threadId: port.threadId,
    patch: { app: { ...currentApp, [key]: normalized } },
  });
}

/** Resolve a mode's backing agentId for the run projection, tolerating a mode
 * that has since been removed from config (fall back to the session default). */
export function modeAgentId(port: ModeModelConfigSessionPort, modeId: string): string {
  try {
    return port.harness._getMode(modeId).agentId;
  } catch {
    return port.harness._getMode(port.record.modeId).agentId;
  }
}
