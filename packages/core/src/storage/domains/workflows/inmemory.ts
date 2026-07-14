import { randomUUID } from 'node:crypto';
import type {
  StepResult,
  WorkflowRunState,
  WorkflowTerminalDestinationReceiptRecord,
  WorkflowTerminalEffectRecord,
  WorkflowTerminalSnapshotRecord,
  WorkflowTerminalizationRecord,
} from '../../../workflows';
import { normalizePerPage } from '../../base';
import type {
  AdvanceWorkflowTerminalizationInput,
  AdvanceWorkflowTerminalizationResult,
  ClaimWorkflowTerminalizationInput,
  ClaimWorkflowTerminalizationResult,
  DeleteCompletedWorkflowTerminalizationsInput,
  DeleteCompletedWorkflowTerminalizationsResult,
  GetWorkflowTerminalizationInput,
  GetWorkflowTerminalizationResult,
  GetWorkflowTerminalEffectForDispatchInput,
  GetWorkflowTerminalEffectForDispatchResult,
  GetWorkflowTerminalDestinationReceiptInput,
  GetWorkflowTerminalDestinationReceiptResult,
  PersistWorkflowTerminalStateInput,
  PersistWorkflowTerminalStateResult,
  PrepareWorkflowTerminalEffectInput,
  PrepareWorkflowTerminalEffectResult,
  ReserveWorkflowTerminalDestinationReceiptInput,
  ReserveWorkflowTerminalDestinationReceiptResult,
  ReleaseWorkflowTerminalizationInput,
  ReleaseWorkflowTerminalizationResult,
  StorageWorkflowRun,
  WorkflowRun,
  WorkflowRuns,
  StorageListWorkflowRunsInput,
  UpdateWorkflowStateOptions,
  WorkflowTerminalizationCapabilities,
} from '../../types';
import { createEmptyWorkflowSnapshot, mergeWorkflowStepResult } from '../../workflow-snapshot';
import type { InMemoryDB } from '../inmemory-db';
import { WorkflowsStorage } from './base';
import {
  advanceWorkflowTerminalizationRecord,
  claimWorkflowTerminalizationRecord,
  copyWorkflowTerminalizationRecord,
  copyWorkflowTerminalEffectRecord,
  copyWorkflowTerminalDestinationReceiptRecord,
  getWorkflowTerminalDestinationReceiptRecord,
  getWorkflowTerminalEffectForDispatchRecord,
  materializeWorkflowTerminalEffectDescriptor,
  materializeWorkflowTerminalEffectKind,
  observeWorkflowTerminalizationRecord,
  observeWorkflowTerminalEffectRecord,
  persistWorkflowTerminalStateRecord,
  prepareWorkflowTerminalEffectRecord,
  reserveWorkflowTerminalDestinationReceiptRecord,
  releaseWorkflowTerminalizationRecord,
  validateWorkflowTerminalizationClaim,
  validateWorkflowTerminalEffectIntegrity,
  validateWorkflowTerminalEffectJournalLink,
  validateWorkflowTerminalDestinationReceiptIntegrity,
  validateWorkflowTerminalizationFence,
  validateWorkflowTerminalizationRunIdentity,
  validateWorkflowTerminalizationIdentity,
  validateWorkflowTerminalSnapshotJournalLink,
} from './terminalization';

/**
 * Deep-clone in-memory workflow state.
 *
 * We previously used `JSON.parse(JSON.stringify(x))` here, but the agent loop
 * and workflow engine legitimately place values in step results that don't
 * survive JSON round-tripping:
 * - `Date` instances (e.g. `response.timestamp`) — JSON turns them into ISO
 *   strings, downstream consumers that do `.getTime()` then break.
 * - Explicitly-`undefined` properties (e.g. `headers`, `providerMetadata`,
 *   `usage.{cacheRead, cacheWrite, reasoning}`) — JSON drops keys with
 *   `undefined` values, breaking snapshot assertions that include them.
 * - `Error` instances (e.g. tool execution failures, AssertionErrors from
 *   inside `tool.execute`) — JSON strips `message`/`name`/`stack` (non-
 *   enumerable). `structuredClone` isn't enough either — it preserves the
 *   Error type but drops subclass-specific enumerable props (`actual`,
 *   `expected`, `operator`).
 *
 * The custom walk below preserves all of that. It also handles builtins with
 * internal slots explicitly — `Map`, `Set`, `RegExp`, `URL`, `ArrayBuffer`,
 * typed arrays, and `DataView` — because cloning them via `Object.create(proto)`
 * would produce a value that passes `instanceof` but whose methods throw (the
 * internal slots were never initialized). Null-prototype dictionaries keep
 * their null prototype.
 */
/** @internal Exported for testing only. */
export function cloneRunData<T>(value: T): T {
  return deepCloneForRun(value, new WeakMap()) as T;
}

function materializeTerminalSnapshot(snapshot: WorkflowRunState): WorkflowRunState {
  const materialized = cloneRunData(snapshot);
  const runId = materialized.runId;
  const status = materialized.status;
  // The canonical snapshot is data, not an executable wrapper. Flatten its
  // top-level prototype so inherited accessors cannot change journal evidence
  // after validation; nested values keep cloneRunData's richer semantics.
  Object.setPrototypeOf(materialized, Object.prototype);
  Object.defineProperties(materialized, {
    runId: { configurable: true, enumerable: true, writable: true, value: runId },
    status: { configurable: true, enumerable: true, writable: true, value: status },
  });
  return materialized;
}

function deepCloneForRun(value: unknown, seen: WeakMap<object, unknown>): unknown {
  if (value === null || typeof value !== 'object') return value;
  const cached = seen.get(value as object);
  if (cached !== undefined) return cached;

  if (value instanceof Date) {
    return new Date(value.getTime());
  }

  if (value instanceof RegExp) {
    return new RegExp(value.source, value.flags);
  }

  if (value instanceof URL) {
    return new URL(value.href);
  }

  if (value instanceof Map) {
    const out = new Map();
    seen.set(value, out);
    for (const [k, v] of value) {
      out.set(deepCloneForRun(k, seen), deepCloneForRun(v, seen));
    }
    return out;
  }

  if (value instanceof Set) {
    const out = new Set();
    seen.set(value, out);
    for (const v of value) {
      out.add(deepCloneForRun(v, seen));
    }
    return out;
  }

  if (value instanceof ArrayBuffer) {
    return value.slice(0);
  }

  // Typed arrays and DataView — `Object.create(proto)` would yield a shell with
  // no backing buffer, so rebuild against a fresh copy of the underlying bytes.
  if (ArrayBuffer.isView(value)) {
    if (value instanceof DataView) {
      return new DataView(value.buffer.slice(value.byteOffset, value.byteOffset + value.byteLength));
    }
    const typed = value as unknown as Uint8Array;
    return new (typed.constructor as Uint8ArrayConstructor)(typed);
  }

  if (value instanceof Error) {
    // Clone via Object.create(proto) so `instanceof Error` and subclass
    // branches keep working (e.g. `expect.any(Error)`) without invoking
    // subclass constructors that may have non-standard signatures
    // (AssertionError expects an options object). Surface `message` as an
    // enumerable own prop so Vitest's snapshot serializer renders it
    // alongside subclass-specific fields.
    const out = Object.create(Object.getPrototypeOf(value)) as Error;
    Object.defineProperty(out, 'message', {
      value: value.message,
      writable: true,
      configurable: true,
      enumerable: true,
    });
    Object.defineProperty(out, 'name', { value: value.name, writable: true, configurable: true });
    // For `stack`, defer to the Error's own `toJSON` if present — that's how
    // producers signal whether they want stack persisted (e.g. step-executor
    // wraps via `getErrorFromUnknown(err, { serializeStack: false })` so the
    // attached toJSON omits stack from the JSON form). We only honour
    // toJSON's stack signal here, not its other fields, to avoid pulling in
    // subclass extras like Chai AssertionError.toJSON's name/ok/stack that
    // the agent-loop snapshot tests don't expect.
    const errRecord = value as unknown as Record<string, unknown>;
    let includeStack = value.stack !== undefined;
    if (includeStack && typeof errRecord.toJSON === 'function') {
      try {
        const serialized = (errRecord.toJSON as () => unknown)();
        if (serialized && typeof serialized === 'object' && !('stack' in serialized)) {
          includeStack = false;
        }
      } catch {
        // Defensive: if toJSON throws, fall back to default behaviour.
      }
    }
    if (includeStack) {
      Object.defineProperty(out, 'stack', { value: value.stack, writable: true, configurable: true });
    }
    // Register in `seen` BEFORE recursing so cycles (incl. self-referential
    // `cause`) terminate.
    seen.set(value, out);
    const outRecord = out as unknown as Record<string, unknown>;
    if (value.cause !== undefined) outRecord.cause = deepCloneForRun(value.cause, seen);
    for (const key of Object.keys(value)) {
      outRecord[key] = deepCloneForRun(errRecord[key], seen);
    }
    return out;
  }

  if (Array.isArray(value)) {
    const out: unknown[] = new Array(value.length);
    seen.set(value, out);
    for (let i = 0; i < value.length; i++) {
      out[i] = deepCloneForRun(value[i], seen);
    }
    return out;
  }

  // Preserve the prototype so class instances stay recognizable to consumers
  // (e.g. `DefaultStepResult` in the agent loop, anything that uses `instanceof`
  // or Vitest's snapshot serializer which prints the class name) and so
  // null-prototype dictionaries (`Object.create(null)`) keep their null proto
  // rather than silently becoming plain `{}`. Builtins with internal slots
  // (Map/Set/RegExp/typed arrays/Date/Error) are handled explicitly above, so
  // the only objects reaching here are plain objects and plain data-holder
  // class instances — `Object.create(proto)` + an own-property copy reproduces
  // those faithfully.
  const proto = Object.getPrototypeOf(value);
  const out: Record<string, unknown> =
    proto === Object.prototype ? {} : (Object.create(proto) as Record<string, unknown>);
  seen.set(value, out);
  // `Object.keys` includes keys whose value is `undefined`, so explicitly-undefined
  // properties are preserved (unlike a JSON round-trip).
  for (const key of Object.keys(value as object)) {
    out[key] = deepCloneForRun((value as Record<string, unknown>)[key], seen);
  }
  return out;
}

export class WorkflowsInMemory extends WorkflowsStorage {
  private db: InMemoryDB;

  constructor({ db }: { db: InMemoryDB }) {
    super();
    this.db = db;
  }

  supportsConcurrentUpdates(): boolean {
    return true;
  }

  supportsWorkflowTerminalizationJournal(): boolean {
    return true;
  }

  getWorkflowTerminalizationCapabilities(): WorkflowTerminalizationCapabilities {
    return { journalVersion: 1, producerOutboxVersion: 1, destinationReceiptVersion: 1 };
  }

  private getTerminalizationKey(workflowName: string, runId: string): string {
    return this.getWorkflowKey(workflowName, runId);
  }

  private getTerminalEffectKey(workflowName: string, runId: string, kind: string): string {
    return JSON.stringify([workflowName, runId, kind]);
  }

  private getTerminalDestinationReceipt(
    effect: WorkflowTerminalEffectRecord,
    consumerId: string,
  ): WorkflowTerminalDestinationReceiptRecord | undefined {
    const matches = this.db.workflowTerminalDestinationReceipts.findMatches(effect, consumerId);
    if (matches.length > 1) {
      throw new TypeError('Conflicting workflow terminal destination receipt storage');
    }
    return matches[0];
  }

  private resolveTerminalDestinationReceiptPreflight(
    operation: ReserveWorkflowTerminalDestinationReceiptInput | GetWorkflowTerminalDestinationReceiptInput,
    now: number,
  ):
    | { status: 'missing_run' }
    | {
        status: 'found';
        journalKey: string;
        journal: WorkflowTerminalizationRecord | undefined;
        effect: WorkflowTerminalEffectRecord | undefined;
        receipt: WorkflowTerminalDestinationReceiptRecord | undefined;
      } {
    validateWorkflowTerminalizationFence(operation);
    validateWorkflowTerminalizationRunIdentity(operation);
    validateWorkflowTerminalizationIdentity(operation.consumerId, 'consumerId', 256);
    const journalKey = this.getTerminalizationKey(operation.workflowName, operation.runId);
    const workflowKey = this.getWorkflowKey(operation.workflowName, operation.runId);
    const journal = this.db.workflowTerminalizations.get(journalKey);
    if (!journal && !this.db.workflows.has(workflowKey)) return { status: 'missing_run' };
    const effect = this.db.workflowTerminalEffects.get(
      this.getTerminalEffectKey(operation.workflowName, operation.runId, operation.effectKind),
    );
    if (effect && journal) {
      validateWorkflowTerminalEffectIntegrity(effect);
      validateWorkflowTerminalEffectJournalLink(effect, journal, operation.workflowName, operation.runId);
    }
    const receipt = effect ? this.getTerminalDestinationReceipt(effect, operation.consumerId) : undefined;
    if (effect && receipt) {
      validateWorkflowTerminalDestinationReceiptIntegrity(receipt, effect, now);
      if (receipt.consumerId !== operation.consumerId) {
        throw new TypeError('Conflicting workflow terminal destination receipt storage');
      }
    }
    return { status: 'found', journalKey, journal, effect, receipt };
  }

  async claimWorkflowTerminalization(
    input: ClaimWorkflowTerminalizationInput,
  ): Promise<ClaimWorkflowTerminalizationResult> {
    const operation: ClaimWorkflowTerminalizationInput = {
      workflowName: input.workflowName,
      runId: input.runId,
      eventKey: input.eventKey,
      terminalStatus: input.terminalStatus,
      ownerId: input.ownerId,
      leaseMs: input.leaseMs,
      claimToken: input.claimToken,
      claimGeneration: input.claimGeneration,
    };
    validateWorkflowTerminalizationClaim(operation);
    const key = this.getTerminalizationKey(operation.workflowName, operation.runId);
    const existing = this.db.workflowTerminalizations.get(key);
    if (!existing && !this.db.workflows.has(this.getWorkflowKey(operation.workflowName, operation.runId))) {
      return { status: 'missing_run' };
    }
    const result = claimWorkflowTerminalizationRecord(existing, operation, Date.now(), randomUUID());
    if (result.status === 'acquired' || result.status === 'renewed') {
      this.db.workflowTerminalizations.set(key, copyWorkflowTerminalizationRecord(result.record));
      return result;
    }
    return 'record' in result
      ? { status: result.status, record: observeWorkflowTerminalizationRecord(result.record) }
      : result;
  }

  async getWorkflowTerminalization(input: GetWorkflowTerminalizationInput): Promise<GetWorkflowTerminalizationResult> {
    const operation: GetWorkflowTerminalizationInput = {
      workflowName: input.workflowName,
      runId: input.runId,
    };
    const record = this.db.workflowTerminalizations.get(
      this.getTerminalizationKey(operation.workflowName, operation.runId),
    );
    if (record) return { status: 'found', record: observeWorkflowTerminalizationRecord(record) };
    return this.db.workflows.has(this.getWorkflowKey(operation.workflowName, operation.runId))
      ? { status: 'missing_record' }
      : { status: 'missing_run' };
  }

  async advanceWorkflowTerminalization(
    input: AdvanceWorkflowTerminalizationInput,
  ): Promise<AdvanceWorkflowTerminalizationResult> {
    const operation: AdvanceWorkflowTerminalizationInput = {
      workflowName: input.workflowName,
      runId: input.runId,
      ownerId: input.ownerId,
      claimToken: input.claimToken,
      claimGeneration: input.claimGeneration,
      expectedPhase: input.expectedPhase,
      nextPhase: input.nextPhase,
      leaseMs: input.leaseMs,
    };
    validateWorkflowTerminalizationFence(operation);
    const key = this.getTerminalizationKey(operation.workflowName, operation.runId);
    const existing = this.db.workflowTerminalizations.get(key);
    if (!existing && !this.db.workflows.has(this.getWorkflowKey(operation.workflowName, operation.runId))) {
      return { status: 'missing_run' };
    }
    const result = advanceWorkflowTerminalizationRecord(existing, operation, Date.now());
    if (result.status === 'advanced') {
      this.db.workflowTerminalizations.set(key, copyWorkflowTerminalizationRecord(result.record));
    }
    return 'record' in result
      ? { status: result.status, record: observeWorkflowTerminalizationRecord(result.record) }
      : result;
  }

  async releaseWorkflowTerminalization(
    input: ReleaseWorkflowTerminalizationInput,
  ): Promise<ReleaseWorkflowTerminalizationResult> {
    const operation: ReleaseWorkflowTerminalizationInput = {
      workflowName: input.workflowName,
      runId: input.runId,
      ownerId: input.ownerId,
      claimToken: input.claimToken,
      claimGeneration: input.claimGeneration,
    };
    validateWorkflowTerminalizationFence(operation);
    const key = this.getTerminalizationKey(operation.workflowName, operation.runId);
    const existing = this.db.workflowTerminalizations.get(key);
    if (!existing && !this.db.workflows.has(this.getWorkflowKey(operation.workflowName, operation.runId))) {
      return { status: 'missing_run' };
    }
    const result = releaseWorkflowTerminalizationRecord(existing, operation, Date.now());
    if (result.status === 'released') {
      this.db.workflowTerminalizations.set(key, copyWorkflowTerminalizationRecord(result.record));
    }
    return 'record' in result
      ? { status: result.status, record: observeWorkflowTerminalizationRecord(result.record) }
      : result;
  }

  async deleteCompletedWorkflowTerminalizations(
    input: DeleteCompletedWorkflowTerminalizationsInput,
  ): Promise<DeleteCompletedWorkflowTerminalizationsResult> {
    const operation: DeleteCompletedWorkflowTerminalizationsInput = {
      workflowName: input.workflowName,
      runId: input.runId,
      olderThan: input.olderThan,
    };
    const olderThan = operation.olderThan.getTime();
    if (Number.isNaN(olderThan)) throw new TypeError('olderThan must be a valid Date');

    const key = this.getTerminalizationKey(operation.workflowName, operation.runId);
    const record = this.db.workflowTerminalizations.get(key);
    if (!record) {
      return this.db.workflows.has(this.getWorkflowKey(operation.workflowName, operation.runId))
        ? { status: 'deleted', count: 0 }
        : { status: 'missing_run', count: 0 };
    }
    if (record.phase === 'complete' && record.completedAt !== undefined && record.completedAt < olderThan) {
      this.db.workflowTerminalizations.delete(key);
      this.db.workflowTerminalEffects.delete(
        this.getTerminalEffectKey(operation.workflowName, operation.runId, 'parent-workflow-step-end'),
      );
      this.db.workflowTerminalEffects.delete(
        this.getTerminalEffectKey(operation.workflowName, operation.runId, 'workflow-finish'),
      );
      this.db.workflowTerminalSnapshots.delete(key);
      for (const [receiptKey, receipt] of this.db.workflowTerminalDestinationReceipts) {
        if (receipt.workflowName === operation.workflowName && receipt.runId === operation.runId) {
          this.db.workflowTerminalDestinationReceipts.delete(receiptKey);
        }
      }
      return { status: 'deleted', count: 1 };
    }
    return { status: 'deleted', count: 0 };
  }

  async persistWorkflowTerminalState(
    input: PersistWorkflowTerminalStateInput,
  ): Promise<PersistWorkflowTerminalStateResult> {
    // Read the operation envelope exactly once. Accessor-backed inputs must not
    // be able to point the journal lookup and snapshot write at different runs.
    const operation: PersistWorkflowTerminalStateInput = {
      workflowName: input.workflowName,
      runId: input.runId,
      ownerId: input.ownerId,
      claimToken: input.claimToken,
      claimGeneration: input.claimGeneration,
      snapshot: input.snapshot,
      resourceId: input.resourceId,
      leaseMs: input.leaseMs,
    };
    validateWorkflowTerminalizationFence(operation);
    const journalKey = this.getTerminalizationKey(operation.workflowName, operation.runId);
    const workflowKey = this.getWorkflowKey(operation.workflowName, operation.runId);
    const existingJournal = this.db.workflowTerminalizations.get(journalKey);
    const existingRun = this.db.workflows.get(workflowKey);
    if (!existingJournal && !existingRun) return { status: 'missing_run' };
    const result = persistWorkflowTerminalStateRecord(
      existingJournal,
      operation,
      Date.now(),
      materializeTerminalSnapshot,
    );
    if (result.status === 'advanced') {
      if (!existingRun) return { status: 'missing_run' };
      const now = new Date(result.record.updatedAt);
      if (this.db.workflowTerminalSnapshots.has(journalKey)) {
        throw new TypeError('Workflow terminal state already retained');
      }
      const resourceId = operation.resourceId ?? existingRun.resourceId;
      const retained: WorkflowTerminalSnapshotRecord = {
        version: 1,
        workflowName: operation.workflowName,
        runId: operation.runId,
        ...(resourceId === undefined ? {} : { resourceId }),
        terminalStatus: result.record.terminalStatus,
        snapshot: cloneRunData(result.snapshot),
        createdAt: result.record.updatedAt,
      };
      this.db.workflows.set(workflowKey, {
        ...existingRun,
        resourceId,
        snapshot: result.snapshot,
        updatedAt: now,
      });
      this.db.workflowTerminalSnapshots.set(journalKey, retained);
      this.db.workflowTerminalizations.set(journalKey, copyWorkflowTerminalizationRecord(result.record));
      return { status: 'persisted', record: observeWorkflowTerminalizationRecord(result.record) };
    }
    return 'record' in result
      ? { status: result.status, record: observeWorkflowTerminalizationRecord(result.record) }
      : result;
  }

  async prepareWorkflowTerminalEffect(
    input: PrepareWorkflowTerminalEffectInput,
  ): Promise<PrepareWorkflowTerminalEffectResult> {
    const operation: PrepareWorkflowTerminalEffectInput = {
      workflowName: input.workflowName,
      runId: input.runId,
      ownerId: input.ownerId,
      claimToken: input.claimToken,
      claimGeneration: input.claimGeneration,
      expectedPhase: input.expectedPhase,
      effect: materializeWorkflowTerminalEffectDescriptor(input.effect),
      leaseMs: input.leaseMs,
    };
    validateWorkflowTerminalizationFence(operation);
    const journalKey = this.getTerminalizationKey(operation.workflowName, operation.runId);
    const workflowKey = this.getWorkflowKey(operation.workflowName, operation.runId);
    const effectKey = this.getTerminalEffectKey(operation.workflowName, operation.runId, operation.effect.kind);
    const existingJournal = this.db.workflowTerminalizations.get(journalKey);
    if (!existingJournal && !this.db.workflows.has(workflowKey)) return { status: 'missing_run' };
    const existingEffect = this.db.workflowTerminalEffects.get(effectKey);
    if (existingEffect && existingJournal) {
      if (existingEffect.kind !== operation.effect.kind) {
        throw new TypeError('Invalid workflow terminal effect kind');
      }
      validateWorkflowTerminalEffectIntegrity(existingEffect);
      validateWorkflowTerminalEffectJournalLink(
        existingEffect,
        existingJournal,
        operation.workflowName,
        operation.runId,
      );
    }
    const result = prepareWorkflowTerminalEffectRecord(existingJournal, existingEffect, operation, Date.now());
    if (result.status === 'prepared' || result.status === 'already_prepared') {
      const retained = this.db.workflowTerminalSnapshots.get(journalKey);
      if (!retained) return { status: 'missing_terminal_state' };
      validateWorkflowTerminalSnapshotJournalLink(retained, existingJournal!, operation.workflowName, operation.runId);
    }
    if (result.status === 'prepared') {
      this.db.workflowTerminalEffects.set(effectKey, copyWorkflowTerminalEffectRecord(result.effect));
      this.db.workflowTerminalizations.set(journalKey, copyWorkflowTerminalizationRecord(result.record));
      return { status: result.status, effect: copyWorkflowTerminalEffectRecord(result.effect) };
    }
    if (result.status === 'already_prepared') {
      if (operation.leaseMs !== undefined) {
        this.db.workflowTerminalizations.set(journalKey, copyWorkflowTerminalizationRecord(result.record));
      }
      return { status: result.status, effect: copyWorkflowTerminalEffectRecord(result.effect) };
    }
    if (result.status === 'effect_conflict') {
      return {
        status: result.status,
        effect: observeWorkflowTerminalEffectRecord(result.effect),
        record: observeWorkflowTerminalizationRecord(result.record),
      };
    }
    return 'record' in result
      ? { status: result.status, record: observeWorkflowTerminalizationRecord(result.record) }
      : result;
  }

  async getWorkflowTerminalEffectForDispatch(
    input: GetWorkflowTerminalEffectForDispatchInput,
  ): Promise<GetWorkflowTerminalEffectForDispatchResult> {
    const operation: GetWorkflowTerminalEffectForDispatchInput = {
      workflowName: input.workflowName,
      runId: input.runId,
      ownerId: input.ownerId,
      claimToken: input.claimToken,
      claimGeneration: input.claimGeneration,
      kind: materializeWorkflowTerminalEffectKind(input.kind),
    };
    validateWorkflowTerminalizationFence(operation);
    validateWorkflowTerminalizationRunIdentity(operation);
    const journalKey = this.getTerminalizationKey(operation.workflowName, operation.runId);
    const workflowKey = this.getWorkflowKey(operation.workflowName, operation.runId);
    const existingJournal = this.db.workflowTerminalizations.get(journalKey);
    if (!existingJournal && !this.db.workflows.has(workflowKey)) return { status: 'missing_run' };
    const existingEffect = this.db.workflowTerminalEffects.get(
      this.getTerminalEffectKey(operation.workflowName, operation.runId, operation.kind),
    );
    if (existingEffect && existingJournal) {
      if (existingEffect.kind !== operation.kind) {
        throw new TypeError('Invalid workflow terminal effect kind');
      }
      validateWorkflowTerminalEffectIntegrity(existingEffect);
      validateWorkflowTerminalEffectJournalLink(
        existingEffect,
        existingJournal,
        operation.workflowName,
        operation.runId,
      );
    }
    const result = getWorkflowTerminalEffectForDispatchRecord(existingJournal, existingEffect, operation, Date.now());
    const retained = result.status === 'found' ? this.db.workflowTerminalSnapshots.get(journalKey) : undefined;
    if (result.status === 'found' && !retained) return { status: 'missing_terminal_state' };
    if (result.status === 'found') {
      validateWorkflowTerminalSnapshotJournalLink(retained!, existingJournal!, operation.workflowName, operation.runId);
    }
    return 'record' in result
      ? { status: result.status, record: observeWorkflowTerminalizationRecord(result.record) }
      : result.status === 'found'
        ? {
            ...result,
            snapshot: cloneRunData(retained!.snapshot),
            ...(retained!.resourceId === undefined ? {} : { resourceId: retained!.resourceId }),
          }
        : result;
  }

  async reserveWorkflowTerminalDestinationReceipt(
    input: ReserveWorkflowTerminalDestinationReceiptInput,
  ): Promise<ReserveWorkflowTerminalDestinationReceiptResult> {
    const operation: ReserveWorkflowTerminalDestinationReceiptInput = {
      workflowName: input.workflowName,
      runId: input.runId,
      ownerId: input.ownerId,
      claimToken: input.claimToken,
      claimGeneration: input.claimGeneration,
      effectKind: materializeWorkflowTerminalEffectKind(input.effectKind),
      consumerId: input.consumerId,
    };
    const now = Date.now();
    const preflight = this.resolveTerminalDestinationReceiptPreflight(operation, now);
    if (preflight.status === 'missing_run') return preflight;
    const { journalKey, journal, effect, receipt: existingReceipt } = preflight;
    const existingReceiptCount = effect ? this.db.workflowTerminalDestinationReceipts.countForEffect(effect) : 0;
    const result = reserveWorkflowTerminalDestinationReceiptRecord(
      journal,
      effect,
      existingReceipt,
      existingReceiptCount,
      operation,
      now,
    );
    if (result.status === 'reserved' || result.status === 'already_exists') {
      const retained = this.db.workflowTerminalSnapshots.get(journalKey);
      if (!retained) return { status: 'missing_terminal_state' };
      validateWorkflowTerminalSnapshotJournalLink(retained, journal!, operation.workflowName, operation.runId);
      if (result.status === 'reserved') {
        this.db.workflowTerminalDestinationReceipts.set(
          JSON.stringify([result.receipt.effectKey, result.receipt.consumerId]),
          copyWorkflowTerminalDestinationReceiptRecord(result.receipt),
        );
      }
      return { status: result.status, receipt: copyWorkflowTerminalDestinationReceiptRecord(result.receipt) };
    }
    return 'record' in result
      ? { status: result.status, record: observeWorkflowTerminalizationRecord(result.record) }
      : result;
  }

  async getWorkflowTerminalDestinationReceipt(
    input: GetWorkflowTerminalDestinationReceiptInput,
  ): Promise<GetWorkflowTerminalDestinationReceiptResult> {
    const operation: GetWorkflowTerminalDestinationReceiptInput = {
      workflowName: input.workflowName,
      runId: input.runId,
      ownerId: input.ownerId,
      claimToken: input.claimToken,
      claimGeneration: input.claimGeneration,
      effectKind: materializeWorkflowTerminalEffectKind(input.effectKind),
      consumerId: input.consumerId,
    };
    const now = Date.now();
    const preflight = this.resolveTerminalDestinationReceiptPreflight(operation, now);
    if (preflight.status === 'missing_run') return preflight;
    const { journalKey, journal, effect, receipt } = preflight;
    const result = getWorkflowTerminalDestinationReceiptRecord(journal, effect, receipt, operation, now);
    if (result.status === 'found') {
      const retained = this.db.workflowTerminalSnapshots.get(journalKey);
      if (!retained) return { status: 'missing_terminal_state' };
      validateWorkflowTerminalSnapshotJournalLink(retained, journal!, operation.workflowName, operation.runId);
      return { status: 'found', receipt: copyWorkflowTerminalDestinationReceiptRecord(result.receipt) };
    }
    return 'record' in result
      ? { status: result.status, record: observeWorkflowTerminalizationRecord(result.record) }
      : result;
  }

  async dangerouslyClearAll(): Promise<void> {
    this.db.workflows.clear();
    this.db.workflowTerminalizations.clear();
    this.db.workflowTerminalEffects.clear();
    this.db.workflowTerminalSnapshots.clear();
    this.db.workflowTerminalDestinationReceipts.clear();
  }

  private getWorkflowKey(workflowName: string, runId: string): string {
    // A delimiter-joined key aliases distinct identities such as
    // (`a-b`, `c`) and (`a`, `b-c`). Preserve the tuple boundary instead.
    return JSON.stringify([workflowName, runId]);
  }

  async updateWorkflowResults({
    workflowName,
    runId,
    stepId,
    result,
    requestContext,
  }: {
    workflowName: string;
    runId: string;
    stepId: string;
    result: StepResult<any, any, any, any>;
    requestContext: Record<string, any>;
  }): Promise<Record<string, StepResult<any, any, any, any>>> {
    const key = this.getWorkflowKey(workflowName, runId);
    const run = this.db.workflows.get(key);

    if (!run) {
      return {};
    }

    let snapshot: WorkflowRunState;
    if (!run.snapshot) {
      snapshot = createEmptyWorkflowSnapshot(run.run_id);

      this.db.workflows.set(key, {
        ...run,
        snapshot,
      });
    } else {
      snapshot = typeof run.snapshot === 'string' ? JSON.parse(run.snapshot) : run.snapshot;
    }

    if (!snapshot || !snapshot?.context) {
      throw new Error(`Snapshot not found for runId ${runId}`);
    }

    const context = mergeWorkflowStepResult({ snapshot, stepId, result, requestContext });

    this.db.workflows.set(key, {
      ...run,
      snapshot: snapshot,
    });

    return cloneRunData(context);
  }

  async updateWorkflowState({
    workflowName,
    runId,
    opts,
  }: {
    workflowName: string;
    runId: string;
    opts: UpdateWorkflowStateOptions;
  }): Promise<WorkflowRunState | undefined> {
    const key = this.getWorkflowKey(workflowName, runId);
    const run = this.db.workflows.get(key);

    if (!run) {
      return;
    }

    let snapshot: WorkflowRunState;
    if (!run.snapshot) {
      snapshot = createEmptyWorkflowSnapshot(run.run_id);

      this.db.workflows.set(key, {
        ...run,
        snapshot,
      });
    } else {
      snapshot = typeof run.snapshot === 'string' ? JSON.parse(run.snapshot) : run.snapshot;
    }

    if (!snapshot || !snapshot?.context) {
      throw new Error(`Snapshot not found for runId ${runId}`);
    }

    snapshot = { ...snapshot, ...opts };
    this.db.workflows.set(key, {
      ...run,
      snapshot: snapshot,
    });

    return snapshot;
  }

  async persistWorkflowSnapshot({
    workflowName,
    runId,
    resourceId,
    snapshot,
    createdAt,
    updatedAt,
  }: {
    workflowName: string;
    runId: string;
    resourceId?: string;
    snapshot: WorkflowRunState;
    createdAt?: Date;
    updatedAt?: Date;
  }): Promise<void> {
    const key = this.getWorkflowKey(workflowName, runId);
    const now = new Date();
    const existing = this.db.workflows.get(key);
    const data: StorageWorkflowRun = {
      workflow_name: workflowName,
      run_id: runId,
      resourceId,
      snapshot,
      // Preserve the original creation time when re-persisting an existing run; only set it
      // on first insert. Otherwise listWorkflowRuns ordering and date filters drift to the
      // last activity time. Matches the persistent stores (pg/mysql/mongodb/libsql).
      createdAt: createdAt ?? existing?.createdAt ?? now,
      updatedAt: updatedAt ?? now,
    };

    this.db.workflows.set(key, data);
  }

  async loadWorkflowSnapshot({
    workflowName,
    runId,
  }: {
    workflowName: string;
    runId: string;
  }): Promise<WorkflowRunState | null> {
    const key = this.getWorkflowKey(workflowName, runId);
    const run = this.db.workflows.get(key);

    if (!run) {
      return null;
    }

    const snapshot = typeof run.snapshot === 'string' ? JSON.parse(run.snapshot) : run.snapshot;
    // Return a deep copy to prevent mutation
    return snapshot ? cloneRunData(snapshot) : null;
  }

  async listWorkflowRuns({
    workflowName,
    fromDate,
    toDate,
    perPage,
    page,
    resourceId,
    status,
  }: StorageListWorkflowRunsInput = {}): Promise<WorkflowRuns> {
    if (page !== undefined && page < 0) {
      throw new Error('page must be >= 0');
    }

    let runs = Array.from(this.db.workflows.values());

    if (workflowName) runs = runs.filter((run: any) => run.workflow_name === workflowName);
    if (status) {
      runs = runs.filter((run: any) => {
        let snapshot: WorkflowRunState | string = run?.snapshot!;

        if (!snapshot) {
          return false;
        }

        if (typeof snapshot === 'string') {
          try {
            snapshot = JSON.parse(snapshot) as WorkflowRunState;
          } catch {
            return false;
          }
        } else {
          snapshot = cloneRunData(snapshot) as WorkflowRunState;
        }

        return snapshot.status === status;
      });
    }

    if (fromDate && toDate) {
      runs = runs.filter(
        (run: any) =>
          new Date(run.createdAt).getTime() >= fromDate.getTime() &&
          new Date(run.createdAt).getTime() <= toDate.getTime(),
      );
    } else if (fromDate) {
      runs = runs.filter((run: any) => new Date(run.createdAt).getTime() >= fromDate.getTime());
    } else if (toDate) {
      runs = runs.filter((run: any) => new Date(run.createdAt).getTime() <= toDate.getTime());
    }
    if (resourceId) runs = runs.filter((run: any) => run.resourceId === resourceId);

    const total = runs.length;

    // Sort by createdAt
    runs.sort((a: any, b: any) => new Date(b.createdAt).getTime() - new Date(a.createdAt).getTime());

    // Apply pagination
    if (perPage !== undefined && page !== undefined) {
      // Use MAX_SAFE_INTEGER as default to maintain "no pagination" behavior when undefined
      const normalizedPerPage = normalizePerPage(perPage, Number.MAX_SAFE_INTEGER);
      const offset = page * normalizedPerPage;
      const start = offset;
      const end = start + normalizedPerPage;
      runs = runs.slice(start, end);
    }

    // Deserialize snapshot if it's a string
    const parsedRuns = runs.map((run: any) => ({
      ...run,
      snapshot: typeof run.snapshot === 'string' ? JSON.parse(run.snapshot) : cloneRunData(run.snapshot),
      createdAt: new Date(run.createdAt),
      updatedAt: new Date(run.updatedAt),
      runId: run.run_id,
      workflowName: run.workflow_name,
      resourceId: run.resourceId,
    }));

    return { runs: parsedRuns as WorkflowRun[], total };
  }

  async getWorkflowRunById({
    runId,
    workflowName,
  }: {
    runId: string;
    workflowName?: string;
  }): Promise<WorkflowRun | null> {
    const runs = Array.from(this.db.workflows.values()).filter((r: any) => r.run_id === runId);
    let run = runs.find((r: any) => r.workflow_name === workflowName);

    if (!run) return null;

    // Return a deep copy to prevent mutation
    const parsedRun = {
      ...run,
      snapshot: typeof run.snapshot === 'string' ? JSON.parse(run.snapshot) : cloneRunData(run.snapshot),
      createdAt: new Date(run.createdAt),
      updatedAt: new Date(run.updatedAt),
      runId: run.run_id,
      workflowName: run.workflow_name,
      resourceId: run.resourceId,
    };

    return parsedRun as WorkflowRun;
  }

  async deleteWorkflowRunById({ runId, workflowName }: { runId: string; workflowName: string }): Promise<void> {
    const key = this.getWorkflowKey(workflowName, runId);
    this.db.workflows.delete(key);
  }
}
