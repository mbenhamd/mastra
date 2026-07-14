import { randomUUID } from 'node:crypto';
import { ErrorCategory, ErrorDomain, MastraError } from '@mastra/core/error';
import {
  mergeWorkflowStepResult,
  normalizePerPage,
  TABLE_WORKFLOW_SNAPSHOT,
  TABLE_SCHEMAS,
  WorkflowsStorage,
  applyWorkflowTerminalParentContinuationPatch,
  copyWorkflowTerminalParentContinuationContract,
  WorkflowTerminalContinuationStoredStateError,
  MAX_WORKFLOW_TERMINALIZATION_LEASE_MS,
  MAX_WORKFLOW_TERMINAL_PARENT_EXECUTION_PATH_LENGTH,
  advanceWorkflowTerminalizationRecord,
  authorizeWorkflowTerminalStateRecord,
  bindWorkflowNestedRunOwnershipRecord,
  captureWorkflowRunIdentity,
  captureWorkflowNestedRunAdmissionInput,
  claimWorkflowTerminalizationRecord,
  createStorageErrorId,
  createWorkflowTerminalDestinationReceiptRecord,
  materializeWorkflowTerminalEffectDescriptor,
  materializeWorkflowTerminalEffectKind,
  observeWorkflowTerminalizationRecord,
  observeWorkflowTerminalEffectRecord,
  copyWorkflowTerminalEffectRecord,
  copyWorkflowTerminalDestinationReceiptRecord,
  copyWorkflowTerminalContinuationPlanRecord,
  getWorkflowTerminalDestinationReceiptRecord,
  getWorkflowTerminalContinuationPlanRecord,
  getWorkflowTerminalSnapshotRecordHash,
  persistWorkflowTerminalStateRecord,
  prepareWorkflowTerminalEffectRecord,
  reserveWorkflowTerminalDestinationReceiptRecord,
  getWorkflowTerminalEffectForDispatchRecord,
  releaseWorkflowTerminalizationRecord,
  validateWorkflowTerminalizationClaim,
  validateWorkflowTerminalEffectIntegrity,
  validateWorkflowTerminalEffectJournalLink,
  validateWorkflowTerminalSnapshotJournalLink,
  validateWorkflowTerminalSnapshotRecordIntegrity,
  validateWorkflowTerminalEffectRecoveryLink,
  createWorkflowTerminalRecoveryAncestryRecord,
  copyWorkflowTerminalRecoveryAncestryRecord,
  validateWorkflowTerminalRecoveryAncestryRecord,
  sameWorkflowTerminalRecoveryAncestry,
  validateWorkflowTerminalDestinationReceiptIntegrity,
  validateWorkflowTerminalizationFence,
  validateWorkflowTerminalizationRunIdentity,
  validateWorkflowTerminalizationIdentity,
  validateWorkflowNestedRunOwnershipInput,
  validateWorkflowNestedRunInitialSnapshot,
  inspectWorkflowNestedRunRetainedSnapshot,
  validateWorkflowRunSnapshotShape,
  validateWorkflowSnapshotTimestampForFinalState,
  prepareWorkflowTerminalParentApplicationRecords,
  finalizeWorkflowTerminalParentApplicationRecords,
  observeWorkflowTerminalContinuationPlanRecord,
  validateWorkflowTerminalContinuationPlanIntegrity,
  WORKFLOW_TERMINAL_PARENT_APPLICATION_CONSUMER_ID,
} from '@mastra/core/storage';
import type {
  AdvanceWorkflowTerminalizationInput,
  AdvanceWorkflowTerminalizationResult,
  ClaimWorkflowTerminalizationInput,
  ClaimWorkflowTerminalizationResult,
  DeleteCompletedWorkflowTerminalizationsInput,
  DeleteCompletedWorkflowTerminalizationsResult,
  GetWorkflowTerminalizationInput,
  GetWorkflowTerminalizationResult,
  GetWorkflowRunTerminalStatusInput,
  GetWorkflowRunTerminalStatusResult,
  GetWorkflowTerminalEffectForDispatchInput,
  GetWorkflowTerminalEffectForDispatchResult,
  GetWorkflowTerminalDestinationReceiptInput,
  GetWorkflowTerminalDestinationReceiptResult,
  GetWorkflowTerminalParentContextInput,
  GetWorkflowTerminalParentContextResult,
  GetWorkflowTerminalContinuationPlanInput,
  GetWorkflowTerminalContinuationPlanResult,
  PersistWorkflowTerminalStateInput,
  PersistWorkflowTerminalStateResult,
  PrepareWorkflowTerminalEffectInput,
  PrepareWorkflowTerminalEffectResult,
  ReserveWorkflowTerminalDestinationReceiptInput,
  ReserveWorkflowTerminalDestinationReceiptResult,
  ApplyWorkflowTerminalParentEffectInput,
  ApplyWorkflowTerminalParentEffectResult,
  BindWorkflowNestedRunOwnershipInput,
  BindWorkflowNestedRunOwnershipResult,
  AdmitWorkflowNestedRunInput,
  AdmitWorkflowNestedRunResult,
  PersistWorkflowTerminalRecoveryAncestryInput,
  PersistWorkflowTerminalRecoveryAncestryResult,
  GetWorkflowTerminalRecoveryAncestryResult,
  ReleaseWorkflowTerminalizationInput,
  ReleaseWorkflowTerminalizationResult,
  UpdateWorkflowStateOptions,
  StorageListWorkflowRunsInput,
  WorkflowRun,
  WorkflowRuns,
  CreateIndexOptions,
  WorkflowTerminalContinuationPlanRecord,
  WorkflowTerminalizationCapabilities,
  WorkflowTerminalRecoveryAncestryRecord,
} from '@mastra/core/storage';
import type {
  StepResult,
  WorkflowRunState,
  WorkflowTerminalRecoveryEnvelopeV1,
  WorkflowTerminalEffectRecord,
  WorkflowTerminalDestinationReceiptRecord,
  WorkflowTerminalSnapshotRecord,
  WorkflowTerminalizationRecord,
} from '@mastra/core/workflows';
import {
  materializeWorkflowTerminalCanonicalJsonObject,
  materializeWorkflowTerminalRecoveryEnvelope,
  validateWorkflowTerminalRecoveryEnvelopeIntegrity,
  validateWorkflowTerminalRecoveryParentFrameGraphBinding,
} from '@mastra/core/workflows';
import type { TxClient } from '../../client';
import { PgDB, resolvePgConfig, generateIndexSQL, generateTableSQL } from '../../db';
import type { PgDomainConfig } from '../../db';

class CorruptWorkflowTerminalSnapshotRecordError extends TypeError {
  constructor() {
    super('Invalid workflow terminal snapshot record');
    this.name = 'CorruptWorkflowTerminalSnapshotRecordError';
  }
}

const TABLE_WORKFLOW_TERMINALIZATIONS = 'mastra_workflow_terminalizations';
// PF-1782 defines one canonical v2 namespace for its linked retained evidence.
const TABLE_WORKFLOW_TERMINAL_EFFECTS = 'mastra_workflow_terminal_effects_v2';
const TABLE_WORKFLOW_TERMINAL_SNAPSHOTS = 'mastra_workflow_terminal_snapshots_v2';
const TABLE_WORKFLOW_TERMINAL_RECOVERY_ANCESTRIES = 'mastra_workflow_terminal_recovery_ancestries';
const TABLE_WORKFLOW_TERMINAL_DESTINATION_RECEIPTS = 'mastra_workflow_terminal_destination_receipts_v2';
const TABLE_WORKFLOW_TERMINAL_CONTINUATION_PLANS = 'mastra_workflow_terminal_continuation_plans_v2';
const TABLE_WORKFLOW_PARENT_REVISIONS = 'mastra_workflow_parent_revisions';
const TERMINAL_WORKFLOW_RUN_STATUSES = ['success', 'failed', 'canceled', 'tripwire', 'bailed'] as const;
type TerminalWorkflowRunStatus = (typeof TERMINAL_WORKFLOW_RUN_STATUSES)[number];

function isTerminalWorkflowRunStatus(value: unknown): value is TerminalWorkflowRunStatus {
  return typeof value === 'string' && TERMINAL_WORKFLOW_RUN_STATUSES.includes(value as TerminalWorkflowRunStatus);
}

function getSchemaName(schema?: string) {
  return schema ? `"${schema}"` : '"public"';
}

function getTableName({ indexName, schemaName }: { indexName: string; schemaName?: string }) {
  const quotedIndexName = `"${indexName}"`;
  return schemaName ? `${schemaName}.${quotedIndexName}` : quotedIndexName;
}

/**
 * Sanitizes JSON string for PostgreSQL jsonb:
 * - Removes problematic Unicode sequences:
 *   - \u0000 (null character) - causes error 22P05 "unsupported Unicode escape sequence"
 *   - \uD800-\uDFFF (unpaired surrogates) - causes "Unicode low surrogate must follow a high surrogate"
 *   - \\uD800 (escaped-backslash + surrogate, e.g. from JS regex literals like [^\ud800-\udfff]):
 *     removing just \uXXXX would leave a dangling backslash that creates a new invalid escape (e.g. \-)
 * - Escapes any remaining invalid JSON escape sequences (e.g. \v, \k, \-)
 */
export function sanitizeJsonForPg(jsonString: string): string {
  return (
    jsonString
      // Remove null char and surrogate escape sequences. The optional extra backslash (\\\\?)
      // also handles the escaped-backslash variant (\\uXXXX), which would otherwise leave a
      // dangling backslash and produce a new invalid escape sequence after removal.
      .replace(/\\\\?u(0000|[Dd][89A-Fa-f][0-9A-Fa-f]{2})/g, '')
      // Fix any remaining invalid JSON escape sequences safely without rewriting
      // already-escaped backslashes. Running this AFTER surrogate removal ensures that
      // characters newly exposed by the removal (e.g. a hyphen left after \\ud800-\\udfff)
      // are also caught and escaped.
      .replace(/(^|[^\\])(\\(?!["\\/bfnrtu]))/g, '$1\\\\')
  );
}

export class WorkflowsPG extends WorkflowsStorage {
  #db: PgDB;
  #schema: string;
  #skipDefaultIndexes?: boolean;
  #indexes?: CreateIndexOptions[];

  /** Tables managed by this domain */
  static readonly MANAGED_TABLES = [
    TABLE_WORKFLOW_SNAPSHOT,
    TABLE_WORKFLOW_TERMINALIZATIONS,
    TABLE_WORKFLOW_TERMINAL_EFFECTS,
    TABLE_WORKFLOW_TERMINAL_SNAPSHOTS,
    TABLE_WORKFLOW_TERMINAL_RECOVERY_ANCESTRIES,
    TABLE_WORKFLOW_TERMINAL_DESTINATION_RECEIPTS,
    TABLE_WORKFLOW_TERMINAL_CONTINUATION_PLANS,
    TABLE_WORKFLOW_PARENT_REVISIONS,
  ] as const;

  constructor(config: PgDomainConfig) {
    super();
    const { client, schemaName, skipDefaultIndexes, indexes } = resolvePgConfig(config);
    this.#db = new PgDB({ client, schemaName, skipDefaultIndexes });
    this.#schema = schemaName || 'public';
    this.#skipDefaultIndexes = skipDefaultIndexes;
    // Filter indexes to only those for tables managed by this domain
    this.#indexes = indexes?.filter(idx => (WorkflowsPG.MANAGED_TABLES as readonly string[]).includes(idx.table));
  }

  supportsConcurrentUpdates(): boolean {
    return true;
  }

  supportsWorkflowTerminalizationJournal(): boolean {
    return true;
  }

  getWorkflowTerminalizationCapabilities(): WorkflowTerminalizationCapabilities {
    return {
      journalVersion: 1,
      producerOutboxVersion: 1,
      destinationReceiptVersion: 1,
      parentApplicationVersion: 1,
      recoveryVersion: 1,
    };
  }

  private terminalizationTableName(): string {
    return getTableName({
      indexName: TABLE_WORKFLOW_TERMINALIZATIONS,
      schemaName: getSchemaName(this.#schema),
    });
  }

  private workflowSnapshotTableName(): string {
    return getTableName({ indexName: TABLE_WORKFLOW_SNAPSHOT, schemaName: getSchemaName(this.#schema) });
  }

  private terminalEffectTableName(): string {
    return getTableName({
      indexName: TABLE_WORKFLOW_TERMINAL_EFFECTS,
      schemaName: getSchemaName(this.#schema),
    });
  }

  private terminalSnapshotTableName(): string {
    return getTableName({
      indexName: TABLE_WORKFLOW_TERMINAL_SNAPSHOTS,
      schemaName: getSchemaName(this.#schema),
    });
  }

  private terminalRecoveryAncestryTableName(): string {
    return getTableName({
      indexName: TABLE_WORKFLOW_TERMINAL_RECOVERY_ANCESTRIES,
      schemaName: getSchemaName(this.#schema),
    });
  }

  private terminalDestinationReceiptTableName(): string {
    return getTableName({
      indexName: TABLE_WORKFLOW_TERMINAL_DESTINATION_RECEIPTS,
      schemaName: getSchemaName(this.#schema),
    });
  }

  private terminalContinuationPlanTableName(): string {
    return getTableName({
      indexName: TABLE_WORKFLOW_TERMINAL_CONTINUATION_PLANS,
      schemaName: getSchemaName(this.#schema),
    });
  }

  private workflowParentRevisionTableName(): string {
    return getTableName({
      indexName: TABLE_WORKFLOW_PARENT_REVISIONS,
      schemaName: getSchemaName(this.#schema),
    });
  }

  private async lockWorkflowParentRevisionWithPresence(
    t: TxClient,
    workflowName: string,
    runId: string,
  ): Promise<{ generation: number; created: boolean; terminalStatus: TerminalWorkflowRunStatus | null }> {
    const inserted = await t.oneOrNone<{ generation: number | string }>(
      `INSERT INTO ${this.workflowParentRevisionTableName()}
       (workflow_name, run_id, generation, updated_at)
       VALUES ($1, $2, 0, floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint)
       ON CONFLICT (workflow_name, run_id) DO NOTHING
       RETURNING generation`,
      [workflowName, runId],
    );
    const row = await t.one<{ generation: number | string; terminal_status: string | null }>(
      `SELECT generation, terminal_status FROM ${this.workflowParentRevisionTableName()}
       WHERE workflow_name = $1 AND run_id = $2
       FOR UPDATE`,
      [workflowName, runId],
    );
    const generation = typeof row.generation === 'string' ? Number(row.generation) : row.generation;
    if (!Number.isSafeInteger(generation) || generation < 0) {
      throw new TypeError('Invalid workflow parent revision generation');
    }
    if (row.terminal_status !== null && !isTerminalWorkflowRunStatus(row.terminal_status)) {
      throw new TypeError('Invalid workflow parent terminal status');
    }
    return { generation, created: inserted !== null, terminalStatus: row.terminal_status };
  }

  private async lockWorkflowParentRevision(t: TxClient, workflowName: string, runId: string): Promise<number> {
    return (await this.lockWorkflowParentRevisionWithPresence(t, workflowName, runId)).generation;
  }

  private async bumpWorkflowParentRevision(
    t: TxClient,
    workflowName: string,
    runId: string,
    generation: number,
  ): Promise<number> {
    const next = generation + 1;
    if (!Number.isSafeInteger(next)) throw new TypeError('Workflow parent revision exhausted');
    const result = await t.query(
      `UPDATE ${this.workflowParentRevisionTableName()} AS revision
       SET generation = $1,
           terminal_status = COALESCE(
             revision.terminal_status,
             (
               SELECT CASE
                 WHEN snapshot.snapshot->>'status' IN ('success', 'failed', 'canceled', 'tripwire', 'bailed')
                   THEN snapshot.snapshot->>'status'
                 ELSE NULL
               END
               FROM ${this.workflowSnapshotTableName()} AS snapshot
               WHERE snapshot.workflow_name = $2 AND snapshot.run_id = $3
             )
           ),
           updated_at = floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint
       WHERE workflow_name = $2 AND run_id = $3 AND generation = $4`,
      [next, workflowName, runId, generation],
    );
    if ((result.rowCount ?? 0) !== 1) throw new TypeError('Workflow parent revision conflict');
    return next;
  }

  private async latchWorkflowParentTerminalStatus(
    t: TxClient,
    workflowName: string,
    runId: string,
    terminalStatus: TerminalWorkflowRunStatus,
  ): Promise<number> {
    const result = await t.oneOrNone<{ generation: number | string; terminal_status: string }>(
      `UPDATE ${this.workflowParentRevisionTableName()}
       SET generation = CASE WHEN terminal_status IS NULL THEN generation + 1 ELSE generation END,
           terminal_status = COALESCE(terminal_status, $1),
           updated_at = floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint
       WHERE workflow_name = $2 AND run_id = $3
       RETURNING generation, terminal_status`,
      [terminalStatus, workflowName, runId],
    );
    if (!result) throw new TypeError('Workflow parent terminal marker is unavailable');
    const generation = typeof result.generation === 'string' ? Number(result.generation) : result.generation;
    if (!Number.isSafeInteger(generation) || generation < 1) {
      throw new TypeError('Workflow parent revision exhausted');
    }
    if (result.terminal_status !== terminalStatus) {
      throw new TypeError('Workflow parent terminal marker conflicts with authoritative terminal status');
    }
    return generation;
  }

  private decodeTerminalizationRow(row: Record<string, unknown>, now: number): WorkflowTerminalizationRecord {
    const toSafeInteger = (value: unknown, field: string): number => {
      const parsed = typeof value === 'string' ? Number(value) : value;
      if (typeof parsed !== 'number' || !Number.isSafeInteger(parsed) || parsed < 0) {
        throw new TypeError(`Invalid workflow terminalization ${field}`);
      }
      return parsed;
    };
    const record = {
      version: toSafeInteger(row.version, 'version'),
      eventKey: row.event_key,
      terminalStatus: row.terminal_status,
      phase: row.phase,
      ownerId: row.owner_id ?? undefined,
      claimToken: row.claim_token ?? undefined,
      claimGeneration: toSafeInteger(row.claim_generation, 'claim_generation'),
      leaseExpiresAt:
        row.lease_expires_at === null || row.lease_expires_at === undefined
          ? undefined
          : toSafeInteger(row.lease_expires_at, 'lease_expires_at'),
      createdAt: toSafeInteger(row.created_at, 'created_at'),
      updatedAt: toSafeInteger(row.updated_at, 'updated_at'),
      completedAt:
        row.completed_at === null || row.completed_at === undefined
          ? undefined
          : toSafeInteger(row.completed_at, 'completed_at'),
    } as WorkflowTerminalizationRecord;

    const phases = new Set([
      'terminalization_pending',
      'run_state_persisted',
      'parent_outbox_pending',
      'parent_effect_recorded',
      'finish_outbox_pending',
      'finish_effect_recorded',
      'complete',
    ]);
    const validOptionalIdentity = (value: unknown, maxLength: number) =>
      value === undefined || (typeof value === 'string' && value.length > 0 && value.length <= maxLength);
    if (
      record.version !== 1 ||
      typeof record.eventKey !== 'string' ||
      record.eventKey.length === 0 ||
      record.eventKey.length > 1024 ||
      !['success', 'failed', 'canceled'].includes(record.terminalStatus) ||
      !phases.has(record.phase) ||
      record.claimGeneration <= 0 ||
      !validOptionalIdentity(record.ownerId, 256) ||
      !validOptionalIdentity(record.claimToken, 256) ||
      record.createdAt > now ||
      record.updatedAt > now ||
      record.createdAt > record.updatedAt ||
      (record.phase === 'complete' &&
        (record.ownerId !== undefined ||
          record.claimToken !== undefined ||
          record.leaseExpiresAt !== undefined ||
          record.completedAt === undefined ||
          record.completedAt !== record.updatedAt)) ||
      (record.phase !== 'complete' &&
        (record.completedAt !== undefined ||
          (record.ownerId === undefined) !== (record.claimToken === undefined) ||
          (record.claimToken === undefined) !== (record.leaseExpiresAt === undefined) ||
          (record.leaseExpiresAt !== undefined &&
            (record.leaseExpiresAt <= record.updatedAt ||
              record.leaseExpiresAt - record.updatedAt > MAX_WORKFLOW_TERMINALIZATION_LEASE_MS))))
    ) {
      throw new TypeError('Invalid workflow terminalization record');
    }
    return record;
  }

  private decodeTerminalEffectRow(row: Record<string, unknown>, now: number): WorkflowTerminalEffectRecord {
    const toSafeInteger = (value: unknown, field: string): number => {
      const parsed = typeof value === 'string' ? Number(value) : value;
      if (typeof parsed !== 'number' || !Number.isSafeInteger(parsed) || parsed < 0) {
        throw new TypeError(`Invalid workflow terminal effect ${field}`);
      }
      return parsed;
    };
    const resourceId = row.resource_id === null || row.resource_id === undefined ? undefined : row.resource_id;
    const common = {
      version: toSafeInteger(row.version, 'version'),
      effectKey: row.effect_key,
      kind: row.effect_kind,
      workflowName: row.workflow_name,
      runId: row.run_id,
      sourceEventKey: row.source_event_key,
      terminalStatus: row.terminal_status,
      recoveryEnvelopeHash: row.recovery_envelope_hash,
      retainedRecordHash: row.retained_record_hash,
      ...(resourceId === undefined ? {} : { resourceId }),
      payloadHash: row.payload_hash,
      createdAt: toSafeInteger(row.created_at, 'created_at'),
    };
    const validIdentity = (value: unknown, maxLength: number) =>
      typeof value === 'string' && value.length > 0 && value.length <= maxLength;
    const parentFields = [row.parent_workflow_name, row.parent_run_id, row.parent_step_id];
    const parentExecutionPath = row.parent_execution_path;
    if (
      common.version !== 1 ||
      !['parent-workflow-step-end', 'workflow-finish'].includes(common.kind as string) ||
      !validIdentity(common.workflowName, 512) ||
      !validIdentity(common.runId, 512) ||
      !validIdentity(common.sourceEventKey, 1024) ||
      !['success', 'failed', 'canceled'].includes(common.terminalStatus as string) ||
      typeof common.effectKey !== 'string' ||
      !/^wte:v1:[a-f0-9]{64}$/.test(common.effectKey) ||
      typeof common.payloadHash !== 'string' ||
      !/^sha256:[a-f0-9]{64}$/.test(common.payloadHash) ||
      typeof common.recoveryEnvelopeHash !== 'string' ||
      !/^sha256:[a-f0-9]{64}$/.test(common.recoveryEnvelopeHash) ||
      typeof common.retainedRecordHash !== 'string' ||
      !/^sha256:[a-f0-9]{64}$/.test(common.retainedRecordHash) ||
      (resourceId !== undefined && !validIdentity(resourceId, 512)) ||
      common.createdAt > now ||
      (common.kind === 'parent-workflow-step-end' && !parentFields.every(value => validIdentity(value, 512))) ||
      (common.kind === 'parent-workflow-step-end' &&
        (!Array.isArray(parentExecutionPath) ||
          parentExecutionPath.length === 0 ||
          parentExecutionPath.length > MAX_WORKFLOW_TERMINAL_PARENT_EXECUTION_PATH_LENGTH ||
          !parentExecutionPath.every(value => Number.isSafeInteger(value) && value >= 0))) ||
      (common.kind === 'workflow-finish' &&
        (!parentFields.every(value => value === null || value === undefined) ||
          (parentExecutionPath !== null && parentExecutionPath !== undefined)))
    ) {
      throw new TypeError('Invalid workflow terminal effect record');
    }
    const effect =
      common.kind === 'parent-workflow-step-end'
        ? ({
            ...common,
            version: 1,
            kind: common.kind,
            terminalStatus: common.terminalStatus,
            parentWorkflowName: row.parent_workflow_name,
            parentRunId: row.parent_run_id,
            parentStepId: row.parent_step_id,
            parentExecutionPath: [...(parentExecutionPath as number[])],
          } as WorkflowTerminalEffectRecord)
        : ({
            ...common,
            version: 1,
            kind: common.kind,
            terminalStatus: common.terminalStatus,
          } as WorkflowTerminalEffectRecord);
    validateWorkflowTerminalEffectIntegrity(effect);
    return effect;
  }

  private async getTerminalizationContext(
    t: TxClient,
    workflowName: string,
    runId: string,
  ): Promise<{ record?: WorkflowTerminalizationRecord; snapshotExists: boolean; now: number }> {
    await t.none(`SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, [JSON.stringify([workflowName, runId])]);
    const row = await t.oneOrNone<Record<string, unknown>>(
      `SELECT * FROM ${this.terminalizationTableName()}
       WHERE workflow_name = $1 AND run_id = $2 FOR UPDATE`,
      [workflowName, runId],
    );
    const snapshot = await t.oneOrNone<{ exists: boolean }>(
      `SELECT TRUE AS exists FROM ${this.workflowSnapshotTableName()}
       WHERE workflow_name = $1 AND run_id = $2`,
      [workflowName, runId],
    );
    const time = await t.one<{ now_ms: string }>(
      `SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint AS now_ms`,
    );
    const now = Number(time.now_ms);
    if (!Number.isSafeInteger(now)) throw new TypeError('Invalid PostgreSQL terminalization clock');
    return {
      record: row ? this.decodeTerminalizationRow(row, now) : undefined,
      snapshotExists: Boolean(snapshot),
      now,
    };
  }

  private async getTerminalizationObservation(
    workflowName: string,
    runId: string,
  ): Promise<GetWorkflowTerminalizationResult> {
    const row = await this.#db.client.one<Record<string, unknown>>(
      `SELECT terminalization.*,
              terminalization.run_id IS NOT NULL AS terminalization_exists,
              snapshot.run_id IS NOT NULL AS snapshot_exists,
              floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint AS observation_now_ms
       FROM (SELECT $1::text AS workflow_name, $2::text AS run_id) AS identity
       LEFT JOIN ${this.terminalizationTableName()} AS terminalization
         ON terminalization.workflow_name = identity.workflow_name AND terminalization.run_id = identity.run_id
       LEFT JOIN ${this.workflowSnapshotTableName()} AS snapshot
         ON snapshot.workflow_name = identity.workflow_name AND snapshot.run_id = identity.run_id`,
      [workflowName, runId],
    );
    if (row.terminalization_exists) {
      const now = Number(row.observation_now_ms);
      if (!Number.isSafeInteger(now)) throw new TypeError('Invalid PostgreSQL terminalization clock');
      return {
        status: 'found',
        record: observeWorkflowTerminalizationRecord(this.decodeTerminalizationRow(row, now)),
      };
    }
    return row.snapshot_exists ? { status: 'missing_record' } : { status: 'missing_run' };
  }

  private async saveTerminalizationRecord(
    t: TxClient,
    workflowName: string,
    runId: string,
    record: WorkflowTerminalizationRecord,
  ): Promise<void> {
    await t.none(
      `INSERT INTO ${this.terminalizationTableName()}
       (workflow_name, run_id, version, event_key, terminal_status, phase, owner_id, claim_token,
        claim_generation, lease_expires_at, created_at, updated_at, completed_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
       ON CONFLICT (workflow_name, run_id) DO UPDATE SET
         version = EXCLUDED.version,
         event_key = EXCLUDED.event_key,
         terminal_status = EXCLUDED.terminal_status,
         phase = EXCLUDED.phase,
         owner_id = EXCLUDED.owner_id,
         claim_token = EXCLUDED.claim_token,
         claim_generation = EXCLUDED.claim_generation,
         lease_expires_at = EXCLUDED.lease_expires_at,
         updated_at = EXCLUDED.updated_at,
         completed_at = EXCLUDED.completed_at`,
      [
        workflowName,
        runId,
        record.version,
        record.eventKey,
        record.terminalStatus,
        record.phase,
        record.ownerId ?? null,
        record.claimToken ?? null,
        record.claimGeneration,
        record.leaseExpiresAt ?? null,
        record.createdAt,
        record.updatedAt,
        record.completedAt ?? null,
      ],
    );
  }

  private async getTerminalEffectRecord(
    t: TxClient,
    workflowName: string,
    runId: string,
    kind: string,
    now: number,
  ): Promise<WorkflowTerminalEffectRecord | undefined> {
    const row = await t.oneOrNone<Record<string, unknown>>(
      `SELECT * FROM ${this.terminalEffectTableName()}
       WHERE workflow_name = $1 AND run_id = $2 AND effect_kind = $3
       FOR UPDATE`,
      [workflowName, runId, kind],
    );
    return row ? this.decodeTerminalEffectRow(row, now) : undefined;
  }

  private async insertTerminalEffectRecord(t: TxClient, effect: WorkflowTerminalEffectRecord): Promise<void> {
    await t.none(
      `INSERT INTO ${this.terminalEffectTableName()}
       (workflow_name, run_id, effect_kind, version, effect_key, source_event_key, terminal_status,
       parent_workflow_name, parent_run_id, parent_step_id, parent_execution_path, recovery_envelope_hash,
        retained_record_hash, resource_id, payload_hash, created_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)`,
      [
        effect.workflowName,
        effect.runId,
        effect.kind,
        effect.version,
        effect.effectKey,
        effect.sourceEventKey,
        effect.terminalStatus,
        effect.kind === 'parent-workflow-step-end' ? effect.parentWorkflowName : null,
        effect.kind === 'parent-workflow-step-end' ? effect.parentRunId : null,
        effect.kind === 'parent-workflow-step-end' ? effect.parentStepId : null,
        effect.kind === 'parent-workflow-step-end' ? JSON.stringify(effect.parentExecutionPath) : null,
        effect.recoveryEnvelopeHash,
        effect.retainedRecordHash,
        effect.resourceId ?? null,
        effect.payloadHash,
        effect.createdAt,
      ],
    );
  }

  private decodeTerminalSnapshotRow(row: Record<string, unknown>, now: number): WorkflowTerminalSnapshotRecord {
    const version = typeof row.version === 'string' ? Number(row.version) : row.version;
    const createdAt = typeof row.created_at === 'string' ? Number(row.created_at) : row.created_at;
    const resourceId = row.resource_id === null || row.resource_id === undefined ? undefined : row.resource_id;
    const recordHash = row.record_hash;
    let envelope = row.envelope;
    if (typeof envelope === 'string') envelope = JSON.parse(envelope);
    if (
      version !== 1 ||
      typeof row.workflow_name !== 'string' ||
      row.workflow_name.length === 0 ||
      row.workflow_name.length > 512 ||
      typeof row.run_id !== 'string' ||
      row.run_id.length === 0 ||
      row.run_id.length > 512 ||
      (resourceId !== undefined && typeof resourceId !== 'string') ||
      !['success', 'failed', 'canceled'].includes(row.terminal_status as string) ||
      typeof createdAt !== 'number' ||
      !Number.isSafeInteger(createdAt) ||
      createdAt < 0 ||
      createdAt > now ||
      typeof row.envelope_hash !== 'string' ||
      !/^sha256:[a-f0-9]{64}$/.test(row.envelope_hash) ||
      typeof recordHash !== 'string' ||
      !/^sha256:[a-f0-9]{64}$/.test(recordHash) ||
      !envelope ||
      typeof envelope !== 'object' ||
      Array.isArray(envelope)
    ) {
      throw new TypeError('Invalid workflow terminal snapshot record');
    }
    const retained = {
      version: 1,
      workflowName: row.workflow_name,
      runId: row.run_id,
      ...(resourceId === undefined ? {} : { resourceId }),
      terminalStatus: row.terminal_status as WorkflowTerminalSnapshotRecord['terminalStatus'],
      envelopeHash: row.envelope_hash,
      recordHash,
      envelope,
      createdAt,
    } as WorkflowTerminalSnapshotRecord;
    validateWorkflowTerminalRecoveryEnvelopeIntegrity(
      { version: 1, envelopeHash: retained.envelopeHash, envelope: retained.envelope },
      {
        workflowName: retained.workflowName,
        runId: retained.runId,
        terminalStatus: retained.terminalStatus,
      },
    );
    validateWorkflowTerminalSnapshotRecordIntegrity(retained);
    return retained;
  }

  private decodeTerminalRecoveryAncestryRow(
    row: Record<string, unknown>,
    now: number,
  ): WorkflowTerminalRecoveryAncestryRecord {
    const version = typeof row.version === 'string' ? Number(row.version) : row.version;
    const createdAt = typeof row.created_at === 'string' ? Number(row.created_at) : row.created_at;
    let ancestry = row.ancestry;
    if (typeof ancestry === 'string') ancestry = JSON.parse(ancestry);
    const record = {
      version,
      workflowName: row.workflow_name,
      runId: row.run_id,
      ancestryHash: row.ancestry_hash,
      ancestry,
      createdAt,
    } as WorkflowTerminalRecoveryAncestryRecord;
    if (version !== 1 || typeof createdAt !== 'number' || !Number.isSafeInteger(createdAt) || createdAt > now) {
      throw new TypeError('Invalid workflow terminal recovery ancestry record');
    }
    validateWorkflowTerminalRecoveryAncestryRecord(record, {
      workflowName: String(row.workflow_name),
      runId: String(row.run_id),
      now,
    });
    const immediate = record.ancestry[0];
    if (
      row.immediate_parent_workflow_name !== (immediate?.parentWorkflowName ?? null) ||
      row.immediate_parent_run_id !== (immediate?.parentRunId ?? null)
    ) {
      throw new TypeError('Invalid workflow terminal recovery ancestry parent binding');
    }
    return record;
  }

  private async getTerminalSnapshotRecord(
    t: TxClient,
    workflowName: string,
    runId: string,
    now: number,
  ): Promise<WorkflowTerminalSnapshotRecord | undefined> {
    const row = await t.oneOrNone<Record<string, unknown>>(
      `SELECT * FROM ${this.terminalSnapshotTableName()}
       WHERE workflow_name = $1 AND run_id = $2 FOR UPDATE`,
      [workflowName, runId],
    );
    if (!row) return undefined;
    try {
      return this.decodeTerminalSnapshotRow(row, now);
    } catch {
      throw new CorruptWorkflowTerminalSnapshotRecordError();
    }
  }

  private decodeTerminalDestinationReceiptRow(
    row: Record<string, unknown>,
    effect: WorkflowTerminalEffectRecord,
    now: number,
  ): WorkflowTerminalDestinationReceiptRecord {
    const integer = (value: unknown, field: string, optional = false): number | undefined => {
      if (optional && (value === null || value === undefined)) return undefined;
      const parsed = typeof value === 'string' ? Number(value) : value;
      if (typeof parsed !== 'number' || !Number.isSafeInteger(parsed) || parsed < 0 || parsed > now) {
        throw new TypeError(`Invalid workflow terminal destination receipt ${field}`);
      }
      return parsed;
    };
    const appliedAt = integer(row.applied_at, 'applied_at', true);
    const dispatchPendingAt = integer(row.dispatch_pending_at, 'dispatch_pending_at', true);
    const destinationAppliedAt = integer(row.destination_applied_at, 'destination_applied_at', true);
    const quarantinedAt = integer(row.quarantined_at, 'quarantined_at', true);
    const record = {
      version: integer(row.version, 'version'),
      receiptKey: row.receipt_key,
      workflowName: row.workflow_name,
      runId: row.run_id,
      effectKey: row.effect_key,
      consumerId: row.consumer_id,
      effectKind: row.effect_kind,
      producerPayloadHash: row.producer_payload_hash,
      destinationHash: row.destination_hash,
      applicationState: row.application_state,
      dispatchState: row.dispatch_state,
      createdAt: integer(row.created_at, 'created_at'),
      updatedAt: integer(row.updated_at, 'updated_at'),
      ...(appliedAt === undefined ? {} : { appliedAt }),
      ...(dispatchPendingAt === undefined ? {} : { dispatchPendingAt }),
      ...(destinationAppliedAt === undefined ? {} : { destinationAppliedAt }),
      ...(quarantinedAt === undefined ? {} : { quarantinedAt }),
    } as WorkflowTerminalDestinationReceiptRecord;
    if (
      record.version !== 1 ||
      typeof record.receiptKey !== 'string' ||
      !/^wtr:v1:[a-f0-9]{64}$/.test(record.receiptKey) ||
      typeof record.consumerId !== 'string' ||
      typeof record.destinationHash !== 'string' ||
      !/^sha256:[a-f0-9]{64}$/.test(record.destinationHash)
    ) {
      throw new TypeError('Invalid workflow terminal destination receipt record');
    }
    validateWorkflowTerminalizationIdentity(record.consumerId, 'consumerId', 256);
    validateWorkflowTerminalDestinationReceiptIntegrity(record, effect, now);
    return record;
  }

  private async getTerminalDestinationReceiptRecord(
    t: TxClient,
    effect: WorkflowTerminalEffectRecord,
    consumerId: string,
    now: number,
  ): Promise<WorkflowTerminalDestinationReceiptRecord | undefined> {
    const expectedReceiptKey = createWorkflowTerminalDestinationReceiptRecord(effect, consumerId, now).receiptKey;
    const rows = await t.manyOrNone<Record<string, unknown>>(
      `SELECT * FROM ${this.terminalDestinationReceiptTableName()}
       WHERE receipt_key = $1
          OR (effect_key = $2 AND consumer_id = $3)
          OR (workflow_name = $4 AND run_id = $5 AND effect_kind = $6 AND consumer_id = $3)
       FOR UPDATE`,
      [expectedReceiptKey, effect.effectKey, consumerId, effect.workflowName, effect.runId, effect.kind],
    );
    if (rows.length > 1) {
      throw new TypeError('Conflicting workflow terminal destination receipt storage');
    }
    return rows[0] ? this.decodeTerminalDestinationReceiptRow(rows[0], effect, now) : undefined;
  }

  private async insertTerminalDestinationReceiptRecord(
    t: TxClient,
    receipt: WorkflowTerminalDestinationReceiptRecord,
  ): Promise<void> {
    await t.none(
      `INSERT INTO ${this.terminalDestinationReceiptTableName()}
       (version, workflow_name, run_id, effect_key, consumer_id, receipt_key, effect_kind,
        producer_payload_hash, destination_hash, application_state, dispatch_state, created_at, updated_at,
        applied_at, dispatch_pending_at, destination_applied_at, quarantined_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)`,
      [
        receipt.version,
        receipt.workflowName,
        receipt.runId,
        receipt.effectKey,
        receipt.consumerId,
        receipt.receiptKey,
        receipt.effectKind,
        receipt.producerPayloadHash,
        receipt.destinationHash,
        receipt.applicationState,
        receipt.dispatchState,
        receipt.createdAt,
        receipt.updatedAt,
        receipt.appliedAt ?? null,
        receipt.dispatchPendingAt ?? null,
        receipt.destinationAppliedAt ?? null,
        receipt.quarantinedAt ?? null,
      ],
    );
  }

  private async countTerminalDestinationReceiptRecords(
    t: TxClient,
    effect: WorkflowTerminalEffectRecord,
  ): Promise<number> {
    const row = await t.one<{ count: string }>(
      `SELECT count(*)::text AS count
       FROM ${this.terminalDestinationReceiptTableName()}
       WHERE effect_key = $1
          OR (workflow_name = $2 AND run_id = $3 AND effect_kind = $4)`,
      [effect.effectKey, effect.workflowName, effect.runId, effect.kind],
    );
    const count = Number(row.count);
    if (!Number.isSafeInteger(count) || count < 0) {
      throw new TypeError('Invalid workflow terminal destination receipt count');
    }
    return count;
  }

  private decodeTerminalContinuationPlanRow(
    row: Record<string, unknown>,
    effect: WorkflowTerminalEffectRecord,
    receipt: WorkflowTerminalDestinationReceiptRecord,
    now: number,
  ): WorkflowTerminalContinuationPlanRecord {
    const createdAt = typeof row.created_at === 'string' ? Number(row.created_at) : row.created_at;
    if (row.version !== 1 && row.version !== '1') {
      throw new TypeError('Invalid workflow terminal continuation plan version');
    }
    if (typeof createdAt !== 'number' || !Number.isSafeInteger(createdAt) || createdAt < 0 || createdAt > now) {
      throw new TypeError('Invalid workflow terminal continuation plan created_at');
    }
    const contract = typeof row.contract === 'string' ? JSON.parse(row.contract) : row.contract;
    const frameworkActionKey =
      row.framework_action_key === null || row.framework_action_key === undefined
        ? undefined
        : row.framework_action_key;
    const record = {
      version: 1 as const,
      planKey: row.plan_key,
      planHash: row.plan_hash,
      receiptKey: row.receipt_key,
      effectKey: row.effect_key,
      consumerId: row.consumer_id,
      workflowName: row.workflow_name,
      runId: row.run_id,
      parentWorkflowName: row.parent_workflow_name,
      parentRunId: row.parent_run_id,
      parentRevision: row.parent_revision,
      contract,
      ...(frameworkActionKey === undefined ? {} : { frameworkActionKey }),
      createdAt,
    } as WorkflowTerminalContinuationPlanRecord;
    if (
      typeof record.planKey !== 'string' ||
      !/^wtp:v1:[a-f0-9]{64}$/.test(record.planKey) ||
      typeof record.planHash !== 'string' ||
      !/^sha256:[a-f0-9]{64}$/.test(record.planHash) ||
      typeof record.parentRevision !== 'string' ||
      typeof record.contract !== 'object' ||
      record.contract === null ||
      typeof row.contract_hash !== 'string' ||
      row.contract_hash !== record.contract.contractHash ||
      (frameworkActionKey !== undefined &&
        (typeof frameworkActionKey !== 'string' || !/^wta:v1:[a-f0-9]{64}$/.test(frameworkActionKey)))
    ) {
      throw new TypeError('Invalid workflow terminal continuation plan record');
    }
    validateWorkflowTerminalContinuationPlanIntegrity(record, effect, receipt, now);
    return record;
  }

  private async getTerminalContinuationPlanRecord(
    t: TxClient,
    effect: Extract<WorkflowTerminalEffectRecord, { kind: 'parent-workflow-step-end' }>,
    receipt: WorkflowTerminalDestinationReceiptRecord,
    now: number,
  ): Promise<WorkflowTerminalContinuationPlanRecord | undefined> {
    const rows = await t.manyOrNone<Record<string, unknown>>(
      `SELECT * FROM ${this.terminalContinuationPlanTableName()}
       WHERE receipt_key = $1
          OR (effect_key = $2 AND consumer_id = $5)
          OR (workflow_name = $3 AND run_id = $4 AND consumer_id = $5)
       FOR UPDATE`,
      [
        receipt.receiptKey,
        effect.effectKey,
        effect.workflowName,
        effect.runId,
        WORKFLOW_TERMINAL_PARENT_APPLICATION_CONSUMER_ID,
      ],
    );
    if (rows.length > 1) {
      throw new TypeError('Conflicting workflow terminal continuation plan storage');
    }
    return rows[0] ? this.decodeTerminalContinuationPlanRow(rows[0], effect, receipt, now) : undefined;
  }

  private async insertTerminalContinuationPlanRecord(
    t: TxClient,
    plan: WorkflowTerminalContinuationPlanRecord,
  ): Promise<void> {
    await t.none(
      `INSERT INTO ${this.terminalContinuationPlanTableName()}
       (version, plan_key, plan_hash, receipt_key, effect_key, consumer_id, workflow_name, run_id,
        parent_workflow_name, parent_run_id, parent_revision, contract_hash, contract,
        framework_action_key, created_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)`,
      [
        plan.version,
        plan.planKey,
        plan.planHash,
        plan.receiptKey,
        plan.effectKey,
        plan.consumerId,
        plan.workflowName,
        plan.runId,
        plan.parentWorkflowName,
        plan.parentRunId,
        plan.parentRevision,
        plan.contract.contractHash,
        sanitizeJsonForPg(JSON.stringify(plan.contract)),
        plan.frameworkActionKey ?? null,
        plan.createdAt,
      ],
    );
  }

  private async saveTerminalDestinationReceiptRecord(
    t: TxClient,
    receipt: WorkflowTerminalDestinationReceiptRecord,
    existing: boolean,
  ): Promise<void> {
    if (existing) {
      await t.none(
        `UPDATE ${this.terminalDestinationReceiptTableName()} SET
           application_state = $1,
           dispatch_state = $2,
           updated_at = $3,
           applied_at = $4,
           dispatch_pending_at = $5,
           destination_applied_at = $6,
           quarantined_at = $7
         WHERE receipt_key = $8`,
        [
          receipt.applicationState,
          receipt.dispatchState,
          receipt.updatedAt,
          receipt.appliedAt ?? null,
          receipt.dispatchPendingAt ?? null,
          receipt.destinationAppliedAt ?? null,
          receipt.quarantinedAt ?? null,
          receipt.receiptKey,
        ],
      );
      return;
    }
    await t.none(
      `INSERT INTO ${this.terminalDestinationReceiptTableName()}
       (version, workflow_name, run_id, effect_key, consumer_id, receipt_key, effect_kind,
        producer_payload_hash, destination_hash, application_state, dispatch_state, created_at, updated_at,
        applied_at, dispatch_pending_at, destination_applied_at, quarantined_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)`,
      [
        receipt.version,
        receipt.workflowName,
        receipt.runId,
        receipt.effectKey,
        receipt.consumerId,
        receipt.receiptKey,
        receipt.effectKind,
        receipt.producerPayloadHash,
        receipt.destinationHash,
        receipt.applicationState,
        receipt.dispatchState,
        receipt.createdAt,
        receipt.updatedAt,
        receipt.appliedAt ?? null,
        receipt.dispatchPendingAt ?? null,
        receipt.destinationAppliedAt ?? null,
        receipt.quarantinedAt ?? null,
      ],
    );
  }

  private async saveTerminalWorkflowSnapshot(
    t: TxClient,
    input: PersistWorkflowTerminalStateInput,
    snapshot: WorkflowRunState,
    recovery: { envelopeHash: string; envelope: unknown },
    now: number,
    parentRevision: number,
  ): Promise<void> {
    const timestamp = new Date(now);
    const sanitizedSnapshot = sanitizeJsonForPg(JSON.stringify(snapshot));
    const serializedEnvelope = JSON.stringify(recovery.envelope);
    const retainedRecordHash = getWorkflowTerminalSnapshotRecordHash({
      version: 1,
      workflowName: input.workflowName,
      runId: input.runId,
      ...(input.resourceId === undefined ? {} : { resourceId: input.resourceId }),
      terminalStatus: snapshot.status as WorkflowTerminalSnapshotRecord['terminalStatus'],
      envelopeHash: recovery.envelopeHash as `sha256:${string}`,
      createdAt: now,
    });
    const update = await t.query(
      `UPDATE ${this.workflowSnapshotTableName()}
       SET "resourceId" = $3, snapshot = $4, "updatedAt" = $5, "updatedAtZ" = $6
       WHERE workflow_name = $1 AND run_id = $2`,
      [input.workflowName, input.runId, input.resourceId, sanitizedSnapshot, timestamp, timestamp],
    );
    if ((update.rowCount ?? 0) !== 1) {
      throw new TypeError('Workflow terminal snapshot disappeared under lock');
    }
    await t.none(
      `INSERT INTO ${this.terminalSnapshotTableName()}
       (workflow_name, run_id, version, resource_id, terminal_status, envelope_hash, record_hash, envelope, created_at)
       VALUES ($1, $2, 1, $3, $4, $5, $6, $7, $8)`,
      [
        input.workflowName,
        input.runId,
        input.resourceId,
        snapshot.status,
        recovery.envelopeHash,
        retainedRecordHash,
        serializedEnvelope,
        now,
      ],
    );
    await this.bumpWorkflowParentRevision(t, input.workflowName, input.runId, parentRevision);
  }

  private terminalizationError(operation: string, workflowName: string, runId: string, error: unknown): never {
    if (error instanceof TypeError || error instanceof RangeError) throw error;
    throw new MastraError(
      {
        id: createStorageErrorId('PG', operation, 'FAILED'),
        domain: ErrorDomain.STORAGE,
        category: ErrorCategory.THIRD_PARTY,
        details: { workflowName, runId },
      },
      error,
    );
  }

  async persistWorkflowTerminalRecoveryAncestry(
    input: PersistWorkflowTerminalRecoveryAncestryInput,
  ): Promise<PersistWorkflowTerminalRecoveryAncestryResult> {
    const requested = createWorkflowTerminalRecoveryAncestryRecord(input.workflowName, input.runId, input.ancestry, 0);
    const operation = {
      workflowName: requested.workflowName,
      runId: requested.runId,
      ancestry: requested.ancestry,
    };
    try {
      return await this.#db.client.tx(async t => {
        const lockedIdentities = new Set([
          JSON.stringify([operation.workflowName, operation.runId]),
          ...operation.ancestry.map(frame => JSON.stringify([frame.parentWorkflowName, frame.parentRunId])),
        ]);
        for (const identity of [...lockedIdentities].sort()) {
          await t.none(`SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, [identity]);
        }
        const clock = await t.one<{ now_ms: string }>(
          `SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint AS now_ms`,
        );
        const now = Number(clock.now_ms);
        if (!Number.isSafeInteger(now) || now < 0) throw new TypeError('Invalid PostgreSQL recovery ancestry clock');
        const row = await t.oneOrNone<Record<string, unknown>>(
          `SELECT * FROM ${this.terminalRecoveryAncestryTableName()}
           WHERE workflow_name = $1 AND run_id = $2 FOR UPDATE`,
          [operation.workflowName, operation.runId],
        );
        const desired = createWorkflowTerminalRecoveryAncestryRecord(
          operation.workflowName,
          operation.runId,
          operation.ancestry,
          now,
        );
        if (row) {
          const existing = this.decodeTerminalRecoveryAncestryRow(row, now);
          return sameWorkflowTerminalRecoveryAncestry(existing, desired)
            ? { status: 'already_persisted', record: copyWorkflowTerminalRecoveryAncestryRecord(existing) }
            : { status: 'ancestry_conflict' };
        }
        for (const frame of desired.ancestry) {
          const parentRevision = await t.oneOrNone<{ terminal_status: string | null }>(
            `SELECT terminal_status FROM ${this.workflowParentRevisionTableName()}
             WHERE workflow_name = $1 AND run_id = $2
             FOR UPDATE`,
            [frame.parentWorkflowName, frame.parentRunId],
          );
          if (!parentRevision || parentRevision.terminal_status !== null) {
            throw new TypeError('Workflow terminal recovery ancestry parent evidence is unavailable');
          }
          const parentEvidence = await t.oneOrNone<{ snapshot: unknown }>(
            `SELECT snapshot FROM ${this.workflowSnapshotTableName()}
             WHERE workflow_name = $1 AND run_id = $2`,
            [frame.parentWorkflowName, frame.parentRunId],
          );
          let parentSnapshot = parentEvidence?.snapshot;
          if (typeof parentSnapshot === 'string') parentSnapshot = JSON.parse(parentSnapshot);
          const parentStatus =
            parentSnapshot && typeof parentSnapshot === 'object' && !Array.isArray(parentSnapshot)
              ? (parentSnapshot as Record<string, unknown>).status
              : undefined;
          if (parentStatus === undefined || isTerminalWorkflowRunStatus(parentStatus)) {
            throw new TypeError('Workflow terminal recovery ancestry parent evidence is unavailable');
          }
          if (!parentSnapshot || typeof parentSnapshot !== 'object' || Array.isArray(parentSnapshot)) {
            throw new TypeError('Workflow terminal recovery ancestry parent graph is unavailable');
          }
          validateWorkflowTerminalRecoveryParentFrameGraphBinding(
            frame,
            (parentSnapshot as WorkflowRunState).serializedStepGraph,
          );
        }
        const immediate = desired.ancestry[0];
        if (immediate) {
          const parentRecoveryRow = await t.oneOrNone<Record<string, unknown>>(
            `SELECT * FROM ${this.terminalRecoveryAncestryTableName()}
             WHERE workflow_name = $1 AND run_id = $2 FOR UPDATE`,
            [immediate.parentWorkflowName, immediate.parentRunId],
          );
          const parentRecovery = parentRecoveryRow
            ? this.decodeTerminalRecoveryAncestryRow(parentRecoveryRow, now)
            : undefined;
          const expectedTailHash = createWorkflowTerminalRecoveryAncestryRecord(
            immediate.parentWorkflowName,
            immediate.parentRunId,
            desired.ancestry.slice(1),
            now,
          ).ancestryHash;
          const retainedTailHash = createWorkflowTerminalRecoveryAncestryRecord(
            immediate.parentWorkflowName,
            immediate.parentRunId,
            parentRecovery?.ancestry ?? [],
            now,
          ).ancestryHash;
          if (expectedTailHash !== retainedTailHash) return { status: 'ancestry_conflict' };
        }
        await t.none(
          `INSERT INTO ${this.terminalRecoveryAncestryTableName()}
           (workflow_name, run_id, version, ancestry_hash, ancestry,
            immediate_parent_workflow_name, immediate_parent_run_id, created_at)
           VALUES ($1, $2, 1, $3, $4, $5, $6, $7)`,
          [
            operation.workflowName,
            operation.runId,
            desired.ancestryHash,
            JSON.stringify(desired.ancestry),
            desired.ancestry[0]?.parentWorkflowName ?? null,
            desired.ancestry[0]?.parentRunId ?? null,
            desired.createdAt,
          ],
        );
        return { status: 'persisted', record: copyWorkflowTerminalRecoveryAncestryRecord(desired) };
      });
    } catch (error) {
      return this.terminalizationError(
        'PERSIST_WORKFLOW_TERMINAL_RECOVERY_ANCESTRY',
        operation.workflowName,
        operation.runId,
        error,
      );
    }
  }

  async getWorkflowTerminalRecoveryAncestry(
    input: GetWorkflowTerminalizationInput,
  ): Promise<GetWorkflowTerminalRecoveryAncestryResult> {
    try {
      return await this.#db.client.tx(async t => {
        const clock = await t.one<{ now_ms: string }>(
          `SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint AS now_ms`,
        );
        const now = Number(clock.now_ms);
        if (!Number.isSafeInteger(now) || now < 0) throw new TypeError('Invalid PostgreSQL recovery ancestry clock');
        const row = await t.oneOrNone<Record<string, unknown>>(
          `SELECT * FROM ${this.terminalRecoveryAncestryTableName()}
           WHERE workflow_name = $1 AND run_id = $2`,
          [input.workflowName, input.runId],
        );
        return row
          ? { status: 'found', record: this.decodeTerminalRecoveryAncestryRow(row, now) }
          : { status: 'missing_ancestry' };
      });
    } catch (error) {
      return this.terminalizationError(
        'GET_WORKFLOW_TERMINAL_RECOVERY_ANCESTRY',
        input.workflowName,
        input.runId,
        error,
      );
    }
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
    try {
      return await this.#db.client.tx(async t => {
        const context = await this.getTerminalizationContext(t, operation.workflowName, operation.runId);
        if (!context.record && !context.snapshotExists) return { status: 'missing_run' };
        const result = claimWorkflowTerminalizationRecord(context.record, operation, context.now, randomUUID());
        if (result.status === 'acquired' || result.status === 'renewed') {
          const revisionLock = await this.lockWorkflowParentRevisionWithPresence(
            t,
            operation.workflowName,
            operation.runId,
          );
          if (!context.record) {
            const snapshot = await t.oneOrNone<{ exists: boolean }>(
              `SELECT TRUE AS exists FROM ${this.workflowSnapshotTableName()}
               WHERE workflow_name = $1 AND run_id = $2 FOR UPDATE`,
              [operation.workflowName, operation.runId],
            );
            if (!snapshot) {
              if (revisionLock.created) {
                await t.none(
                  `DELETE FROM ${this.workflowParentRevisionTableName()}
                   WHERE workflow_name = $1 AND run_id = $2`,
                  [operation.workflowName, operation.runId],
                );
              }
              return { status: 'missing_run' };
            }
          }
          await this.latchWorkflowParentTerminalStatus(
            t,
            operation.workflowName,
            operation.runId,
            result.record.terminalStatus,
          );
          await this.saveTerminalizationRecord(t, operation.workflowName, operation.runId, result.record);
          return result;
        }
        return 'record' in result
          ? { status: result.status, record: observeWorkflowTerminalizationRecord(result.record) }
          : result;
      });
    } catch (error) {
      return this.terminalizationError(
        'CLAIM_WORKFLOW_TERMINALIZATION',
        operation.workflowName,
        operation.runId,
        error,
      );
    }
  }

  async getWorkflowTerminalization(input: GetWorkflowTerminalizationInput): Promise<GetWorkflowTerminalizationResult> {
    const operation: GetWorkflowTerminalizationInput = {
      workflowName: input.workflowName,
      runId: input.runId,
    };
    try {
      return await this.getTerminalizationObservation(operation.workflowName, operation.runId);
    } catch (error) {
      return this.terminalizationError('GET_WORKFLOW_TERMINALIZATION', operation.workflowName, operation.runId, error);
    }
  }

  async getWorkflowRunTerminalStatus(
    input: GetWorkflowRunTerminalStatusInput,
  ): Promise<GetWorkflowRunTerminalStatusResult> {
    const operation = captureWorkflowRunIdentity(input);
    try {
      const row = await this.#db.client.one<{
        terminal_status: string | null;
        snapshot_exists: boolean;
        snapshot_status: string | null;
      }>(
        `SELECT revision.terminal_status,
                snapshot.run_id IS NOT NULL AS snapshot_exists,
                snapshot.snapshot->>'status' AS snapshot_status
         FROM (SELECT $1::text AS workflow_name, $2::text AS run_id) AS identity
         LEFT JOIN ${this.workflowParentRevisionTableName()} AS revision
           ON revision.workflow_name = identity.workflow_name AND revision.run_id = identity.run_id
         LEFT JOIN ${this.workflowSnapshotTableName()} AS snapshot
           ON snapshot.workflow_name = identity.workflow_name AND snapshot.run_id = identity.run_id`,
        [operation.workflowName, operation.runId],
      );
      if (row.terminal_status !== null) {
        if (!isTerminalWorkflowRunStatus(row.terminal_status)) {
          throw new TypeError('Invalid workflow parent terminal status');
        }
        return { status: 'terminal', terminalStatus: row.terminal_status };
      }
      if (!row.snapshot_exists) return { status: 'missing_run' };
      if (isTerminalWorkflowRunStatus(row.snapshot_status)) {
        return { status: 'terminal', terminalStatus: row.snapshot_status };
      }
      if (!['running', 'suspended', 'waiting', 'pending', 'paused'].includes(row.snapshot_status ?? '')) {
        throw new TypeError('Invalid workflow run status');
      }
      return { status: 'nonterminal' };
    } catch (error) {
      return this.terminalizationError(
        'GET_WORKFLOW_RUN_TERMINAL_STATUS',
        operation.workflowName,
        operation.runId,
        error,
      );
    }
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
    try {
      return await this.#db.client.tx(async t => {
        const context = await this.getTerminalizationContext(t, operation.workflowName, operation.runId);
        if (!context.record && !context.snapshotExists) return { status: 'missing_run' };
        const result = advanceWorkflowTerminalizationRecord(context.record, operation, context.now);
        if (result.status === 'advanced') {
          await this.saveTerminalizationRecord(t, operation.workflowName, operation.runId, result.record);
        }
        return 'record' in result
          ? { status: result.status, record: observeWorkflowTerminalizationRecord(result.record) }
          : result;
      });
    } catch (error) {
      return this.terminalizationError(
        'ADVANCE_WORKFLOW_TERMINALIZATION',
        operation.workflowName,
        operation.runId,
        error,
      );
    }
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
    try {
      return await this.#db.client.tx(async t => {
        const context = await this.getTerminalizationContext(t, operation.workflowName, operation.runId);
        if (!context.record && !context.snapshotExists) return { status: 'missing_run' };
        const result = releaseWorkflowTerminalizationRecord(context.record, operation, context.now);
        if (result.status === 'released') {
          await this.saveTerminalizationRecord(t, operation.workflowName, operation.runId, result.record);
        }
        return 'record' in result
          ? { status: result.status, record: observeWorkflowTerminalizationRecord(result.record) }
          : result;
      });
    } catch (error) {
      return this.terminalizationError(
        'RELEASE_WORKFLOW_TERMINALIZATION',
        operation.workflowName,
        operation.runId,
        error,
      );
    }
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
    try {
      return await this.#db.client.tx(async t => {
        const identity = JSON.stringify([operation.workflowName, operation.runId]);
        await t.none(`SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, [identity]);
        const context = await this.getTerminalizationContext(t, operation.workflowName, operation.runId);
        if (!context.record && !context.snapshotExists) return { status: 'missing_run', count: 0 };
        const eligible =
          context.record?.phase === 'complete' &&
          context.record.completedAt !== undefined &&
          context.record.completedAt < olderThan;
        if (!eligible) return { status: 'deleted', count: 0 };

        // Follow both pre-terminal ancestry and retained parent effects. The
        // ancestry edge exists before a child journal/effect and closes the
        // crash window where parent recovery evidence could otherwise be
        // deleted while a nested child is only partially admitted.
        const dependencyState = await t.one<{ pending: boolean }>(
          `WITH RECURSIVE descendants(workflow_name, run_id) AS (
             SELECT child.workflow_name, child.run_id
             FROM (
               SELECT effect.workflow_name, effect.run_id
               FROM ${this.terminalEffectTableName()} AS effect
               WHERE effect.effect_kind = 'parent-workflow-step-end'
                 AND effect.parent_workflow_name = $1 AND effect.parent_run_id = $2
               UNION
               SELECT ancestry.workflow_name, ancestry.run_id
               FROM ${this.terminalRecoveryAncestryTableName()} AS ancestry
               WHERE ancestry.immediate_parent_workflow_name = $1
                 AND ancestry.immediate_parent_run_id = $2
             ) AS child
             UNION
             SELECT child.workflow_name, child.run_id
             FROM descendants AS parent
             CROSS JOIN LATERAL (
               SELECT effect.workflow_name, effect.run_id
               FROM ${this.terminalEffectTableName()} AS effect
               WHERE effect.effect_kind = 'parent-workflow-step-end'
                 AND effect.parent_workflow_name = parent.workflow_name
                 AND effect.parent_run_id = parent.run_id
               UNION
               SELECT ancestry.workflow_name, ancestry.run_id
               FROM ${this.terminalRecoveryAncestryTableName()} AS ancestry
               WHERE ancestry.immediate_parent_workflow_name = parent.workflow_name
                 AND ancestry.immediate_parent_run_id = parent.run_id
             ) AS child
           )
           SELECT EXISTS (
             SELECT 1
             FROM descendants AS child
             LEFT JOIN ${this.terminalizationTableName()} AS journal
               ON journal.workflow_name = child.workflow_name AND journal.run_id = child.run_id
             WHERE journal.phase IS DISTINCT FROM 'complete'
           ) AS pending`,
          [operation.workflowName, operation.runId],
        );
        if (dependencyState.pending) return { status: 'deleted', count: 0 };

        await this.lockWorkflowParentRevisionWithPresence(t, operation.workflowName, operation.runId);
        await this.latchWorkflowParentTerminalStatus(
          t,
          operation.workflowName,
          operation.runId,
          context.record!.terminalStatus,
        );

        // Delete leaf evidence first and the journal last. The transaction
        // preserves all-or-nothing cleanup when any dependent delete fails.
        await t.none(
          `DELETE FROM ${this.terminalContinuationPlanTableName()}
           WHERE workflow_name = $1 AND run_id = $2`,
          [operation.workflowName, operation.runId],
        );
        await t.none(
          `DELETE FROM ${this.terminalDestinationReceiptTableName()}
           WHERE workflow_name = $1 AND run_id = $2`,
          [operation.workflowName, operation.runId],
        );
        await t.none(
          `DELETE FROM ${this.terminalEffectTableName()}
           WHERE workflow_name = $1 AND run_id = $2`,
          [operation.workflowName, operation.runId],
        );
        await t.none(
          `DELETE FROM ${this.terminalSnapshotTableName()}
           WHERE workflow_name = $1 AND run_id = $2`,
          [operation.workflowName, operation.runId],
        );
        await t.none(
          `DELETE FROM ${this.terminalRecoveryAncestryTableName()}
           WHERE workflow_name = $1 AND run_id = $2`,
          [operation.workflowName, operation.runId],
        );
        const result = await t.query(
          `DELETE FROM ${this.terminalizationTableName()}
           WHERE workflow_name = $1 AND run_id = $2 AND phase = 'complete'
             AND completed_at IS NOT NULL AND completed_at < $3`,
          [operation.workflowName, operation.runId, olderThan],
        );
        const count = result.rowCount ?? 0;
        if (count !== 1) {
          throw new TypeError('Workflow terminal cleanup eligibility changed under lock');
        }
        return { status: 'deleted', count };
      });
    } catch (error) {
      return this.terminalizationError(
        'DELETE_COMPLETED_WORKFLOW_TERMINALIZATIONS',
        operation.workflowName,
        operation.runId,
        error,
      );
    }
  }

  async persistWorkflowTerminalState(
    input: PersistWorkflowTerminalStateInput,
  ): Promise<PersistWorkflowTerminalStateResult> {
    // Capture only scalar identity/fence fields before locking. Caller-owned
    // snapshot and recovery values are read in deterministic order only after
    // the live journal fence and canonical rows authorize this operation.
    const operation = {
      workflowName: input.workflowName,
      runId: input.runId,
      ownerId: input.ownerId,
      claimToken: input.claimToken,
      claimGeneration: input.claimGeneration,
      resourceId: input.resourceId,
      leaseMs: input.leaseMs,
    };
    validateWorkflowTerminalizationFence(operation);
    try {
      return await this.#db.client.tx(async t => {
        const context = await this.getTerminalizationContext(t, operation.workflowName, operation.runId);
        if (!context.record && !context.snapshotExists) return { status: 'missing_run' };
        const authorization = authorizeWorkflowTerminalStateRecord(context.record, operation, context.now);
        if (authorization.status !== 'authorized') {
          return 'record' in authorization
            ? { status: authorization.status, record: observeWorkflowTerminalizationRecord(authorization.record) }
            : authorization;
        }
        const revisionLock = await this.lockWorkflowParentRevisionWithPresence(
          t,
          operation.workflowName,
          operation.runId,
        );
        const deleteCreatedRevision = async (): Promise<void> => {
          if (!revisionLock.created) return;
          await t.none(
            `DELETE FROM ${this.workflowParentRevisionTableName()}
             WHERE workflow_name = $1 AND run_id = $2 AND generation = 0 AND terminal_status IS NULL`,
            [operation.workflowName, operation.runId],
          );
        };
        const snapshotRow = await t.oneOrNone<{ resource_id: unknown }>(
          `SELECT "resourceId" AS resource_id FROM ${this.workflowSnapshotTableName()}
           WHERE workflow_name = $1 AND run_id = $2 FOR UPDATE`,
          [operation.workflowName, operation.runId],
        );
        if (!snapshotRow) {
          await deleteCreatedRevision();
          return { status: 'missing_run' };
        }
        const ancestryRow = await t.oneOrNone<Record<string, unknown>>(
          `SELECT * FROM ${this.terminalRecoveryAncestryTableName()}
           WHERE workflow_name = $1 AND run_id = $2 FOR UPDATE`,
          [operation.workflowName, operation.runId],
        );
        const ancestry = ancestryRow ? this.decodeTerminalRecoveryAncestryRow(ancestryRow, context.now) : undefined;
        const snapshotCapture:
          | { status: 'captured'; value: PersistWorkflowTerminalStateInput['snapshot'] }
          | { status: 'invalid_snapshot' } = (() => {
          try {
            const serializedSnapshot = JSON.stringify(input.snapshot);
            if (serializedSnapshot === undefined) return { status: 'invalid_snapshot' };
            return { status: 'captured', value: JSON.parse(sanitizeJsonForPg(serializedSnapshot)) };
          } catch {
            return { status: 'invalid_snapshot' };
          }
        })();
        if (snapshotCapture.status === 'invalid_snapshot') {
          await deleteCreatedRevision();
          return snapshotCapture;
        }
        const recoveryCapture:
          | { status: 'captured'; value: WorkflowTerminalRecoveryEnvelopeV1 }
          | { status: 'invalid_recovery_envelope' } = (() => {
          try {
            return { status: 'captured', value: materializeWorkflowTerminalRecoveryEnvelope(input.recoveryEnvelope) };
          } catch {
            return { status: 'invalid_recovery_envelope' };
          }
        })();
        if (recoveryCapture.status === 'invalid_recovery_envelope') {
          await deleteCreatedRevision();
          return recoveryCapture;
        }
        const authorizedOperation: PersistWorkflowTerminalStateInput = {
          ...operation,
          snapshot: snapshotCapture.value,
          recoveryEnvelope: recoveryCapture.value,
        };
        const result = persistWorkflowTerminalStateRecord(
          context.record,
          ancestry,
          authorizedOperation,
          context.now,
          snapshot => snapshot,
          (_envelope): WorkflowTerminalRecoveryEnvelopeV1 => recoveryCapture.value,
        );
        if (result.status === 'advanced') {
          const currentResourceId =
            snapshotRow.resource_id === null || snapshotRow.resource_id === undefined
              ? undefined
              : snapshotRow.resource_id;
          const resourceId = operation.resourceId ?? currentResourceId;
          if (resourceId !== undefined) {
            validateWorkflowTerminalizationIdentity(resourceId as string, 'resourceId', 512);
          }
          await this.saveTerminalWorkflowSnapshot(
            t,
            { ...authorizedOperation, resourceId: resourceId as string | undefined },
            result.snapshot,
            result.recovery,
            context.now,
            revisionLock.generation,
          );
          await this.saveTerminalizationRecord(t, operation.workflowName, operation.runId, result.record);
          return { status: 'persisted', record: observeWorkflowTerminalizationRecord(result.record) };
        }
        await deleteCreatedRevision();
        return 'record' in result
          ? { status: result.status, record: observeWorkflowTerminalizationRecord(result.record) }
          : result;
      });
    } catch (error) {
      return this.terminalizationError(
        'PERSIST_WORKFLOW_TERMINAL_STATE',
        operation.workflowName,
        operation.runId,
        error,
      );
    }
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
    try {
      return await this.#db.client.tx(async t => {
        const context = await this.getTerminalizationContext(t, operation.workflowName, operation.runId);
        if (!context.record && !context.snapshotExists) return { status: 'missing_run' };
        const existingEffect = await this.getTerminalEffectRecord(
          t,
          operation.workflowName,
          operation.runId,
          operation.effect.kind,
          context.now,
        );
        if (existingEffect && context.record) {
          if (existingEffect.kind !== operation.effect.kind) {
            throw new TypeError('Invalid workflow terminal effect kind');
          }
          validateWorkflowTerminalEffectJournalLink(
            existingEffect,
            context.record,
            operation.workflowName,
            operation.runId,
          );
        }
        const retained = await this.getTerminalSnapshotRecord(t, operation.workflowName, operation.runId, context.now);
        const result = prepareWorkflowTerminalEffectRecord(
          context.record,
          existingEffect,
          retained,
          operation,
          context.now,
        );
        if (result.status === 'prepared' || result.status === 'already_prepared') {
          if (!retained) return { status: 'missing_terminal_state' };
          validateWorkflowTerminalSnapshotJournalLink(retained, result.record, operation.workflowName, operation.runId);
          validateWorkflowTerminalEffectRecoveryLink(result.effect, retained);
        }
        if (result.status === 'prepared') {
          await this.insertTerminalEffectRecord(t, result.effect);
          await this.saveTerminalizationRecord(t, operation.workflowName, operation.runId, result.record);
          return { status: result.status, effect: result.effect };
        }
        if (result.status === 'already_prepared') {
          if (operation.leaseMs !== undefined) {
            await this.saveTerminalizationRecord(t, operation.workflowName, operation.runId, result.record);
          }
          return { status: result.status, effect: result.effect };
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
      });
    } catch (error) {
      return this.terminalizationError(
        'PREPARE_WORKFLOW_TERMINAL_EFFECT',
        operation.workflowName,
        operation.runId,
        error,
      );
    }
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
    try {
      return await this.#db.client.tx(async t => {
        const context = await this.getTerminalizationContext(t, operation.workflowName, operation.runId);
        if (!context.record && !context.snapshotExists) return { status: 'missing_run' };
        const existingEffect = await this.getTerminalEffectRecord(
          t,
          operation.workflowName,
          operation.runId,
          operation.kind,
          context.now,
        );
        if (existingEffect && context.record) {
          if (existingEffect.kind !== operation.kind) {
            throw new TypeError('Invalid workflow terminal effect kind');
          }
          validateWorkflowTerminalEffectJournalLink(
            existingEffect,
            context.record,
            operation.workflowName,
            operation.runId,
          );
        }
        const result = getWorkflowTerminalEffectForDispatchRecord(
          context.record,
          existingEffect,
          operation,
          context.now,
        );
        const retained =
          result.status === 'found'
            ? await this.getTerminalSnapshotRecord(t, operation.workflowName, operation.runId, context.now)
            : undefined;
        if (result.status === 'found' && !retained) return { status: 'missing_terminal_state' };
        if (result.status === 'found') {
          validateWorkflowTerminalSnapshotJournalLink(
            retained!,
            context.record!,
            operation.workflowName,
            operation.runId,
          );
          validateWorkflowTerminalEffectRecoveryLink(result.effect, retained!);
        }
        return 'record' in result
          ? { status: result.status, record: observeWorkflowTerminalizationRecord(result.record) }
          : result.status === 'found'
            ? { ...result, recovery: retained! }
            : result;
      });
    } catch (error) {
      return this.terminalizationError(
        'GET_WORKFLOW_TERMINAL_EFFECT_FOR_DISPATCH',
        operation.workflowName,
        operation.runId,
        error,
      );
    }
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
    validateWorkflowTerminalizationFence(operation);
    validateWorkflowTerminalizationRunIdentity(operation);
    validateWorkflowTerminalizationIdentity(operation.consumerId, 'consumerId', 256);
    try {
      return await this.#db.client.tx(async t => {
        const context = await this.getTerminalizationContext(t, operation.workflowName, operation.runId);
        if (!context.record && !context.snapshotExists) return { status: 'missing_run' };
        const effect = await this.getTerminalEffectRecord(
          t,
          operation.workflowName,
          operation.runId,
          operation.effectKind,
          context.now,
        );
        if (effect && context.record) {
          validateWorkflowTerminalEffectJournalLink(effect, context.record, operation.workflowName, operation.runId);
        }
        const receipt = effect
          ? await this.getTerminalDestinationReceiptRecord(t, effect, operation.consumerId, context.now)
          : undefined;
        const receiptCount = effect ? await this.countTerminalDestinationReceiptRecords(t, effect) : 0;
        const result = reserveWorkflowTerminalDestinationReceiptRecord(
          context.record,
          effect,
          receipt,
          receiptCount,
          operation,
          context.now,
        );
        if (result.status === 'reserved' || result.status === 'already_exists') {
          const retained = await this.getTerminalSnapshotRecord(
            t,
            operation.workflowName,
            operation.runId,
            context.now,
          );
          if (!retained) return { status: 'missing_terminal_state' };
          validateWorkflowTerminalSnapshotJournalLink(
            retained,
            context.record!,
            operation.workflowName,
            operation.runId,
          );
          validateWorkflowTerminalEffectRecoveryLink(effect!, retained);
          if (result.status === 'reserved') {
            await this.insertTerminalDestinationReceiptRecord(t, result.receipt);
          }
          return { status: result.status, receipt: copyWorkflowTerminalDestinationReceiptRecord(result.receipt) };
        }
        return 'record' in result
          ? { status: result.status, record: observeWorkflowTerminalizationRecord(result.record) }
          : result;
      });
    } catch (error) {
      return this.terminalizationError(
        'RESERVE_WORKFLOW_TERMINAL_DESTINATION_RECEIPT',
        operation.workflowName,
        operation.runId,
        error,
      );
    }
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
    validateWorkflowTerminalizationFence(operation);
    validateWorkflowTerminalizationRunIdentity(operation);
    validateWorkflowTerminalizationIdentity(operation.consumerId, 'consumerId', 256);
    try {
      return await this.#db.client.tx(async t => {
        const context = await this.getTerminalizationContext(t, operation.workflowName, operation.runId);
        if (!context.record && !context.snapshotExists) return { status: 'missing_run' };
        const effect = await this.getTerminalEffectRecord(
          t,
          operation.workflowName,
          operation.runId,
          operation.effectKind,
          context.now,
        );
        if (effect && context.record) {
          validateWorkflowTerminalEffectJournalLink(effect, context.record, operation.workflowName, operation.runId);
        }
        const receipt = effect
          ? await this.getTerminalDestinationReceiptRecord(t, effect, operation.consumerId, context.now)
          : undefined;
        const result = getWorkflowTerminalDestinationReceiptRecord(
          context.record,
          effect,
          receipt,
          operation,
          context.now,
        );
        if (result.status === 'found') {
          const retained = await this.getTerminalSnapshotRecord(
            t,
            operation.workflowName,
            operation.runId,
            context.now,
          );
          if (!retained) return { status: 'missing_terminal_state' };
          validateWorkflowTerminalSnapshotJournalLink(
            retained,
            context.record!,
            operation.workflowName,
            operation.runId,
          );
          validateWorkflowTerminalEffectRecoveryLink(effect!, retained);
          return { status: 'found', receipt: copyWorkflowTerminalDestinationReceiptRecord(result.receipt) };
        }
        return 'record' in result
          ? { status: result.status, record: observeWorkflowTerminalizationRecord(result.record) }
          : result;
      });
    } catch (error) {
      return this.terminalizationError(
        'GET_WORKFLOW_TERMINAL_DESTINATION_RECEIPT',
        operation.workflowName,
        operation.runId,
        error,
      );
    }
  }

  async getWorkflowTerminalParentContext(
    input: GetWorkflowTerminalParentContextInput,
  ): Promise<GetWorkflowTerminalParentContextResult> {
    const operation = {
      workflowName: input.workflowName,
      runId: input.runId,
      ownerId: input.ownerId,
      claimToken: input.claimToken,
      claimGeneration: input.claimGeneration,
      kind: 'parent-workflow-step-end' as const,
    };
    validateWorkflowTerminalizationRunIdentity(operation);
    validateWorkflowTerminalizationFence(operation);
    try {
      return await this.#db.client.tx(async t => {
        const context = await this.getTerminalizationContext(t, operation.workflowName, operation.runId);
        if (!context.record && !context.snapshotExists) return { status: 'missing_run' };
        const effect = await this.getTerminalEffectRecord(
          t,
          operation.workflowName,
          operation.runId,
          operation.kind,
          context.now,
        );
        const result = getWorkflowTerminalEffectForDispatchRecord(context.record, effect, operation, context.now);
        if (result.status !== 'found') {
          return 'record' in result
            ? { status: result.status, record: observeWorkflowTerminalizationRecord(result.record) }
            : result;
        }
        if (result.effect.kind !== 'parent-workflow-step-end') return { status: 'missing_effect' };
        if (context.record?.phase !== 'parent_outbox_pending') {
          return context.record
            ? { status: 'phase_conflict', record: observeWorkflowTerminalizationRecord(context.record) }
            : { status: 'missing_record' };
        }
        validateWorkflowTerminalEffectJournalLink(
          result.effect,
          context.record,
          operation.workflowName,
          operation.runId,
        );
        const retained = await this.getTerminalSnapshotRecord(t, operation.workflowName, operation.runId, context.now);
        if (!retained) return { status: 'missing_terminal_state' };
        validateWorkflowTerminalSnapshotJournalLink(retained, context.record, operation.workflowName, operation.runId);
        validateWorkflowTerminalEffectRecoveryLink(result.effect, retained);
        const parentWorkflowName = result.effect.parentWorkflowName;
        const parentRunId = result.effect.parentRunId;
        const revisionLock = await this.lockWorkflowParentRevisionWithPresence(t, parentWorkflowName, parentRunId);
        const deleteProvisionalRevision = async (): Promise<void> => {
          await t.none(
            `DELETE FROM ${this.workflowParentRevisionTableName()}
             WHERE workflow_name = $1 AND run_id = $2 AND generation = 0 AND terminal_status IS NULL`,
            [parentWorkflowName, parentRunId],
          );
        };
        const row = await t.oneOrNone<{ snapshot: WorkflowRunState | string }>(
          `SELECT snapshot FROM ${this.workflowSnapshotTableName()}
           WHERE workflow_name = $1 AND run_id = $2 FOR UPDATE`,
          [parentWorkflowName, parentRunId],
        );
        if (!row) {
          await deleteProvisionalRevision();
          return { status: 'missing_parent' };
        }
        let generation = revisionLock.generation;
        if (!Number.isSafeInteger(generation) || generation < 1) {
          await deleteProvisionalRevision();
          return { status: 'corrupt_parent_state' };
        }
        const snapshot = typeof row.snapshot === 'string' ? JSON.parse(row.snapshot) : row.snapshot;
        const snapshotTerminalStatus = isTerminalWorkflowRunStatus(snapshot.status) ? snapshot.status : undefined;
        if (revisionLock.terminalStatus && revisionLock.terminalStatus !== snapshotTerminalStatus) {
          return { status: 'parent_conflict' };
        }
        if (!revisionLock.terminalStatus && snapshotTerminalStatus) {
          generation = await this.latchWorkflowParentTerminalStatus(
            t,
            result.effect.parentWorkflowName,
            result.effect.parentRunId,
            snapshotTerminalStatus,
          );
        }
        return {
          status: 'found',
          effect: copyWorkflowTerminalEffectRecord(result.effect) as Extract<
            WorkflowTerminalEffectRecord,
            { kind: 'parent-workflow-step-end' }
          >,
          retainedChild: structuredClone(retained),
          parentWorkflowName: result.effect.parentWorkflowName,
          parentRunId: result.effect.parentRunId,
          revision: `pg:v1:${generation}`,
          snapshot: structuredClone(snapshot),
        };
      });
    } catch (error) {
      return this.terminalizationError(
        'GET_WORKFLOW_TERMINAL_PARENT_CONTEXT',
        operation.workflowName,
        operation.runId,
        error,
      );
    }
  }

  async getWorkflowTerminalContinuationPlan(
    input: GetWorkflowTerminalContinuationPlanInput,
  ): Promise<GetWorkflowTerminalContinuationPlanResult> {
    const operation: GetWorkflowTerminalContinuationPlanInput = {
      workflowName: input.workflowName,
      runId: input.runId,
      ownerId: input.ownerId,
      claimToken: input.claimToken,
      claimGeneration: input.claimGeneration,
    };
    validateWorkflowTerminalizationRunIdentity(operation);
    validateWorkflowTerminalizationFence(operation);
    try {
      return await this.#db.client.tx(async t => {
        const context = await this.getTerminalizationContext(t, operation.workflowName, operation.runId);
        if (!context.record && !context.snapshotExists) return { status: 'missing_run' };
        const effect = await this.getTerminalEffectRecord(
          t,
          operation.workflowName,
          operation.runId,
          'parent-workflow-step-end',
          context.now,
        );
        const receipt =
          effect?.kind === 'parent-workflow-step-end'
            ? await this.getTerminalDestinationReceiptRecord(
                t,
                effect,
                WORKFLOW_TERMINAL_PARENT_APPLICATION_CONSUMER_ID,
                context.now,
              )
            : undefined;
        const plan =
          effect?.kind === 'parent-workflow-step-end' && receipt
            ? await this.getTerminalContinuationPlanRecord(t, effect, receipt, context.now)
            : undefined;
        const result = getWorkflowTerminalContinuationPlanRecord(
          context.record,
          effect,
          receipt,
          plan,
          operation,
          context.now,
        );
        return 'record' in result
          ? { status: result.status, record: observeWorkflowTerminalizationRecord(result.record) }
          : result;
      });
    } catch (error) {
      return this.terminalizationError(
        'GET_WORKFLOW_TERMINAL_CONTINUATION_PLAN',
        operation.workflowName,
        operation.runId,
        error,
      );
    }
  }

  async applyWorkflowTerminalParentEffect(
    input: ApplyWorkflowTerminalParentEffectInput,
  ): Promise<ApplyWorkflowTerminalParentEffectResult> {
    let contract: ApplyWorkflowTerminalParentEffectInput['contract'];
    try {
      contract = copyWorkflowTerminalParentContinuationContract(input.contract);
    } catch {
      return { status: 'invalid_contract' };
    }
    const operation: ApplyWorkflowTerminalParentEffectInput = {
      workflowName: input.workflowName,
      runId: input.runId,
      ownerId: input.ownerId,
      claimToken: input.claimToken,
      claimGeneration: input.claimGeneration,
      contract,
    };
    validateWorkflowTerminalizationRunIdentity(operation);
    validateWorkflowTerminalizationFence(operation);
    try {
      return await this.#db.client.tx(async t => {
        const context = await this.getTerminalizationContext(t, operation.workflowName, operation.runId);
        if (!context.record && !context.snapshotExists) return { status: 'missing_run' };
        const effect = await this.getTerminalEffectRecord(
          t,
          operation.workflowName,
          operation.runId,
          'parent-workflow-step-end',
          context.now,
        );
        const receipt =
          effect?.kind === 'parent-workflow-step-end'
            ? await this.getTerminalDestinationReceiptRecord(
                t,
                effect,
                WORKFLOW_TERMINAL_PARENT_APPLICATION_CONSUMER_ID,
                context.now,
              )
            : undefined;
        const existingPlan =
          effect?.kind === 'parent-workflow-step-end' && receipt
            ? await this.getTerminalContinuationPlanRecord(t, effect, receipt, context.now)
            : undefined;
        let prepared = prepareWorkflowTerminalParentApplicationRecords(
          context.record,
          effect,
          receipt,
          existingPlan,
          operation,
          context.now,
        );
        if (prepared.status === 'contract_conflict') {
          return { status: prepared.status, plan: observeWorkflowTerminalContinuationPlanRecord(prepared.plan) };
        }
        if ('record' in prepared) {
          return { status: prepared.status, record: observeWorkflowTerminalizationRecord(prepared.record) };
        }
        if (!('journal' in prepared)) return { status: prepared.status };
        if (prepared.status === 'already_applied' || prepared.status === 'already_quarantined') {
          return { status: prepared.status, plan: copyWorkflowTerminalContinuationPlanRecord(prepared.plan) };
        }
        if (!effect || effect.kind !== 'parent-workflow-step-end') {
          throw new TypeError('Parent application became ready without a parent effect');
        }

        let retained: WorkflowTerminalSnapshotRecord | undefined;
        try {
          retained = await this.getTerminalSnapshotRecord(t, operation.workflowName, operation.runId, context.now);
        } catch (error) {
          if (error instanceof CorruptWorkflowTerminalSnapshotRecordError) {
            return { status: 'corrupt_child_terminal_state' };
          }
          throw error;
        }
        if (!retained) return { status: 'missing_child_terminal_state' };
        try {
          validateWorkflowTerminalSnapshotJournalLink(
            retained,
            prepared.journal,
            operation.workflowName,
            operation.runId,
          );
          validateWorkflowTerminalEffectRecoveryLink(effect, retained);
        } catch {
          return { status: 'corrupt_child_terminal_state' };
        }
        const revisionRow = await t.oneOrNone<{ generation: number | string; terminal_status: string | null }>(
          `SELECT generation, terminal_status FROM ${this.workflowParentRevisionTableName()}
           WHERE workflow_name = $1 AND run_id = $2
           FOR UPDATE`,
          [effect.parentWorkflowName, effect.parentRunId],
        );
        const parentRow = await t.oneOrNone<{ snapshot: WorkflowRunState | string }>(
          `SELECT snapshot FROM ${this.workflowSnapshotTableName()}
           WHERE workflow_name = $1 AND run_id = $2 FOR UPDATE`,
          [effect.parentWorkflowName, effect.parentRunId],
        );
        if (!parentRow) return { status: 'missing_parent' };
        if (!revisionRow) return { status: 'corrupt_parent_state' };
        const parentGeneration =
          typeof revisionRow.generation === 'string' ? Number(revisionRow.generation) : revisionRow.generation;
        if (!Number.isSafeInteger(parentGeneration) || parentGeneration < 1) {
          return { status: 'corrupt_parent_state' };
        }
        let parentSnapshot: WorkflowRunState;
        try {
          parentSnapshot = structuredClone(
            typeof parentRow.snapshot === 'string' ? JSON.parse(parentRow.snapshot) : parentRow.snapshot,
          );
        } catch {
          return { status: 'corrupt_parent_state' };
        }

        if (`pg:v1:${parentGeneration}` !== operation.contract.expectedParentRevision) {
          return { status: 'parent_conflict' };
        }
        const snapshotTerminalStatus = isTerminalWorkflowRunStatus(parentSnapshot.status)
          ? parentSnapshot.status
          : undefined;
        const terminalNoop = operation.contract.action.kind === 'noop' && operation.contract.patch.kind === 'none';
        if (revisionRow.terminal_status !== null || snapshotTerminalStatus !== undefined) {
          if (
            !isTerminalWorkflowRunStatus(revisionRow.terminal_status) ||
            snapshotTerminalStatus === undefined ||
            revisionRow.terminal_status !== snapshotTerminalStatus ||
            !terminalNoop
          ) {
            return { status: 'parent_conflict' };
          }
        }

        const finalClock = await t.one<{ now_ms: string }>(
          `SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint AS now_ms`,
        );
        const finalNow = Number(finalClock.now_ms);
        if (!Number.isSafeInteger(finalNow)) throw new TypeError('Invalid PostgreSQL terminalization clock');
        const revalidated = prepareWorkflowTerminalParentApplicationRecords(
          context.record,
          effect,
          receipt,
          existingPlan,
          operation,
          finalNow,
        );
        if (revalidated.status === 'contract_conflict') {
          return {
            status: revalidated.status,
            plan: observeWorkflowTerminalContinuationPlanRecord(revalidated.plan),
          };
        }
        if ('record' in revalidated) {
          return { status: revalidated.status, record: observeWorkflowTerminalizationRecord(revalidated.record) };
        }
        if (!('journal' in revalidated)) return { status: revalidated.status };
        if (revalidated.status === 'already_applied' || revalidated.status === 'already_quarantined') {
          return {
            status: revalidated.status,
            plan: copyWorkflowTerminalContinuationPlanRecord(revalidated.plan),
          };
        }
        prepared = revalidated;

        const storageTimestamp = finalNow;
        let patchTimestamp: number;
        try {
          patchTimestamp = validateWorkflowSnapshotTimestampForFinalState(parentSnapshot.timestamp, storageTimestamp);
        } catch {
          return { status: 'corrupt_parent_state' };
        }
        let patchedParent: WorkflowRunState;
        try {
          patchedParent = applyWorkflowTerminalParentContinuationPatch({
            contract: prepared.plan.contract,
            effect,
            parentRevision: operation.contract.expectedParentRevision,
            parentWorkflowName: effect.parentWorkflowName,
            parentSnapshot,
            retainedChild: retained,
            storageTimestamp: patchTimestamp,
            executionMode: 'continuous',
          });
        } catch (error) {
          if (error instanceof WorkflowTerminalContinuationStoredStateError) {
            return {
              status: error.state === 'child' ? 'corrupt_child_terminal_state' : 'corrupt_parent_state',
            };
          }
          return { status: 'invalid_contract' };
        }
        const finalized = finalizeWorkflowTerminalParentApplicationRecords(
          prepared.journal,
          prepared.receipt,
          prepared.plan,
          storageTimestamp,
        );
        if (finalized.status === 'applied' && finalized.plan.contract.patch.kind !== 'none') {
          const serializedParent = sanitizeJsonForPg(JSON.stringify(patchedParent));
          const timestamp = new Date(storageTimestamp);
          const update = await t.query(
            `UPDATE ${this.workflowSnapshotTableName()}
             SET snapshot = $1, "updatedAt" = $2, "updatedAtZ" = $3
             WHERE workflow_name = $4 AND run_id = $5`,
            [serializedParent, timestamp, timestamp, effect.parentWorkflowName, effect.parentRunId],
          );
          if ((update.rowCount ?? 0) !== 1) return { status: 'parent_conflict' };
          await this.bumpWorkflowParentRevision(t, effect.parentWorkflowName, effect.parentRunId, parentGeneration);
        }
        await this.saveTerminalDestinationReceiptRecord(t, finalized.receipt, receipt !== undefined);
        await this.insertTerminalContinuationPlanRecord(t, finalized.plan);
        await this.saveTerminalizationRecord(t, operation.workflowName, operation.runId, finalized.journal);
        return { status: finalized.status, plan: copyWorkflowTerminalContinuationPlanRecord(finalized.plan) };
      });
    } catch (error) {
      return this.terminalizationError(
        'APPLY_WORKFLOW_TERMINAL_PARENT_EFFECT',
        operation.workflowName,
        operation.runId,
        error,
      );
    }
  }

  private parseWorkflowRun(row: Record<string, any>): WorkflowRun {
    let parsedSnapshot: WorkflowRunState | string = row.snapshot as string;
    if (typeof parsedSnapshot === 'string') {
      try {
        parsedSnapshot = JSON.parse(row.snapshot as string) as WorkflowRunState;
      } catch (e) {
        this.logger.warn(`Failed to parse snapshot for workflow ${row.workflow_name}: ${e}`);
      }
    }
    return {
      workflowName: row.workflow_name as string,
      runId: row.run_id as string,
      snapshot: parsedSnapshot,
      resourceId: row.resourceId as string,
      createdAt: new Date(row.createdAtZ || (row.createdAt as string)),
      updatedAt: new Date(row.updatedAtZ || (row.updatedAt as string)),
    };
  }

  /**
   * Returns all DDL statements for this domain: table with unique constraint.
   * Used by exportSchemas to produce a complete, reproducible schema export.
   */
  static getExportDDL(schemaName?: string): string[] {
    const statements: string[] = [];

    // Table (includes the UNIQUE constraint on workflow_name, run_id via generateTableSQL)
    statements.push(
      generateTableSQL({
        tableName: TABLE_WORKFLOW_SNAPSHOT,
        schema: TABLE_SCHEMAS[TABLE_WORKFLOW_SNAPSHOT],
        schemaName,
        includeAllConstraints: true,
      }),
    );
    statements.push(WorkflowsPG.getTerminalizationTableDDL(schemaName));
    statements.push(WorkflowsPG.getTerminalEffectTableDDL(schemaName));
    statements.push(WorkflowsPG.getTerminalSnapshotTableDDL(schemaName));
    statements.push(WorkflowsPG.getTerminalRecoveryAncestryTableDDL(schemaName));
    statements.push(WorkflowsPG.getTerminalDestinationReceiptTableDDL(schemaName));
    statements.push(WorkflowsPG.getWorkflowParentRevisionTableDDL(schemaName));
    statements.push(WorkflowsPG.getTerminalContinuationPlanTableDDL(schemaName));
    for (const index of WorkflowsPG.getDefaultIndexDefs(schemaName)) {
      statements.push(generateIndexSQL(index, schemaName));
    }

    return statements;
  }

  /** Returns the terminalization recovery and retention indexes. */
  static getDefaultIndexDefs(_schemaName?: string): CreateIndexOptions[] {
    return [
      {
        name: 'mastra_workflow_terminalizations_phase_lease_idx',
        table: TABLE_WORKFLOW_TERMINALIZATIONS,
        columns: ['phase', 'lease_expires_at'],
      },
      {
        name: 'mastra_workflow_terminalizations_completed_idx',
        table: TABLE_WORKFLOW_TERMINALIZATIONS,
        columns: ['completed_at'],
      },
      {
        name: 'mastra_workflow_terminal_destination_receipts_v2_lookup_idx',
        table: TABLE_WORKFLOW_TERMINAL_DESTINATION_RECEIPTS,
        columns: ['workflow_name', 'run_id', 'effect_kind', 'consumer_id'],
      },
      {
        name: 'mastra_workflow_terminal_continuation_plans_v2_run_idx',
        table: TABLE_WORKFLOW_TERMINAL_CONTINUATION_PLANS,
        columns: ['workflow_name', 'run_id'],
      },
      {
        name: 'mastra_workflow_terminal_recovery_ancestries_parent_idx',
        table: TABLE_WORKFLOW_TERMINAL_RECOVERY_ANCESTRIES,
        columns: ['immediate_parent_workflow_name', 'immediate_parent_run_id'],
      },
      {
        name: 'mastra_workflow_terminal_effects_v2_parent_idx',
        table: TABLE_WORKFLOW_TERMINAL_EFFECTS,
        columns: ['parent_workflow_name', 'parent_run_id'],
        where: `"effect_kind" = 'parent-workflow-step-end'`,
      },
    ];
  }

  private static getTerminalizationTableDDL(schemaName?: string): string {
    const tableName = getTableName({
      indexName: TABLE_WORKFLOW_TERMINALIZATIONS,
      schemaName: getSchemaName(schemaName),
    });
    return `CREATE TABLE IF NOT EXISTS ${tableName} (
      "workflow_name" TEXT NOT NULL,
      "run_id" TEXT NOT NULL,
      "version" INTEGER NOT NULL,
      "event_key" TEXT NOT NULL,
      "terminal_status" TEXT NOT NULL,
      "phase" TEXT NOT NULL,
      "owner_id" TEXT,
      "claim_token" TEXT,
      "claim_generation" BIGINT NOT NULL,
      "lease_expires_at" BIGINT,
      "created_at" BIGINT NOT NULL,
      "updated_at" BIGINT NOT NULL,
      "completed_at" BIGINT,
      PRIMARY KEY ("workflow_name", "run_id")
    );`;
  }

  private static getTerminalEffectTableDDL(schemaName?: string): string {
    const tableName = getTableName({
      indexName: TABLE_WORKFLOW_TERMINAL_EFFECTS,
      schemaName: getSchemaName(schemaName),
    });
    return `CREATE TABLE IF NOT EXISTS ${tableName} (
      "workflow_name" TEXT NOT NULL,
      "run_id" TEXT NOT NULL,
      "effect_kind" TEXT NOT NULL,
      "version" INTEGER NOT NULL,
      "effect_key" TEXT NOT NULL UNIQUE,
      "source_event_key" TEXT NOT NULL,
      "terminal_status" TEXT NOT NULL,
      "parent_workflow_name" TEXT,
      "parent_run_id" TEXT,
      "parent_step_id" TEXT,
      "parent_execution_path" JSONB,
      "recovery_envelope_hash" TEXT NOT NULL,
      "retained_record_hash" TEXT NOT NULL,
      "resource_id" TEXT,
      "payload_hash" TEXT NOT NULL,
      "created_at" BIGINT NOT NULL,
      PRIMARY KEY ("workflow_name", "run_id", "effect_kind")
    );`;
  }

  private static getTerminalSnapshotTableDDL(schemaName?: string): string {
    const tableName = getTableName({
      indexName: TABLE_WORKFLOW_TERMINAL_SNAPSHOTS,
      schemaName: getSchemaName(schemaName),
    });
    return `CREATE TABLE IF NOT EXISTS ${tableName} (
      "workflow_name" TEXT NOT NULL,
      "run_id" TEXT NOT NULL,
      "version" INTEGER NOT NULL,
      "resource_id" TEXT,
      "terminal_status" TEXT NOT NULL,
      "envelope_hash" TEXT NOT NULL,
      "record_hash" TEXT NOT NULL,
      "envelope" JSONB NOT NULL,
      "created_at" BIGINT NOT NULL,
      CHECK ("version" = 1),
      CHECK ("envelope_hash" ~ '^sha256:[a-f0-9]{64}$'),
      CHECK ("record_hash" ~ '^sha256:[a-f0-9]{64}$'),
      CHECK ((jsonb_typeof("envelope") = 'object') IS TRUE),
      CHECK ("created_at" >= 0),
      PRIMARY KEY ("workflow_name", "run_id")
    );`;
  }

  private static getTerminalRecoveryAncestryTableDDL(schemaName?: string): string {
    const tableName = getTableName({
      indexName: TABLE_WORKFLOW_TERMINAL_RECOVERY_ANCESTRIES,
      schemaName: getSchemaName(schemaName),
    });
    return `CREATE TABLE IF NOT EXISTS ${tableName} (
      "workflow_name" TEXT NOT NULL,
      "run_id" TEXT NOT NULL,
      "version" INTEGER NOT NULL,
      "ancestry_hash" TEXT NOT NULL,
      "ancestry" JSONB NOT NULL,
      "immediate_parent_workflow_name" TEXT,
      "immediate_parent_run_id" TEXT,
      "created_at" BIGINT NOT NULL,
      CHECK ("version" = 1),
      CHECK ("ancestry_hash" ~ '^sha256:[a-f0-9]{64}$'),
      CHECK ((jsonb_typeof("ancestry") = 'array') IS TRUE),
      CHECK (("immediate_parent_workflow_name" IS NULL) = ("immediate_parent_run_id" IS NULL)),
      CHECK (
        (jsonb_array_length("ancestry") = 0 AND "immediate_parent_workflow_name" IS NULL)
        OR (
          jsonb_array_length("ancestry") > 0
          AND "immediate_parent_workflow_name" = "ancestry" #>> '{0,parentWorkflowName}'
          AND "immediate_parent_run_id" = "ancestry" #>> '{0,parentRunId}'
        )
      ),
      CHECK ("created_at" >= 0),
      PRIMARY KEY ("workflow_name", "run_id")
    );`;
  }

  private static getTerminalDestinationReceiptTableDDL(schemaName?: string): string {
    const tableName = getTableName({
      indexName: TABLE_WORKFLOW_TERMINAL_DESTINATION_RECEIPTS,
      schemaName: getSchemaName(schemaName),
    });
    const effectTableName = getTableName({
      indexName: TABLE_WORKFLOW_TERMINAL_EFFECTS,
      schemaName: getSchemaName(schemaName),
    });
    return `CREATE TABLE IF NOT EXISTS ${tableName} (
      "version" INTEGER NOT NULL,
      "workflow_name" TEXT NOT NULL,
      "run_id" TEXT NOT NULL,
      "effect_key" TEXT NOT NULL,
      "consumer_id" TEXT NOT NULL,
      "receipt_key" TEXT NOT NULL UNIQUE,
      "effect_kind" TEXT NOT NULL,
      "producer_payload_hash" TEXT NOT NULL,
      "destination_hash" TEXT NOT NULL,
      "application_state" TEXT NOT NULL,
      "dispatch_state" TEXT NOT NULL,
      "created_at" BIGINT NOT NULL,
      "updated_at" BIGINT NOT NULL,
      "applied_at" BIGINT,
      "dispatch_pending_at" BIGINT,
      "destination_applied_at" BIGINT,
      "quarantined_at" BIGINT,
      PRIMARY KEY ("effect_key", "consumer_id"),
      FOREIGN KEY ("effect_key") REFERENCES ${effectTableName} ("effect_key") ON DELETE CASCADE,
      CHECK ("version" = 1),
      CHECK ("created_at" >= 0 AND "updated_at" >= "created_at"),
      CHECK (
        ("application_state" = 'reserved' AND "dispatch_state" = 'none'
          AND "updated_at" = "created_at"
          AND "applied_at" IS NULL AND "dispatch_pending_at" IS NULL
          AND "destination_applied_at" IS NULL AND "quarantined_at" IS NULL)
        OR
        ("application_state" = 'applied' AND "dispatch_state" = 'none'
          AND "applied_at" IS NOT NULL AND "applied_at" = "updated_at" AND "dispatch_pending_at" IS NULL
          AND "destination_applied_at" IS NULL AND "quarantined_at" IS NULL)
        OR
        ("application_state" = 'applied' AND "dispatch_state" = 'pending'
          AND "applied_at" IS NOT NULL AND "dispatch_pending_at" IS NOT NULL
          AND "dispatch_pending_at" = "updated_at"
          AND "created_at" <= "applied_at"
          AND "applied_at" <= "dispatch_pending_at"
          AND "destination_applied_at" IS NULL AND "quarantined_at" IS NULL)
        OR
        ("application_state" = 'applied' AND "dispatch_state" = 'destination_applied'
          AND "applied_at" IS NOT NULL AND "dispatch_pending_at" IS NOT NULL
          AND "destination_applied_at" IS NOT NULL AND "destination_applied_at" = "updated_at"
          AND "created_at" <= "applied_at"
          AND "applied_at" <= "dispatch_pending_at"
          AND "dispatch_pending_at" <= "destination_applied_at" AND "quarantined_at" IS NULL)
        OR
        ("application_state" = 'quarantined' AND "dispatch_state" = 'none'
          AND "quarantined_at" IS NOT NULL AND "quarantined_at" = "updated_at"
          AND "applied_at" IS NULL AND "dispatch_pending_at" IS NULL AND "destination_applied_at" IS NULL)
      )
    );`;
  }

  private static getWorkflowParentRevisionTableDDL(schemaName?: string): string {
    const tableName = getTableName({
      indexName: TABLE_WORKFLOW_PARENT_REVISIONS,
      schemaName: getSchemaName(schemaName),
    });
    return `CREATE TABLE IF NOT EXISTS ${tableName} (
      "workflow_name" TEXT NOT NULL,
      "run_id" TEXT NOT NULL,
      "generation" BIGINT NOT NULL,
      "terminal_status" TEXT,
      "updated_at" BIGINT NOT NULL,
      PRIMARY KEY ("workflow_name", "run_id"),
      CHECK ("generation" >= 0),
      CHECK ("terminal_status" IS NULL OR "terminal_status" IN ('success', 'failed', 'canceled', 'tripwire', 'bailed')),
      CHECK ("updated_at" >= 0)
    );`;
  }

  private static getTerminalContinuationPlanTableDDL(schemaName?: string): string {
    const tableName = getTableName({
      indexName: TABLE_WORKFLOW_TERMINAL_CONTINUATION_PLANS,
      schemaName: getSchemaName(schemaName),
    });
    const receiptTableName = getTableName({
      indexName: TABLE_WORKFLOW_TERMINAL_DESTINATION_RECEIPTS,
      schemaName: getSchemaName(schemaName),
    });
    const effectTableName = getTableName({
      indexName: TABLE_WORKFLOW_TERMINAL_EFFECTS,
      schemaName: getSchemaName(schemaName),
    });
    return `CREATE TABLE IF NOT EXISTS ${tableName} (
      "version" INTEGER NOT NULL,
      "plan_key" TEXT NOT NULL UNIQUE,
      "plan_hash" TEXT NOT NULL,
      "receipt_key" TEXT NOT NULL PRIMARY KEY,
      "effect_key" TEXT NOT NULL,
      "consumer_id" TEXT NOT NULL,
      "workflow_name" TEXT NOT NULL,
      "run_id" TEXT NOT NULL,
      "parent_workflow_name" TEXT NOT NULL,
      "parent_run_id" TEXT NOT NULL,
      "parent_revision" TEXT NOT NULL,
      "contract_hash" TEXT NOT NULL,
      "contract" JSONB NOT NULL,
      "framework_action_key" TEXT,
      "created_at" BIGINT NOT NULL,
      UNIQUE ("effect_key", "consumer_id"),
      FOREIGN KEY ("effect_key") REFERENCES ${effectTableName} ("effect_key") ON DELETE CASCADE,
      FOREIGN KEY ("effect_key", "consumer_id")
        REFERENCES ${receiptTableName} ("effect_key", "consumer_id") ON DELETE CASCADE,
      FOREIGN KEY ("receipt_key") REFERENCES ${receiptTableName} ("receipt_key") ON DELETE CASCADE,
      CHECK ("version" = 1),
      CHECK ("consumer_id" = 'mastra.parent-application.v1'),
      CHECK ("plan_key" ~ '^wtp:v1:[a-f0-9]{64}$'),
      CHECK ("plan_hash" ~ '^sha256:[a-f0-9]{64}$'),
      CHECK ("contract_hash" ~ '^sha256:[a-f0-9]{64}$'),
      CHECK (length("parent_revision") BETWEEN 1 AND 256),
      CHECK ((jsonb_typeof("contract") = 'object') IS TRUE),
      CHECK (("contract"->>'version' = '1') IS TRUE),
      CHECK (("contract"->>'contractHash' = "contract_hash") IS TRUE),
      CHECK (("contract"->>'executionMode' = 'continuous') IS TRUE),
      CHECK (("contract"->>'expectedParentRevision' = "parent_revision") IS TRUE),
      CHECK (("contract"->>'terminalEffectKey' = "effect_key") IS TRUE),
      CHECK (
        ((("contract"#>>'{action,kind}') IN ('wait', 'noop', 'quarantine')
          AND "framework_action_key" IS NULL) IS TRUE)
        OR ((("contract"#>>'{action,kind}') IN
          ('run-entry', 'complete-entry', 'fail-parent', 'finish-parent', 'cancel-parent', 'suspend-parent')
          AND "framework_action_key" ~ '^wta:v1:[a-f0-9]{64}$') IS TRUE)
      ),
      CHECK ("created_at" >= 0)
    );`;
  }

  getDefaultIndexDefinitions(): CreateIndexOptions[] {
    return WorkflowsPG.getDefaultIndexDefs(this.#schema);
  }

  /** Creates the terminalization recovery and retention indexes. */
  async createDefaultIndexes(): Promise<void> {
    if (this.#skipDefaultIndexes) {
      return;
    }
    for (const indexDef of this.getDefaultIndexDefinitions()) {
      try {
        await this.#db.createIndex(indexDef);
      } catch (error) {
        this.logger?.warn?.(`Failed to create workflow index ${indexDef.name}:`, error);
      }
    }
  }

  async init(): Promise<void> {
    await this.#db.createTable({ tableName: TABLE_WORKFLOW_SNAPSHOT, schema: TABLE_SCHEMAS[TABLE_WORKFLOW_SNAPSHOT] });
    await this.#db.client.none(WorkflowsPG.getTerminalizationTableDDL(this.#schema));
    await this.#db.client.none(WorkflowsPG.getTerminalEffectTableDDL(this.#schema));
    await this.#db.client.none(WorkflowsPG.getTerminalSnapshotTableDDL(this.#schema));
    await this.#db.client.none(WorkflowsPG.getTerminalRecoveryAncestryTableDDL(this.#schema));
    await this.#db.client.none(WorkflowsPG.getTerminalDestinationReceiptTableDDL(this.#schema));
    await this.#db.client.none(WorkflowsPG.getWorkflowParentRevisionTableDDL(this.#schema));
    await this.#db.client.tx(async t => {
      await t.none(
        `ALTER TABLE ${this.workflowParentRevisionTableName()}
         ADD COLUMN IF NOT EXISTS terminal_status TEXT
         CHECK (terminal_status IS NULL OR terminal_status IN ('success', 'failed', 'canceled', 'tripwire', 'bailed'))`,
      );
      await t.none(
        `INSERT INTO ${this.workflowParentRevisionTableName()}
         (workflow_name, run_id, generation, terminal_status, updated_at)
         SELECT identity.workflow_name, identity.run_id, 1,
           COALESCE(
             journal.terminal_status,
             CASE
               WHEN snapshot.snapshot->>'status' IN ('success', 'failed', 'canceled', 'tripwire', 'bailed')
                 THEN snapshot.snapshot->>'status'
               ELSE NULL
             END
           ),
           floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint
         FROM (
           SELECT workflow_name, run_id FROM ${this.workflowSnapshotTableName()}
           UNION
           SELECT workflow_name, run_id FROM ${this.terminalizationTableName()}
         ) AS identity
         LEFT JOIN ${this.terminalizationTableName()} AS journal
           ON journal.workflow_name = identity.workflow_name AND journal.run_id = identity.run_id
         LEFT JOIN ${this.workflowSnapshotTableName()} AS snapshot
           ON snapshot.workflow_name = identity.workflow_name AND snapshot.run_id = identity.run_id
         ON CONFLICT (workflow_name, run_id) DO NOTHING`,
      );
      const conflict = await t.one<{ conflict: boolean }>(
        `SELECT EXISTS (
           SELECT 1
           FROM ${this.workflowParentRevisionTableName()} AS revision
           LEFT JOIN ${this.terminalizationTableName()} AS journal
             ON journal.workflow_name = revision.workflow_name AND journal.run_id = revision.run_id
           LEFT JOIN ${this.workflowSnapshotTableName()} AS snapshot
             ON snapshot.workflow_name = revision.workflow_name AND snapshot.run_id = revision.run_id
           WHERE
             (journal.terminal_status IS NOT NULL
               AND snapshot.snapshot->>'status' IN ('success', 'failed', 'canceled', 'tripwire', 'bailed')
               AND journal.terminal_status <> snapshot.snapshot->>'status')
             OR (revision.terminal_status IS NOT NULL
               AND journal.terminal_status IS NOT NULL
               AND revision.terminal_status <> journal.terminal_status)
             OR (revision.terminal_status IS NOT NULL
               AND journal.terminal_status IS NULL
               AND snapshot.snapshot->>'status' IN ('success', 'failed', 'canceled', 'tripwire', 'bailed')
               AND revision.terminal_status <> snapshot.snapshot->>'status')
         ) AS conflict`,
      );
      if (conflict.conflict) {
        throw new TypeError('Workflow parent terminal marker conflicts with authoritative terminal evidence');
      }
      await t.none(
        `UPDATE ${this.workflowParentRevisionTableName()} AS revision
         SET terminal_status = journal.terminal_status
         FROM ${this.terminalizationTableName()} AS journal
         WHERE revision.workflow_name = journal.workflow_name
           AND revision.run_id = journal.run_id
           AND revision.terminal_status IS NULL`,
      );
      await t.none(
        `UPDATE ${this.workflowParentRevisionTableName()} AS revision
         SET terminal_status = snapshot.snapshot->>'status'
         FROM ${this.workflowSnapshotTableName()} AS snapshot
         WHERE revision.workflow_name = snapshot.workflow_name
           AND revision.run_id = snapshot.run_id
           AND revision.terminal_status IS NULL
           AND snapshot.snapshot->>'status' IN ('success', 'failed', 'canceled', 'tripwire', 'bailed')`,
      );
    });
    await this.#db.client.none(WorkflowsPG.getTerminalContinuationPlanTableDDL(this.#schema));
    await this.#db.alterTable({
      tableName: TABLE_WORKFLOW_SNAPSHOT,
      schema: TABLE_SCHEMAS[TABLE_WORKFLOW_SNAPSHOT],
      ifNotExists: ['resourceId'],
    });
    await this.createDefaultIndexes();
    await this.createCustomIndexes();
  }

  /**
   * Creates custom user-defined indexes for this domain's tables.
   */
  async createCustomIndexes(): Promise<void> {
    if (!this.#indexes || this.#indexes.length === 0) {
      return;
    }

    for (const indexDef of this.#indexes) {
      try {
        await this.#db.createIndex(indexDef);
      } catch (error) {
        // Log but continue - indexes are performance optimizations
        this.logger?.warn?.(`Failed to create custom index ${indexDef.name}:`, error);
      }
    }
  }

  async dangerouslyClearAll(): Promise<void> {
    await this.#db.client.none(
      `TRUNCATE TABLE ${this.terminalContinuationPlanTableName()}, ${this.terminalDestinationReceiptTableName()}, ${this.terminalEffectTableName()}, ${this.terminalSnapshotTableName()}, ${this.terminalRecoveryAncestryTableName()}, ${this.terminalizationTableName()}, ${this.workflowSnapshotTableName()}, ${this.workflowParentRevisionTableName()} CASCADE`,
    );
  }

  async bindWorkflowNestedRunOwnership(
    input: BindWorkflowNestedRunOwnershipInput,
  ): Promise<BindWorkflowNestedRunOwnershipResult> {
    const operation = {
      workflowName: input.workflowName,
      runId: input.runId,
      stepId: input.stepId,
      nestedRunId: input.nestedRunId,
      forEachIndex: input.forEachIndex,
      result: input.result,
      requestContext: input.requestContext,
    };
    validateWorkflowNestedRunOwnershipInput(operation);
    try {
      return await this.#db.client.tx(async t => {
        const tableName = this.workflowSnapshotTableName();
        const revisionLock = await this.lockWorkflowParentRevisionWithPresence(
          t,
          operation.workflowName,
          operation.runId,
        );
        const row = await t.oneOrNone<{ snapshot: WorkflowRunState }>(
          `SELECT snapshot FROM ${tableName} WHERE workflow_name = $1 AND run_id = $2 FOR UPDATE`,
          [operation.workflowName, operation.runId],
        );
        if (!row) {
          if (revisionLock.created) {
            await t.none(
              `DELETE FROM ${this.workflowParentRevisionTableName()}
               WHERE workflow_name = $1 AND run_id = $2`,
              [operation.workflowName, operation.runId],
            );
          }
          return { status: 'missing_run' };
        }
        const revision = revisionLock.generation;
        const snapshot = typeof row.snapshot === 'string' ? JSON.parse(row.snapshot) : row.snapshot;
        const ownership = bindWorkflowNestedRunOwnershipRecord(snapshot, operation);
        if (ownership.status === 'ownership_conflict') return ownership;
        if (ownership.status === 'already_bound') {
          return { status: 'already_bound', stepResults: ownership.snapshot.context };
        }
        const serialized = sanitizeJsonForPg(JSON.stringify(ownership.snapshot));
        const now = new Date();
        await t.none(
          `UPDATE ${tableName}
           SET snapshot = $1, "updatedAt" = $2, "updatedAtZ" = $3
           WHERE workflow_name = $4 AND run_id = $5`,
          [serialized, now, now, operation.workflowName, operation.runId],
        );
        await this.bumpWorkflowParentRevision(t, operation.workflowName, operation.runId, revision);
        return {
          status: 'bound',
          stepResults: ownership.snapshot.context,
        };
      });
    } catch (error) {
      throw new MastraError(
        {
          id: createStorageErrorId('PG', 'BIND_WORKFLOW_NESTED_RUN_OWNERSHIP', 'FAILED'),
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
          details: { workflowName: operation.workflowName, runId: operation.runId, stepId: operation.stepId },
        },
        error,
      );
    }
  }

  async admitWorkflowNestedRun(input: AdmitWorkflowNestedRunInput): Promise<AdmitWorkflowNestedRunResult> {
    const capturedInput = captureWorkflowNestedRunAdmissionInput(input);
    const requestedInitialChildSnapshot = capturedInput.initialChildSnapshot;
    const expectedChildGraphFingerprint = capturedInput.expectedChildGraphFingerprint;
    const initialChildSnapshot = validateWorkflowNestedRunInitialSnapshot(
      requestedInitialChildSnapshot,
      capturedInput.nestedRunId,
      expectedChildGraphFingerprint,
    );
    const serializedInitialChildSnapshot = initialChildSnapshot
      ? sanitizeJsonForPg(JSON.stringify(initialChildSnapshot.snapshot))
      : undefined;
    const requested = createWorkflowTerminalRecoveryAncestryRecord(
      capturedInput.nestedWorkflowName,
      capturedInput.nestedRunId,
      capturedInput.recoveryAncestry,
      0,
    );
    const operation = {
      workflowName: capturedInput.workflowName,
      runId: capturedInput.runId,
      stepId: capturedInput.stepId,
      nestedWorkflowName: requested.workflowName,
      nestedRunId: requested.runId,
      forEachIndex: capturedInput.forEachIndex,
      result: capturedInput.result,
      requestContext: capturedInput.requestContext,
      recoveryAncestry: requested.ancestry,
    };
    validateWorkflowNestedRunOwnershipInput(operation);
    validateWorkflowTerminalizationIdentity(operation.nestedWorkflowName, 'nestedWorkflowName', 512);

    try {
      return await this.#db.client.tx(async t => {
        const lockedIdentities = new Set([
          JSON.stringify([operation.nestedWorkflowName, operation.nestedRunId]),
          ...operation.recoveryAncestry.map(frame => JSON.stringify([frame.parentWorkflowName, frame.parentRunId])),
        ]);
        for (const identity of [...lockedIdentities].sort()) {
          await t.none(`SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, [identity]);
        }
        const clock = await t.one<{ now_ms: string }>(
          `SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint AS now_ms`,
        );
        const now = Number(clock.now_ms);
        if (!Number.isSafeInteger(now) || now < 0) throw new TypeError('Invalid PostgreSQL admission clock');
        const recovery = createWorkflowTerminalRecoveryAncestryRecord(
          operation.nestedWorkflowName,
          operation.nestedRunId,
          operation.recoveryAncestry,
          now,
        );
        const immediate = recovery.ancestry[0];
        const expectedSource =
          immediate &&
          immediate.parentWorkflowName === operation.workflowName &&
          immediate.parentRunId === operation.runId &&
          immediate.source.stepId === operation.stepId &&
          (operation.forEachIndex === undefined
            ? immediate.source.kind === 'step'
            : immediate.source.kind === 'foreach-iteration' &&
              immediate.source.iterationIndex === operation.forEachIndex);
        if (!expectedSource) return { status: 'ancestry_conflict' };

        const revisionLock = await this.lockWorkflowParentRevisionWithPresence(
          t,
          operation.workflowName,
          operation.runId,
        );
        const parentRow = await t.oneOrNone<{ snapshot: WorkflowRunState | string }>(
          `SELECT snapshot FROM ${this.workflowSnapshotTableName()}
           WHERE workflow_name = $1 AND run_id = $2 FOR UPDATE`,
          [operation.workflowName, operation.runId],
        );
        if (!parentRow) {
          if (revisionLock.created) {
            await t.none(
              `DELETE FROM ${this.workflowParentRevisionTableName()}
               WHERE workflow_name = $1 AND run_id = $2`,
              [operation.workflowName, operation.runId],
            );
          }
          return { status: 'missing_run' };
        }
        if (revisionLock.terminalStatus !== null) return { status: 'parent_terminal' };
        const snapshot = typeof parentRow.snapshot === 'string' ? JSON.parse(parentRow.snapshot) : parentRow.snapshot;
        try {
          validateWorkflowRunSnapshotShape(snapshot, operation.runId, 'Nested workflow parent snapshot');
        } catch {
          if (revisionLock.created) {
            await t.none(
              `DELETE FROM ${this.workflowParentRevisionTableName()}
               WHERE workflow_name = $1 AND run_id = $2 AND generation = 0 AND terminal_status IS NULL`,
              [operation.workflowName, operation.runId],
            );
          }
          return { status: 'parent_snapshot_conflict' };
        }
        if (isTerminalWorkflowRunStatus(snapshot.status)) {
          await this.latchWorkflowParentTerminalStatus(t, operation.workflowName, operation.runId, snapshot.status);
          return { status: 'parent_terminal' };
        }
        const revision = revisionLock.generation;
        validateWorkflowTerminalRecoveryParentFrameGraphBinding(immediate, snapshot.serializedStepGraph);

        const parentRecoveryRow = await t.oneOrNone<Record<string, unknown>>(
          `SELECT * FROM ${this.terminalRecoveryAncestryTableName()}
           WHERE workflow_name = $1 AND run_id = $2 FOR UPDATE`,
          [operation.workflowName, operation.runId],
        );
        const parentRecovery = parentRecoveryRow
          ? this.decodeTerminalRecoveryAncestryRow(parentRecoveryRow, now)
          : undefined;
        const expectedTailHash = createWorkflowTerminalRecoveryAncestryRecord(
          operation.workflowName,
          operation.runId,
          recovery.ancestry.slice(1),
          now,
        ).ancestryHash;
        const retainedTailHash = createWorkflowTerminalRecoveryAncestryRecord(
          operation.workflowName,
          operation.runId,
          parentRecovery?.ancestry ?? [],
          now,
        ).ancestryHash;
        if (expectedTailHash !== retainedTailHash) return { status: 'ancestry_conflict' };

        const childRecoveryRow = await t.oneOrNone<Record<string, unknown>>(
          `SELECT * FROM ${this.terminalRecoveryAncestryTableName()}
           WHERE workflow_name = $1 AND run_id = $2 FOR UPDATE`,
          [operation.nestedWorkflowName, operation.nestedRunId],
        );
        const existingRecovery = childRecoveryRow
          ? this.decodeTerminalRecoveryAncestryRow(childRecoveryRow, now)
          : undefined;
        if (existingRecovery && !sameWorkflowTerminalRecoveryAncestry(existingRecovery, recovery)) {
          return { status: 'ancestry_conflict' };
        }

        const ownership = bindWorkflowNestedRunOwnershipRecord(snapshot, operation);
        if (ownership.status === 'ownership_conflict') return ownership;
        if ((ownership.status === 'already_bound') !== Boolean(existingRecovery)) {
          return { status: 'ancestry_conflict' };
        }

        // The child revision is the serialization point shared with generic
        // snapshot writers. Inspect the retained row while that lock is held,
        // before mutating parent ownership or recovery ancestry.
        const childRevision = await this.lockWorkflowParentRevisionWithPresence(
          t,
          operation.nestedWorkflowName,
          operation.nestedRunId,
        );
        if (childRevision.terminalStatus !== null) return { status: 'child_terminal' };
        const childRow = await t.oneOrNone<{ snapshot: WorkflowRunState | string }>(
          `SELECT snapshot FROM ${this.workflowSnapshotTableName()}
           WHERE workflow_name = $1 AND run_id = $2 FOR UPDATE`,
          [operation.nestedWorkflowName, operation.nestedRunId],
        );
        let retainedChildSnapshot: WorkflowRunState | undefined;
        if (childRow) {
          try {
            retainedChildSnapshot =
              typeof childRow.snapshot === 'string' ? JSON.parse(childRow.snapshot) : childRow.snapshot;
          } catch {
            if (childRevision.created) {
              await t.none(
                `DELETE FROM ${this.workflowParentRevisionTableName()}
                 WHERE workflow_name = $1 AND run_id = $2 AND generation = 0 AND terminal_status IS NULL`,
                [operation.nestedWorkflowName, operation.nestedRunId],
              );
            }
            return { status: 'child_snapshot_conflict' };
          }
          const inspection = inspectWorkflowNestedRunRetainedSnapshot(
            retainedChildSnapshot,
            operation.nestedRunId,
            expectedChildGraphFingerprint,
          );
          if (inspection.status === 'conflict') {
            if (childRevision.created) {
              await t.none(
                `DELETE FROM ${this.workflowParentRevisionTableName()}
                 WHERE workflow_name = $1 AND run_id = $2 AND generation = 0 AND terminal_status IS NULL`,
                [operation.nestedWorkflowName, operation.nestedRunId],
              );
            }
            return { status: 'child_snapshot_conflict' };
          }
          if (inspection.status === 'terminal') {
            await this.latchWorkflowParentTerminalStatus(
              t,
              operation.nestedWorkflowName,
              operation.nestedRunId,
              inspection.terminalStatus,
            );
            return { status: 'child_terminal' };
          }
        }

        const ensureInitialChildSnapshot = async (): Promise<'initialized' | 'retained' | 'not_requested'> => {
          if (retainedChildSnapshot) {
            if (childRevision.created) {
              await this.bumpWorkflowParentRevision(
                t,
                operation.nestedWorkflowName,
                operation.nestedRunId,
                childRevision.generation,
              );
            }
            return initialChildSnapshot ? 'retained' : 'not_requested';
          }
          if (!initialChildSnapshot || serializedInitialChildSnapshot === undefined) {
            if (childRevision.created) {
              await t.none(
                `DELETE FROM ${this.workflowParentRevisionTableName()}
                 WHERE workflow_name = $1 AND run_id = $2 AND generation = 0 AND terminal_status IS NULL`,
                [operation.nestedWorkflowName, operation.nestedRunId],
              );
            }
            return 'not_requested';
          }
          const timestamp = new Date(now);
          await t.none(
            `INSERT INTO ${this.workflowSnapshotTableName()}
             (workflow_name, run_id, "resourceId", snapshot, "createdAt", "updatedAt", "createdAtZ", "updatedAtZ")
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
            [
              operation.nestedWorkflowName,
              operation.nestedRunId,
              initialChildSnapshot.resourceId ?? null,
              serializedInitialChildSnapshot,
              timestamp,
              timestamp,
              timestamp,
              timestamp,
            ],
          );
          await this.bumpWorkflowParentRevision(
            t,
            operation.nestedWorkflowName,
            operation.nestedRunId,
            childRevision.generation,
          );
          return 'initialized';
        };
        if (ownership.status === 'already_bound' && existingRecovery) {
          const childSnapshotState = await ensureInitialChildSnapshot();
          return {
            status: 'already_admitted',
            stepResults: ownership.snapshot.context,
            recovery: copyWorkflowTerminalRecoveryAncestryRecord(existingRecovery),
            childSnapshotState,
          };
        }
        const serialized = sanitizeJsonForPg(JSON.stringify(ownership.snapshot));
        const timestamp = new Date(now);
        await t.none(
          `INSERT INTO ${this.terminalRecoveryAncestryTableName()}
           (workflow_name, run_id, version, ancestry_hash, ancestry,
            immediate_parent_workflow_name, immediate_parent_run_id, created_at)
           VALUES ($1, $2, 1, $3, $4, $5, $6, $7)`,
          [
            recovery.workflowName,
            recovery.runId,
            recovery.ancestryHash,
            JSON.stringify(recovery.ancestry),
            immediate.parentWorkflowName,
            immediate.parentRunId,
            recovery.createdAt,
          ],
        );
        await t.none(
          `UPDATE ${this.workflowSnapshotTableName()}
           SET snapshot = $1, "updatedAt" = $2, "updatedAtZ" = $3
           WHERE workflow_name = $4 AND run_id = $5`,
          [serialized, timestamp, timestamp, operation.workflowName, operation.runId],
        );
        await this.bumpWorkflowParentRevision(t, operation.workflowName, operation.runId, revision);
        const childSnapshotState = await ensureInitialChildSnapshot();
        return {
          status: 'admitted',
          stepResults: ownership.snapshot.context,
          recovery: copyWorkflowTerminalRecoveryAncestryRecord(recovery),
          childSnapshotState,
        };
      });
    } catch (error) {
      return this.terminalizationError(
        'ADMIT_WORKFLOW_NESTED_RUN',
        operation.nestedWorkflowName,
        operation.nestedRunId,
        error,
      );
    }
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
    try {
      // Use a transaction with row-level locking to ensure atomicity
      return await this.#db.client.tx(async t => {
        const tableName = getTableName({ indexName: TABLE_WORKFLOW_SNAPSHOT, schemaName: getSchemaName(this.#schema) });
        const revision = await this.lockWorkflowParentRevision(t, workflowName, runId);

        // Load existing snapshot within transaction with FOR UPDATE to lock the row
        // This prevents concurrent updates from reading stale data
        const existingSnapshotResult = await t.oneOrNone<{ snapshot: WorkflowRunState }>(
          `SELECT snapshot FROM ${tableName} WHERE workflow_name = $1 AND run_id = $2 FOR UPDATE`,
          [workflowName, runId],
        );

        let snapshot: WorkflowRunState;
        if (!existingSnapshotResult) {
          // Create new snapshot if none exists
          snapshot = {
            context: {},
            activePaths: [],
            timestamp: Date.now(),
            suspendedPaths: {},
            activeStepsPath: {},
            resumeLabels: {},
            serializedStepGraph: [],
            status: 'pending',
            value: {},
            waitingPaths: {},
            runId: runId,
            requestContext: {},
          } as WorkflowRunState;
        } else {
          // Parse existing snapshot
          const existingSnapshot = existingSnapshotResult.snapshot;
          snapshot = typeof existingSnapshot === 'string' ? JSON.parse(existingSnapshot) : existingSnapshot;
        }

        // Merge the new step result using element-wise array merging
        // (critical for concurrent foreach iteration results)
        mergeWorkflowStepResult({ snapshot, stepId, result, requestContext });

        // Upsert the snapshot within the same transaction
        const now = new Date();
        const sanitizedSnapshot = sanitizeJsonForPg(JSON.stringify(snapshot));
        await t.none(
          `INSERT INTO ${tableName}
           (workflow_name, run_id, snapshot, "createdAt", "updatedAt", "createdAtZ", "updatedAtZ")
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (workflow_name, run_id) DO UPDATE
           SET snapshot = $3, "updatedAt" = $5, "updatedAtZ" = $7`,
          [workflowName, runId, sanitizedSnapshot, now, now, now, now],
        );
        await this.bumpWorkflowParentRevision(t, workflowName, runId, revision);

        return snapshot.context;
      });
    } catch (error) {
      throw new MastraError(
        {
          id: createStorageErrorId('PG', 'UPDATE_WORKFLOW_RESULTS', 'FAILED'),
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
          details: {
            workflowName,
            runId,
            stepId,
          },
        },
        error,
      );
    }
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
    try {
      // Use a transaction with row-level locking to ensure atomicity
      return await this.#db.client.tx(async t => {
        const tableName = getTableName({ indexName: TABLE_WORKFLOW_SNAPSHOT, schemaName: getSchemaName(this.#schema) });
        const revision = await this.lockWorkflowParentRevision(t, workflowName, runId);

        // Load existing snapshot within transaction with FOR UPDATE to lock the row
        // This prevents concurrent updates from reading stale data
        const existingSnapshotResult = await t.oneOrNone<{ snapshot: WorkflowRunState }>(
          `SELECT snapshot FROM ${tableName} WHERE workflow_name = $1 AND run_id = $2 FOR UPDATE`,
          [workflowName, runId],
        );

        if (!existingSnapshotResult) {
          return undefined;
        }

        // Parse existing snapshot
        const existingSnapshot = existingSnapshotResult.snapshot;
        const snapshot = typeof existingSnapshot === 'string' ? JSON.parse(existingSnapshot) : existingSnapshot;

        if (!snapshot || !snapshot?.context) {
          throw new Error(`Snapshot not found for runId ${runId}`);
        }

        // Merge the new options with the existing snapshot. A terminal
        // final-state write replaces both persisted state views under the same
        // row lock and uses the database clock for the workflow timestamp.
        const { finalState, ...stateOptions } = opts;
        const updatedSnapshot = { ...snapshot, ...stateOptions };
        if (finalState !== undefined) {
          const clock = await t.one<{ now_ms: string }>(
            `SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint AS now_ms`,
          );
          const storageTimestamp = Number(clock.now_ms);
          if (!Number.isSafeInteger(storageTimestamp) || storageTimestamp < 0) {
            throw new TypeError('Invalid PostgreSQL workflow state clock');
          }
          const finalTimestamp = validateWorkflowSnapshotTimestampForFinalState(snapshot.timestamp, storageTimestamp);
          const canonicalFinalState = materializeWorkflowTerminalCanonicalJsonObject(finalState, 'finalState');
          updatedSnapshot.context = { ...updatedSnapshot.context, __state: canonicalFinalState };
          updatedSnapshot.value = canonicalFinalState;
          updatedSnapshot.timestamp = finalTimestamp;
        }

        // Update the snapshot within the same transaction
        const sanitizedSnapshot = sanitizeJsonForPg(JSON.stringify(updatedSnapshot));
        const now = new Date();
        await t.none(
          `UPDATE ${tableName}
           SET snapshot = $1, "updatedAt" = $2, "updatedAtZ" = $3
           WHERE workflow_name = $4 AND run_id = $5`,
          [sanitizedSnapshot, now, now, workflowName, runId],
        );
        await this.bumpWorkflowParentRevision(t, workflowName, runId, revision);

        return updatedSnapshot;
      });
    } catch (error) {
      throw new MastraError(
        {
          id: createStorageErrorId('PG', 'UPDATE_WORKFLOW_STATE', 'FAILED'),
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
          details: {
            workflowName,
            runId,
          },
        },
        error,
      );
    }
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
    try {
      const now = new Date();
      const createdAtValue = createdAt ? createdAt : now;
      const updatedAtValue = updatedAt ? updatedAt : now;
      // Sanitize the snapshot JSON to remove problematic Unicode sequences
      const sanitizedSnapshot = sanitizeJsonForPg(JSON.stringify(snapshot));
      await this.#db.client.tx(async t => {
        const revision = await this.lockWorkflowParentRevision(t, workflowName, runId);
        await t.none(
          `INSERT INTO ${getTableName({ indexName: TABLE_WORKFLOW_SNAPSHOT, schemaName: getSchemaName(this.#schema) })}
                 (workflow_name, run_id, "resourceId", snapshot, "createdAt", "updatedAt", "createdAtZ", "updatedAtZ")
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                 ON CONFLICT (workflow_name, run_id) DO UPDATE
                 SET "resourceId" = $3, snapshot = $4, "updatedAt" = $6, "updatedAtZ" = $8`,
          [
            workflowName,
            runId,
            resourceId,
            sanitizedSnapshot,
            createdAtValue,
            updatedAtValue,
            createdAtValue,
            updatedAtValue,
          ],
        );
        await this.bumpWorkflowParentRevision(t, workflowName, runId, revision);
      });
    } catch (error) {
      throw new MastraError(
        {
          id: createStorageErrorId('PG', 'PERSIST_WORKFLOW_SNAPSHOT', 'FAILED'),
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
        },
        error,
      );
    }
  }

  async loadWorkflowSnapshot({
    workflowName,
    runId,
  }: {
    workflowName: string;
    runId: string;
  }): Promise<WorkflowRunState | null> {
    try {
      const result = await this.#db.load<{ snapshot: WorkflowRunState }>({
        tableName: TABLE_WORKFLOW_SNAPSHOT,
        keys: { workflow_name: workflowName, run_id: runId },
      });

      return result ? result.snapshot : null;
    } catch (error) {
      throw new MastraError(
        {
          id: createStorageErrorId('PG', 'LOAD_WORKFLOW_SNAPSHOT', 'FAILED'),
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
        },
        error,
      );
    }
  }

  async getWorkflowRunById({
    runId,
    workflowName,
  }: {
    runId: string;
    workflowName?: string;
  }): Promise<WorkflowRun | null> {
    try {
      const conditions: string[] = [];
      const values: any[] = [];
      let paramIndex = 1;

      if (runId) {
        conditions.push(`run_id = $${paramIndex}`);
        values.push(runId);
        paramIndex++;
      }

      if (workflowName) {
        conditions.push(`workflow_name = $${paramIndex}`);
        values.push(workflowName);
        paramIndex++;
      }

      const whereClause = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';

      const query = `
          SELECT * FROM ${getTableName({ indexName: TABLE_WORKFLOW_SNAPSHOT, schemaName: getSchemaName(this.#schema) })}
          ${whereClause}
          ORDER BY "createdAt" DESC LIMIT 1
        `;

      const queryValues = values;

      const result = await this.#db.client.oneOrNone(query, queryValues);

      if (!result) {
        return null;
      }

      return this.parseWorkflowRun(result);
    } catch (error) {
      throw new MastraError(
        {
          id: createStorageErrorId('PG', 'GET_WORKFLOW_RUN_BY_ID', 'FAILED'),
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
          details: {
            runId,
            workflowName: workflowName || '',
          },
        },
        error,
      );
    }
  }

  async deleteWorkflowRunById({ runId, workflowName }: { runId: string; workflowName: string }): Promise<void> {
    try {
      await this.#db.client.tx(async t => {
        const generation = await this.lockWorkflowParentRevision(t, workflowName, runId);
        const result = await t.query(
          `DELETE FROM ${getTableName({ indexName: TABLE_WORKFLOW_SNAPSHOT, schemaName: getSchemaName(this.#schema) })}
           WHERE run_id = $1 AND workflow_name = $2`,
          [runId, workflowName],
        );
        if ((result.rowCount ?? 0) > 0) {
          await this.bumpWorkflowParentRevision(t, workflowName, runId, generation);
        }
      });
    } catch (error) {
      throw new MastraError(
        {
          id: createStorageErrorId('PG', 'DELETE_WORKFLOW_RUN_BY_ID', 'FAILED'),
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
          details: {
            runId,
            workflowName,
          },
        },
        error,
      );
    }
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
    try {
      const conditions: string[] = [];
      const values: any[] = [];
      let paramIndex = 1;

      if (workflowName) {
        conditions.push(`workflow_name = $${paramIndex}`);
        values.push(workflowName);
        paramIndex++;
      }

      if (status) {
        // Use regexp_replace to strip problematic Unicode escape sequences before casting to jsonb.
        // PostgreSQL's jsonb cast fails on:
        // - \u0000 (null character) with error 22P05 "unsupported Unicode escape sequence"
        // - \uD800-\uDFFF (unpaired surrogates) with "Unicode low surrogate must follow a high surrogate"
        // The regex pattern matches \u0000 and all surrogate code points (D800-DFFF).
        // See: https://github.com/mastra-ai/mastra/issues/11563
        conditions.push(
          `regexp_replace(snapshot::text, '\\\\u(0000|[Dd][89A-Fa-f][0-9A-Fa-f]{2})', '', 'g')::jsonb ->> 'status' = $${paramIndex}`,
        );
        values.push(status);
        paramIndex++;
      }

      if (resourceId) {
        const hasResourceId = await this.#db.hasColumn(TABLE_WORKFLOW_SNAPSHOT, 'resourceId');
        if (hasResourceId) {
          conditions.push(`"resourceId" = $${paramIndex}`);
          values.push(resourceId);
          paramIndex++;
        } else {
          this.logger?.warn?.(`[${TABLE_WORKFLOW_SNAPSHOT}] resourceId column not found. Skipping resourceId filter.`);
        }
      }

      if (fromDate) {
        conditions.push(`"createdAt" >= $${paramIndex}`);
        values.push(fromDate);
        paramIndex++;
      }

      if (toDate) {
        conditions.push(`"createdAt" <= $${paramIndex}`);
        values.push(toDate);
        paramIndex++;
      }
      const whereClause = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';

      let total = 0;
      const usePagination = typeof perPage === 'number' && typeof page === 'number';
      if (usePagination) {
        const countResult = await this.#db.client.one(
          `SELECT COUNT(*) as count FROM ${getTableName({ indexName: TABLE_WORKFLOW_SNAPSHOT, schemaName: getSchemaName(this.#schema) })} ${whereClause}`,
          values,
        );
        total = Number(countResult.count);
      }

      const normalizedPerPage = usePagination ? normalizePerPage(perPage, Number.MAX_SAFE_INTEGER) : 0;
      const offset = usePagination ? page! * normalizedPerPage : undefined;

      const query = `
          SELECT * FROM ${getTableName({ indexName: TABLE_WORKFLOW_SNAPSHOT, schemaName: getSchemaName(this.#schema) })}
          ${whereClause}
          ORDER BY "createdAt" DESC
          ${usePagination ? ` LIMIT $${paramIndex} OFFSET $${paramIndex + 1}` : ''}
        `;

      const queryValues = usePagination ? [...values, normalizedPerPage, offset] : values;

      const result = await this.#db.client.manyOrNone(query, queryValues);

      const runs = (result || []).map(row => {
        return this.parseWorkflowRun(row);
      });

      return { runs, total: total || runs.length };
    } catch (error) {
      throw new MastraError(
        {
          id: createStorageErrorId('PG', 'LIST_WORKFLOW_RUNS', 'FAILED'),
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
          details: {
            workflowName: workflowName || 'all',
          },
        },
        error,
      );
    }
  }
}
