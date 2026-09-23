import { randomUUID } from 'node:crypto';

import { MastraError, ErrorDomain, ErrorCategory } from '@mastra/core/error';
import {
  DEFAULT_HARNESS_SESSION_RECORD_PROJECTION_MAX_PAYLOAD_BYTES,
  EXECUTION_CLOSURE_TABLES,
  OBSERVATIONAL_MEMORY_TABLE_SCHEMA,
  TABLE_CONFIGS,
  TABLE_HARNESS_ATTACHMENTS,
  TABLE_HARNESS_ATTACHMENT_REFERENCES,
  TABLE_HARNESS_CHANNEL_ACTION_RECEIPTS,
  TABLE_HARNESS_MESSAGE_RESULTS,
  TABLE_HARNESS_PLAN_TASKS,
  TABLE_HARNESS_SESSIONS,
  TABLE_HARNESS_SESSION_PROJECTION_FENCES,
  TABLE_HARNESS_SESSION_PROJECTION_INTENTS,
  TABLE_HARNESS_SESSION_PROJECTION_PRESSURE,
  TABLE_HARNESS_TERMINAL_ADMISSIONS,
  TABLE_HARNESS_TERMINAL_INTENTS,
  TABLE_HARNESS_TERMINAL_PRESSURE,
  TABLE_HARNESS_WORKSPACE_ACTIONS,
  TABLE_OBSERVATIONAL_MEMORY,
  TABLE_RESOURCES,
  TABLE_SCHEMAS,
  TABLE_THREADS,
  TABLE_THREAD_STATE,
  TABLE_WORKFLOW_SNAPSHOT,
  TABLE_WORKFLOW_SNAPSHOT_HANDOFF,
  buildExecutionClosureManifest,
  buildHarnessSessionRecordProjectionIntent,
  canonicalClosureRow,
  createStorageErrorId,
  verifyExecutionClosurePayload,
} from '@mastra/core/storage';
import type {
  ExecutionClosureImportResult,
  ExecutionClosureKey,
  ExecutionClosurePayload,
  ExecutionClosurePin,
  ExecutionClosureTableName,
} from '@mastra/core/storage';
import { parseSqlIdentifier } from '@mastra/core/utils';
import type { DbClient, TxClient } from './client';
import { getSchemaName, getTableName } from './db';
import { rowToSession } from './domains/harness';
import { parseJsonResilient } from './domains/utils';

export interface ExportExecutionClosureOptions {
  schemaName?: string;
}

type Dimension = 'session' | 'thread' | 'run' | 'resource';
type DimensionSets = Record<Dimension, Set<string>>;

function tableSql(table: ExecutionClosureTableName, schemaName?: string) {
  return getTableName({ indexName: table, schemaName });
}

function quoteIdent(name: string): string {
  return `"${parseSqlIdentifier(name, 'column name')}"`;
}

function hasHarnessName(table: ExecutionClosureTableName): boolean {
  const schema = (TABLE_SCHEMAS as Record<string, Record<string, unknown> | undefined>)[table];
  return schema !== undefined && 'harness_name' in schema;
}

function pairKeyOf(workflowName: unknown, runId: unknown): string | undefined {
  return typeof workflowName === 'string' && typeof runId === 'string' ? `${workflowName}\u0000${runId}` : undefined;
}

/**
 * Inverse of `pairKeyOf`. Postgres text cannot contain NUL, so the separator
 * split is always unambiguous even for workflow names containing spaces.
 */
function splitPairKey(pair: string): [workflowName: string, runId: string] {
  const idx = pair.indexOf('\u0000');
  return [pair.slice(0, idx), pair.slice(idx + 1)];
}

/** Scalar-shaped details bag accepted by {@link MastraError}. */
type ClosureErrorDetails = Record<string, null | boolean | number | string>;

function closureError(operation: string, reason: string, text: string, details: ClosureErrorDetails): MastraError {
  return new MastraError({
    id: createStorageErrorId('PG', operation, reason),
    domain: ErrorDomain.STORAGE,
    category: ErrorCategory.USER,
    text,
    details,
  });
}

/**
 * Columns of the run-scoped workflow terminal tables. They are declared by
 * this package's DDL (not `TABLE_SCHEMAS`), so the importer validates payload
 * row keys against them explicitly — a caller-supplied row must never inject
 * an identifier outside the registered schema.
 */
const WORKFLOW_TERMINAL_TABLE_COLUMNS: Record<string, readonly string[]> = {
  mastra_workflow_terminalizations: [
    'workflow_name',
    'run_id',
    'version',
    'event_key',
    'terminal_status',
    'phase',
    'owner_id',
    'claim_token',
    'claim_generation',
    'lease_expires_at',
    'created_at',
    'updated_at',
    'completed_at',
  ],
  mastra_workflow_terminal_effects_v2: [
    'workflow_name',
    'run_id',
    'effect_kind',
    'version',
    'effect_key',
    'source_event_key',
    'terminal_status',
    'parent_workflow_name',
    'parent_run_id',
    'parent_step_id',
    'parent_execution_path',
    'recovery_envelope_hash',
    'retained_record_hash',
    'resource_id',
    'payload_hash',
    'created_at',
  ],
  mastra_workflow_terminal_destination_receipts_v2: [
    'version',
    'workflow_name',
    'run_id',
    'effect_key',
    'consumer_id',
    'receipt_key',
    'effect_kind',
    'producer_payload_hash',
    'destination_hash',
    'application_state',
    'dispatch_state',
    'created_at',
    'updated_at',
    'applied_at',
    'dispatch_pending_at',
    'destination_applied_at',
    'quarantined_at',
  ],
  mastra_workflow_terminal_continuation_plans_v2: [
    'version',
    'plan_key',
    'plan_hash',
    'receipt_key',
    'effect_key',
    'consumer_id',
    'workflow_name',
    'run_id',
    'parent_workflow_name',
    'parent_run_id',
    'parent_revision',
    'contract_hash',
    'contract',
    'framework_action_key',
    'created_at',
  ],
  mastra_workflow_terminal_snapshots_v2: [
    'workflow_name',
    'run_id',
    'version',
    'resource_id',
    'terminal_status',
    'envelope_hash',
    'record_hash',
    'envelope',
    'created_at',
  ],
  mastra_workflow_terminal_recovery_ancestries: [
    'workflow_name',
    'run_id',
    'version',
    'ancestry_hash',
    'ancestry',
    'immediate_parent_workflow_name',
    'immediate_parent_run_id',
    'created_at',
  ],
  mastra_workflow_parent_revisions: ['workflow_name', 'run_id', 'generation', 'terminal_status', 'updated_at'],
};

/**
 * Primary keys that TABLE_SCHEMAS/TABLE_CONFIGS do not declare — the run-pair
 * workflow terminal tables plus the canonical snapshot, whose
 * (workflow_name, run_id) PK is declared by DDL only.
 */
const PRIMARY_KEY_OVERRIDES: Record<string, readonly string[]> = {
  mastra_workflow_snapshot: ['workflow_name', 'run_id'],
  mastra_workflow_terminalizations: ['workflow_name', 'run_id'],
  mastra_workflow_terminal_effects_v2: ['workflow_name', 'run_id', 'effect_kind'],
  mastra_workflow_terminal_destination_receipts_v2: ['effect_key', 'consumer_id'],
  mastra_workflow_terminal_continuation_plans_v2: ['receipt_key'],
  mastra_workflow_terminal_snapshots_v2: ['workflow_name', 'run_id'],
  mastra_workflow_terminal_recovery_ancestries: ['workflow_name', 'run_id'],
  mastra_workflow_parent_revisions: ['workflow_name', 'run_id'],
};

/**
 * Lifecycle columns a destination worker may legitimately advance between a
 * committed import and a lost-ack retry — claim/apply/ack transitions on the
 * same row. A retry that meets the row it imported earlier must converge:
 * identity and payload columns still compare canonically, so the same row's
 * forward progress is accepted while a foreign row colliding on the primary
 * key still fails closed. Tables whose rows are immutable once written
 * (tombstones, session events, run summaries, terminal lineage evidence) are
 * deliberately absent — any difference there remains a real conflict.
 */
const ADVANCEABLE_COLUMNS: Partial<Record<ExecutionClosureTableName, ReadonlySet<string>>> = {
  // Terminal worker lifecycle: pending -> claimed -> acked/dead.
  [TABLE_HARNESS_TERMINAL_INTENTS]: new Set([
    'status',
    'attempts',
    'claim_id',
    'claim_expires_at',
    'next_attempt_at',
    'last_error_json',
    'updated_at',
    'acked_at',
    'dead_at',
  ]),
  // Terminal admission lifecycle: pending -> committed/cancelled.
  [TABLE_HARNESS_TERMINAL_ADMISSIONS]: new Set([
    'status',
    'terminal_result_json',
    'projection_json',
    'revision',
    'updated_at',
  ]),
  // Projection pipeline lifecycle: pending -> claimed -> applied/failed/dead.
  // Applies to the intents the importer itself restages — payload intent rows
  // are `authority` and never reach this comparison. `created_at` is
  // advanceable too: the intent id is a sha256 over the session identity and
  // payload digest, so a worker-staged row at the same revision is provably
  // the same intent even though its staging timestamp differs.
  [TABLE_HARNESS_SESSION_PROJECTION_INTENTS]: new Set([
    'status',
    'attempts',
    'claim_id',
    'claim_expires_at',
    'next_attempt_at',
    'applied_at',
    'failed_at',
    'dead_at',
    'last_error',
    'created_at',
    'updated_at',
  ]),
  // The projection fence tracks the applied revision as the pipeline advances.
  [TABLE_HARNESS_SESSION_PROJECTION_FENCES]: new Set(['revision', 'state', 'updated_at']),
  // Message/signal evidence settles in place (pending -> completed/failed)
  // and a duplicate retry can still stamp run/dispatch/model fields.
  [TABLE_HARNESS_MESSAGE_RESULTS]: new Set([
    'status',
    'run_id',
    'mode_id',
    'model_id',
    'result',
    'error',
    'dispatch',
    'updated_at',
  ]),
  // Channel action receipts move through their own claim/apply lifecycle.
  [TABLE_HARNESS_CHANNEL_ACTION_RECEIPTS]: new Set([
    'status',
    'conflict_reason',
    'attempts',
    'claim_id',
    'claim_expires_at',
    'next_attempt_at',
    'accepted_at',
    'applied_at',
    'failed_at',
    'dead_at',
    'result',
    'last_error',
    'updated_at',
  ]),
  // A resumed run rewrites its canonical snapshot in place.
  [TABLE_WORKFLOW_SNAPSHOT]: new Set(['snapshot', 'updatedAt', 'updatedAtZ']),
  // A terminalizing run's owner/claim/phase columns advance as the claim
  // progresses; `claim_generation` is preserved on import so a live claim
  // legitimately differs on retry.
  mastra_workflow_terminalizations: new Set([
    'phase',
    'owner_id',
    'claim_token',
    'claim_generation',
    'lease_expires_at',
    'updated_at',
    'completed_at',
  ]),
  // Destination consumers advance receipt application/dispatch state.
  mastra_workflow_terminal_destination_receipts_v2: new Set([
    'application_state',
    'dispatch_state',
    'updated_at',
    'applied_at',
    'dispatch_pending_at',
    'destination_applied_at',
    'quarantined_at',
  ]),
  // The parent revision generation/terminal marker moves forward on commit.
  mastra_workflow_parent_revisions: new Set(['generation', 'terminal_status', 'updated_at']),
  // A resumed session touches its thread and mutable session-scoped state.
  [TABLE_THREADS]: new Set(['title', 'metadata', 'updatedAt', 'updatedAtZ']),
  [TABLE_THREAD_STATE]: new Set(['value', 'updatedAt', 'updatedAtZ']),
  // Plan tasks and workspace actions are mutable work/audit rows.
  [TABLE_HARNESS_PLAN_TASKS]: new Set([
    'status',
    'status_source',
    'content',
    'active_form',
    'priority',
    'blocked_by',
    'metadata',
    'updated_at',
    'started_at',
    'completed_at',
    'version',
    'order',
  ]),
  [TABLE_HARNESS_WORKSPACE_ACTIONS]: new Set(['result']),
  // Attachment byte identity settles through pending put operations.
  [TABLE_HARNESS_ATTACHMENTS]: new Set(['blob_ref', 'data_b64', 'put_operation_id', 'session_incarnation']),
  [TABLE_HARNESS_ATTACHMENT_REFERENCES]: new Set(['retained_until', 'session_incarnation']),
  // Observational memory advances continuously while the session runs — only
  // the row's identity/binding columns are stable across a retry.
  [TABLE_OBSERVATIONAL_MEMORY]: new Set([
    'activeObservations',
    'activeObservationsPendingUpdate',
    'generationCount',
    'lastObservedAt',
    'lastObservedAtZ',
    'lastReflectionAt',
    'lastReflectionAtZ',
    'pendingMessageTokens',
    'totalTokensObserved',
    'observationTokenCount',
    'isObserving',
    'isReflecting',
    'observedMessageIds',
    'observedTimezone',
    'bufferedObservations',
    'bufferedObservationTokens',
    'bufferedMessageIds',
    'bufferedReflection',
    'bufferedReflectionTokens',
    'bufferedReflectionInputTokens',
    'reflectedObservationLineCount',
    'bufferedObservationChunks',
    'isBufferingObservation',
    'isBufferingReflection',
    'lastBufferedAtTokens',
    'lastBufferedAtTime',
    'lastBufferedAtTimeZ',
    'metadata',
    'updatedAt',
    'updatedAtZ',
  ]),
};

/**
 * Session columns that anchor the row's creation identity. A stored session
 * may differ in every other column and still be the imported row advanced by
 * a destination worker — provided its version moved strictly forward. A row
 * whose stored version is not ahead is compared strictly instead, so a stale
 * or diverged row stays a conflict.
 */
const SESSION_IDENTITY_COLUMNS: ReadonlySet<string> = new Set([
  'harness_name',
  'id',
  'resource_id',
  'thread_id',
  'parent_session_id',
  'origin',
  'subagent_depth',
  'subagent_type_id',
  'subagent_tool_allowlist_scoped',
  'owns_thread',
  'created_at',
]);

const columnSchemaCache = new Map<string, Record<string, { type?: string; primaryKey?: boolean }> | undefined>();
function columnSchemaFor(table: ExecutionClosureTableName) {
  if (!columnSchemaCache.has(table)) {
    columnSchemaCache.set(
      table,
      (TABLE_SCHEMAS as Record<string, Record<string, { type?: string; primaryKey?: boolean }> | undefined>)[table] ??
        (
          OBSERVATIONAL_MEMORY_TABLE_SCHEMA as Record<
            string,
            Record<string, { type?: string; primaryKey?: boolean }> | undefined
          >
        )[table],
    );
  }
  return columnSchemaCache.get(table);
}

const allowedColumnsCache = new Map<string, ReadonlySet<string>>();
/**
 * The column names a payload row may carry for a table: the registered schema
 * plus the `*Z` timestamptz twins generated next to every timestamp column.
 */
function allowedColumnsFor(table: ExecutionClosureTableName): ReadonlySet<string> {
  const cached = allowedColumnsCache.get(table);
  if (cached) return cached;
  const cols = new Set<string>(WORKFLOW_TERMINAL_TABLE_COLUMNS[table] ?? []);
  const schema = columnSchemaFor(table);
  if (schema) {
    for (const [name, def] of Object.entries(schema)) {
      cols.add(name);
      if (def?.type === 'timestamp') cols.add(`${name}Z`);
    }
  }
  allowedColumnsCache.set(table, cols);
  return cols;
}

const primaryKeyCache = new Map<string, readonly string[]>();
function primaryKeyColumnsFor(table: ExecutionClosureTableName): readonly string[] {
  const cached = primaryKeyCache.get(table);
  if (cached) return cached;
  let pk: readonly string[] | undefined = PRIMARY_KEY_OVERRIDES[table];
  if (!pk) {
    const config = (TABLE_CONFIGS as Record<string, { compositePrimaryKey?: string[] } | undefined>)[table];
    if (config?.compositePrimaryKey) pk = config.compositePrimaryKey;
  }
  if (!pk) {
    const schema = columnSchemaFor(table);
    pk = Object.entries(schema ?? {})
      .filter(([, def]) => def?.primaryKey)
      .map(([name]) => name);
  }
  primaryKeyCache.set(table, pk);
  return pk;
}

/**
 * Canonical per-column comparison: the stored row equals the applied row when
 * every column the import wrote matches canonically. Columns the payload did
 * not carry (e.g. driver-defaulted `*Z` twins) cannot turn a retry into a
 * false conflict.
 */
function rowsMatch(stored: Record<string, unknown>, applied: Record<string, unknown>): boolean {
  const projection: Record<string, unknown> = {};
  for (const key of Object.keys(applied)) projection[key] = stored[key];
  return canonicalClosureRow(projection) === canonicalClosureRow(applied);
}

function rowsMatchExcept(
  stored: Record<string, unknown>,
  applied: Record<string, unknown>,
  excluded: ReadonlySet<string>,
): boolean {
  const storedProjection: Record<string, unknown> = {};
  const appliedProjection: Record<string, unknown> = {};
  for (const key of Object.keys(applied)) {
    if (excluded.has(key)) continue;
    storedProjection[key] = stored[key];
    appliedProjection[key] = applied[key];
  }
  return canonicalClosureRow(storedProjection) === canonicalClosureRow(appliedProjection);
}

/** Canonical comparison over a named column subset only. */
function columnsMatchOn(
  stored: Record<string, unknown>,
  applied: Record<string, unknown>,
  columns: ReadonlySet<string>,
): boolean {
  const storedProjection: Record<string, unknown> = {};
  const appliedProjection: Record<string, unknown> = {};
  for (const column of columns) {
    storedProjection[column] = stored[column];
    appliedProjection[column] = applied[column];
  }
  return canonicalClosureRow(storedProjection) === canonicalClosureRow(appliedProjection);
}

/** Regclass-resolvable name matching how `getTableName` qualifies references. */
function regclassName(table: string, schemaName: string | undefined): string {
  const parsedTable = parseSqlIdentifier(table, 'table name');
  // `schemaName` arrives already quoted by `getSchemaName` — reuse it verbatim.
  if (schemaName === undefined) return parsedTable;
  return `${schemaName}."${parsedTable}"`;
}

/** Qualified names of every registered closure table that does not resolve in this schema/search_path. */
async function missingClosureTables(
  t: TxClient,
  schemaName: string | undefined,
  tableNames: readonly string[],
): Promise<Set<string>> {
  const qualified = tableNames.map(name => regclassName(name, schemaName));
  const rows = await t.manyOrNone<{ name: string }>(
    `SELECT name FROM unnest($1::text[]) AS name WHERE to_regclass(name) IS NULL`,
    [qualified],
  );
  return new Set(rows.map(row => row.name));
}

/**
 * Read every registered closure row for one table. Rows bind through
 * OR-combined (column -> dimension) scope filters — each optionally gated by
 * a `when` discriminator — and harness tables also carry a `harness_name`
 * predicate. `harnessScoped` tables bind by namespace alone.
 */
async function readClosureTable(
  t: TxClient,
  table: ExecutionClosureTableName,
  schemaName: string | undefined,
  harnessName: string,
  dims: DimensionSets,
): Promise<Record<string, unknown>[]> {
  const spec = EXECUTION_CLOSURE_TABLES[table];
  if (!spec) return [];

  const conditions: string[] = [];
  const args: unknown[] = [];
  const push = (v: unknown) => `$${args.push(v)}`;

  if (hasHarnessName(table)) {
    conditions.push(`"harness_name" = ${push(harnessName)}`);
  }
  if (spec.scope.length > 0) {
    const ors = spec.scope
      .map(({ column, dimension, when }) => {
        const member = `${quoteIdent(column)} = ANY(${push([...dims[dimension]])}::text[])`;
        return when ? `(${quoteIdent(when.column)} = ${push(when.equals)} AND ${member})` : `(${member})`;
      })
      .join(' OR ');
    conditions.push(`(${ors})`);
  }
  // A registry entry with no scope filters and no harness_name column would
  // emit an unbounded SELECT — fail closed instead of dumping the whole table.
  if (conditions.length === 0) return [];
  const where = ` WHERE ${conditions.join(' AND ')}`;
  return t.manyOrNone<Record<string, unknown>>(`SELECT * FROM ${tableSql(table, schemaName)}${where}`, args);
}

/** Read the rows of a run-pair-scoped workflow table for exact (workflow, run) pairs. */
async function readRunPairTable(
  t: TxClient,
  table: ExecutionClosureTableName,
  schemaName: string | undefined,
  harnessName: string,
  pairs: ReadonlySet<string>,
): Promise<Record<string, unknown>[]> {
  const spec = EXECUTION_CLOSURE_TABLES[table];
  const pairScope = spec?.runPairScope;
  if (!spec || !pairScope || pairs.size === 0) return [];

  const conditions: string[] = [];
  const args: unknown[] = [];
  const push = (v: unknown) => `$${args.push(v)}`;
  if (hasHarnessName(table)) {
    conditions.push(`"harness_name" = ${push(harnessName)}`);
  }
  const ors = [...pairs]
    .map(pair => {
      const [workflowName, runId] = splitPairKey(pair);
      return `(${quoteIdent(pairScope.workflowNameColumn)} = ${push(workflowName)} AND ${quoteIdent(
        pairScope.runIdColumn,
      )} = ${push(runId)})`;
    })
    .join(' OR ');
  conditions.push(`(${ors})`);
  return t.manyOrNone<Record<string, unknown>>(
    `SELECT * FROM ${tableSql(table, schemaName)} WHERE ${conditions.join(' AND ')}`,
    args,
  );
}

/**
 * Discover which workflow names claim each candidate run id. Run ids are only
 * unique within a workflow, so the exporter reads every run-pair table by
 * run_id first, then binds rows to their exact (workflow_name, run_id) pair —
 * and pins run ids claimed by more than one workflow rather than exporting a
 * row set that could belong to a different workflow.
 */
async function readRunPairCandidates(
  t: TxClient,
  table: ExecutionClosureTableName,
  schemaName: string | undefined,
  harnessName: string,
  runIds: ReadonlySet<string>,
): Promise<Record<string, unknown>[]> {
  const spec = EXECUTION_CLOSURE_TABLES[table];
  const pairScope = spec?.runPairScope;
  if (!spec || !pairScope || runIds.size === 0) return [];

  const conditions: string[] = [];
  const args: unknown[] = [];
  const push = (v: unknown) => `$${args.push(v)}`;
  if (hasHarnessName(table)) {
    conditions.push(`"harness_name" = ${push(harnessName)}`);
  }
  conditions.push(`${quoteIdent(pairScope.runIdColumn)} = ANY(${push([...runIds])}::text[])`);
  return t.manyOrNone<Record<string, unknown>>(
    `SELECT * FROM ${tableSql(table, schemaName)} WHERE ${conditions.join(' AND ')}`,
    args,
  );
}

/** Parent (workflow, run) references carried by terminal lineage rows. */
function parentPairsOf(table: ExecutionClosureTableName, row: Record<string, unknown>): string[] {
  const out: string[] = [];
  const add = (workflowName: unknown, runId: unknown) => {
    const pair = pairKeyOf(workflowName, runId);
    if (pair) out.push(pair);
  };
  if (table === 'mastra_workflow_terminal_effects_v2' || table === 'mastra_workflow_terminal_continuation_plans_v2') {
    add(row.parent_workflow_name, row.parent_run_id);
  }
  if (table === 'mastra_workflow_terminal_recovery_ancestries') {
    add(row.immediate_parent_workflow_name, row.immediate_parent_run_id);
  }
  return out;
}

/**
 * Export one harness session subtree as a versioned closure payload.
 *
 * The whole read runs under `REPEATABLE READ`: every table observes one
 * consistent point-in-time, so a concurrent write or delete cannot interleave
 * into the export — the manifest always describes a self-consistent snapshot.
 * Historical IDs are preserved verbatim; re-allocating execution authority is
 * the importer's job, not the exporter's.
 *
 * Tables legitimately absent because their storage domain was disabled read
 * as empty sets. Unknown ancestry/ownership does not abort the export — it
 * records a pin so the unit can never be imported as `complete`.
 */
export async function exportExecutionClosure(
  db: DbClient,
  key: ExecutionClosureKey,
  options?: ExportExecutionClosureOptions,
): Promise<ExecutionClosurePayload> {
  const schemaName = getSchemaName(options?.schemaName);
  const sessionsTable = tableSql(TABLE_HARNESS_SESSIONS, schemaName);
  const tableNames = Object.keys(EXECUTION_CLOSURE_TABLES);

  return db.tx(async t => {
    // First statement in the transaction fixes the isolation level for the
    // whole export — a single snapshot across ~30 table reads.
    await t.none('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ');

    // --- Subtree enumeration: root + descendants via parent_session_id.
    // UNION (not UNION ALL) row-dedupes the frontier so a parentage cycle
    // terminates instead of recursing to the engine depth cap.
    const subtree = await t.manyOrNone<{ id: string }>(
      `WITH RECURSIVE subtree(id) AS (
         SELECT id FROM ${sessionsTable} WHERE harness_name = $1 AND id = $2
         UNION
         SELECT s.id FROM ${sessionsTable} s
         JOIN subtree ON s.parent_session_id = subtree.id
         WHERE s.harness_name = $1
       ) SELECT id FROM subtree`,
      [key.harnessName, key.sessionId],
    );
    if (subtree.length === 0) {
      throw closureError(
        'EXPORT_EXECUTION_CLOSURE',
        'ROOT_SESSION_NOT_FOUND',
        `Harness session ${key.sessionId} not found in namespace ${key.harnessName}`,
        { harnessName: key.harnessName, sessionId: key.sessionId },
      );
    }
    const sessionIds = subtree.map(r => r.id);

    const sessionRows = await t.manyOrNone<Record<string, unknown>>(
      `SELECT * FROM ${sessionsTable} WHERE harness_name = $1 AND id = ANY($2::text[])`,
      [key.harnessName, sessionIds],
    );

    // --- Dimensions from session rows.
    const dims: DimensionSets = {
      session: new Set(sessionIds),
      thread: new Set(),
      run: new Set(),
      resource: new Set(),
    };
    const incarnations: Record<string, string> = {};
    const pins: ExecutionClosurePin[] = [];

    // Runs whose durable id only survives on the session row: `current_run`
    // for the in-flight run and `pending_resume.runId` for a suspended one
    // (the runtime deliberately does not persist `currentRun` while parked).
    const sessionRunIds = new Map<string, Set<string>>();
    const collectSessionRun = (sessionId: string, runId: unknown) => {
      if (typeof runId !== 'string' || runId.length === 0) return;
      dims.run.add(runId);
      const set = sessionRunIds.get(sessionId) ?? new Set<string>();
      set.add(runId);
      sessionRunIds.set(sessionId, set);
    };

    for (const row of sessionRows) {
      const id = row.id as string;
      incarnations[id] = typeof row.session_incarnation === 'string' ? row.session_incarnation : '';
      if (typeof row.thread_id === 'string') dims.thread.add(row.thread_id);
      if (typeof row.resource_id === 'string') dims.resource.add(row.resource_id);
      const currentRun = parseJsonResilient(row.current_run) as { runId?: unknown } | undefined;
      collectSessionRun(id, currentRun?.runId);
      const pendingResume = parseJsonResilient(row.pending_resume) as { runId?: unknown } | undefined;
      collectSessionRun(id, pendingResume?.runId);
    }
    const rootRow = sessionRows.find(r => r.id === key.sessionId);
    if (rootRow && typeof rootRow.parent_session_id === 'string' && !dims.session.has(rootRow.parent_session_id)) {
      pins.push({
        reason: 'root-parent-outside-closure',
        detail: { sessionId: key.sessionId, parentSessionId: rootRow.parent_session_id },
      });
    }

    // Domains disabled on this store legitimately leave their tables absent —
    // probe once so a harness-only export reads them as empty sets instead of
    // failing on `relation does not exist`.
    const absentTables = await missingClosureTables(t, schemaName, tableNames);
    const tablePresent = (table: string) => !absentTables.has(regclassName(table, schemaName));

    // --- Session/thread/resource-scoped tables. Run-pair-scoped workflow
    // rows wait for the candidate run ids collected in this pass.
    const rows: Partial<Record<ExecutionClosureTableName, Record<string, unknown>[]>> = {
      [TABLE_HARNESS_SESSIONS]: sessionRows,
    };
    for (const [table, spec] of Object.entries(EXECUTION_CLOSURE_TABLES)) {
      const tableName = table as ExecutionClosureTableName;
      if (tableName === TABLE_HARNESS_SESSIONS || spec!.runPairScope) continue;
      rows[tableName] = tablePresent(tableName)
        ? await readClosureTable(t, tableName, schemaName, key.harnessName, dims)
        : [];
    }

    // --- Run ids: session refs plus every run_id column already read.
    for (const tableRows of Object.values(rows)) {
      for (const row of tableRows ?? []) {
        if (typeof row.run_id === 'string') dims.run.add(row.run_id);
      }
    }

    // --- Workflow run-pair discovery. Every run-pair table is probed by
    // run_id, then rows bind to their exact (workflow_name, run_id) pair. A
    // run id claimed by more than one workflow cannot be attributed safely —
    // drop it from every table and pin instead of exporting a foreign row.
    const runWorkflows = new Map<string, Set<string>>();
    const pairCandidates = new Map<ExecutionClosureTableName, Record<string, unknown>[]>();
    for (const [table, spec] of Object.entries(EXECUTION_CLOSURE_TABLES)) {
      const tableName = table as ExecutionClosureTableName;
      if (!spec!.runPairScope) continue;
      const tableRows = tablePresent(tableName)
        ? await readRunPairCandidates(t, tableName, schemaName, key.harnessName, dims.run)
        : [];
      pairCandidates.set(tableName, tableRows);
      for (const row of tableRows) {
        const pair = pairKeyOf(row.workflow_name, row.run_id);
        if (!pair) continue;
        const [workflowName, runId] = splitPairKey(pair);
        const names = runWorkflows.get(runId) ?? new Set<string>();
        names.add(workflowName);
        runWorkflows.set(runId, names);
      }
    }
    const ambiguousRuns = new Set<string>();
    const runPairs = new Set<string>();
    for (const [runId, workflowNames] of runWorkflows) {
      if (workflowNames.size > 1) {
        ambiguousRuns.add(runId);
        pins.push({
          reason: 'workflow-run-ambiguous',
          detail: { runId, workflowNames: [...workflowNames].sort() },
        });
        continue;
      }
      runPairs.add(`${[...workflowNames][0]!}\u0000${runId}`);
    }
    for (const [tableName, tableRows] of pairCandidates) {
      rows[tableName] = tableRows.filter(row => {
        const pair = pairKeyOf(row.workflow_name, row.run_id);
        return pair !== undefined && runPairs.has(pair);
      });
    }

    // --- Parent workflow lineage. Terminal effects, recovery ancestries, and
    // continuation plans reference parent runs that can live outside the
    // session's own run set; traverse them until the referenced closure is
    // closed, then pin the parents whose snapshot could not be carried.
    const referencedParentPairs = new Set<string>();
    let frontier = new Set<string>();
    const collectParentRefs = () => {
      for (const [tableName, tableRows] of Object.entries(rows)) {
        const name = tableName as ExecutionClosureTableName;
        for (const row of tableRows ?? []) {
          for (const pair of parentPairsOf(name, row)) {
            referencedParentPairs.add(pair);
            if (!runPairs.has(pair)) frontier.add(pair);
          }
        }
      }
    };
    collectParentRefs();
    while (frontier.size > 0) {
      const wave = frontier;
      frontier = new Set<string>();
      for (const pair of wave) runPairs.add(pair);
      for (const [table, spec] of Object.entries(EXECUTION_CLOSURE_TABLES)) {
        const tableName = table as ExecutionClosureTableName;
        if (!spec!.runPairScope || !tablePresent(tableName)) continue;
        const waveRows = await readRunPairTable(t, tableName, schemaName, key.harnessName, wave);
        if (waveRows.length > 0) {
          rows[tableName] = [...(rows[tableName] ?? []), ...waveRows];
        }
      }
      collectParentRefs();
    }
    for (const pair of referencedParentPairs) {
      const [, runId] = splitPairKey(pair);
      dims.run.add(runId);
    }
    const snapshotPairs = new Set(
      (rows[TABLE_WORKFLOW_SNAPSHOT] ?? [])
        .map(row => pairKeyOf(row.workflow_name, row.run_id))
        .filter((pair): pair is string => pair !== undefined),
    );
    // A snapshot-handoff row carries the run's complete snapshot itself —
    // import materializes it as canonical state rather than restoring the
    // fence — so a handoff-backed pair proves the parent run just as an
    // exported canonical snapshot row does.
    for (const row of rows[TABLE_WORKFLOW_SNAPSHOT_HANDOFF] ?? []) {
      const pair = pairKeyOf(row.workflow_name, row.run_id);
      if (pair) snapshotPairs.add(pair);
    }
    for (const pair of referencedParentPairs) {
      if (snapshotPairs.has(pair)) continue;
      const [workflowName, runId] = splitPairKey(pair);
      pins.push({
        reason: 'workflow-parent-run-missing',
        detail: { workflowName, runId },
      });
    }

    // --- Pins: unknown ancestry/ownership marks the unit incomplete instead of
    // silently exporting a broken continuation.
    const threadRows = new Set((rows[TABLE_THREADS] ?? []).map(r => r.id));
    const resourceRows = new Set((rows[TABLE_RESOURCES] ?? []).map(r => r.id));
    const snapshotRunIds = new Set((rows[TABLE_WORKFLOW_SNAPSHOT] ?? []).map(r => r.run_id));
    for (const row of sessionRows) {
      const threadId = row.thread_id;
      if (typeof threadId === 'string' && !threadRows.has(threadId)) {
        pins.push({
          reason: 'session-thread-missing',
          detail: { sessionId: row.id, threadId },
        });
      }
      const resourceId = row.resource_id;
      if (typeof resourceId === 'string' && !resourceRows.has(resourceId)) {
        pins.push({
          reason: 'session-resource-missing',
          detail: { sessionId: row.id, resourceId },
        });
      }
      for (const runId of sessionRunIds.get(row.id as string) ?? []) {
        if (ambiguousRuns.has(runId)) continue; // ambiguity already pinned
        if (!snapshotRunIds.has(runId)) {
          pins.push({
            reason: 'current-run-without-snapshot',
            detail: { sessionId: row.id, runId },
          });
        }
      }
    }

    // Attachment rows that reference externally-owned bytes (`blob_ref`) are
    // metadata without usable bytes at the destination: `loadAttachment`
    // resolves bytes through the destination byte owner with the stored
    // reference and incarnation — it never reads `data_b64` — and import
    // neither uploads retained inline bytes nor rewrites the reference, so a
    // destination without the shared source provider loses the bytes either
    // way. Pin every blob-backed attachment rather than claiming an
    // apparently complete but unloadable row.
    for (const row of rows[TABLE_HARNESS_ATTACHMENTS] ?? []) {
      const hasExternalRef = typeof row.blob_ref === 'string' && row.blob_ref.length > 0;
      if (hasExternalRef) {
        pins.push({
          reason: 'attachment-bytes-external',
          detail: { sessionId: row.session_id, attachmentId: row.attachment_id },
        });
      }
    }

    // A pending message-result whose dispatch marker is `dispatching` or
    // `accepted` means provider side effects may already have executed — the
    // runtime deliberately never auto-replays those states, so the imported
    // row would wait forever for a live run that only exists on the source.
    // The same ambiguity applies to a legacy pending row carrying a run id
    // with no dispatch marker (treated as accepted-equivalent). `reserved`
    // and marker-less pending rows are provably undispatched and re-drive
    // safely, so only the ambiguous states pin the closure.
    for (const row of rows[TABLE_HARNESS_MESSAGE_RESULTS] ?? []) {
      if (row.status !== 'pending') continue;
      const dispatch = parseJsonResilient(row.dispatch) as { state?: unknown } | undefined;
      const dispatchState = typeof dispatch?.state === 'string' ? dispatch.state : undefined;
      const ambiguous =
        dispatchState === 'dispatching' ||
        dispatchState === 'accepted' ||
        (dispatchState === undefined && typeof row.run_id === 'string' && row.run_id.length > 0);
      if (ambiguous) {
        pins.push({
          reason: 'in-flight-dispatch',
          detail: {
            sessionId: row.session_id,
            signalId: row.signal_id,
            ...(typeof row.run_id === 'string' && row.run_id.length > 0 ? { runId: row.run_id } : {}),
            dispatchState: dispatchState ?? 'accepted',
          },
        });
      }
    }

    const manifest = buildExecutionClosureManifest({
      key,
      source: { store: 'pg', schemaName: options?.schemaName ?? 'public' },
      sessionIds,
      incarnations,
      threadIds: [...dims.thread],
      runIds: [...dims.run],
      resourceIds: [...dims.resource],
      rows,
      pins,
    });
    return { manifest, rows };
  });
}

export interface ImportExecutionClosureOptions {
  schemaName?: string;
}

const LIVE_TERMINAL_INTENT_STATUSES = new Set(['pending', 'claimed', 'failed']);

/**
 * Rows carrying a `harness_name` must belong to the manifest's namespace —
 * the payload was scoped by it at export, so a foreign namespace row means the
 * closure was tampered with rather than built by this exporter.
 */
function assertRowHarnessName(
  tableName: ExecutionClosureTableName,
  applied: Record<string, unknown>,
  harnessName: string,
): void {
  if ('harness_name' in applied && applied.harness_name !== harnessName) {
    throw closureError(
      'IMPORT_EXECUTION_CLOSURE',
      'HARNESS_NAME_MISMATCH',
      `Execution closure row for ${tableName} carries harness_name ${String(applied.harness_name)} outside the manifest namespace`,
      { tableName, harnessName: String(applied.harness_name), expected: harnessName },
    );
  }
}

/**
 * BIGINT columns of the run-pair workflow terminal tables. Their DDL lives in
 * this package (not `TABLE_SCHEMAS`), so the bigint normalization below cannot
 * reach them through `columnSchemaFor`.
 */
const WORKFLOW_TERMINAL_BIGINT_COLUMNS: Record<string, ReadonlySet<string>> = {
  mastra_workflow_terminalizations: new Set([
    'claim_generation',
    'lease_expires_at',
    'created_at',
    'updated_at',
    'completed_at',
  ]),
  mastra_workflow_terminal_effects_v2: new Set(['created_at']),
  mastra_workflow_terminal_destination_receipts_v2: new Set([
    'created_at',
    'updated_at',
    'applied_at',
    'dispatch_pending_at',
    'destination_applied_at',
    'quarantined_at',
  ]),
  mastra_workflow_terminal_continuation_plans_v2: new Set(['created_at']),
  mastra_workflow_terminal_snapshots_v2: new Set(['created_at']),
  mastra_workflow_terminal_recovery_ancestries: new Set(['created_at']),
  mastra_workflow_parent_revisions: new Set(['generation', 'updated_at']),
};

/** Columns stripped of live authority before a row is applied. */
function applyImportTransforms(
  tableName: ExecutionClosureTableName,
  row: Record<string, unknown>,
  incarnations: Record<string, string>,
): Record<string, unknown> {
  const spec = EXECUTION_CLOSURE_TABLES[tableName]!;
  const applied: Record<string, unknown> = { ...row };
  for (const column of spec.clearOnImport ?? []) {
    applied[column] = null;
  }
  // The pg driver returns int8 as text, so an exported payload already carries
  // bigint values as strings. A hand-built payload may carry them as numbers —
  // normalize so the canonical read-back comparison on a retry sees the same
  // value the driver returns ('5' === '5', not 5 !== '5').
  const schema = columnSchemaFor(tableName);
  if (schema) {
    for (const [column, def] of Object.entries(schema)) {
      if (def?.type === 'bigint' && typeof applied[column] === 'number') {
        applied[column] = String(applied[column]);
      }
    }
  }
  for (const column of WORKFLOW_TERMINAL_BIGINT_COLUMNS[tableName] ?? []) {
    if (typeof applied[column] === 'number') applied[column] = String(applied[column]);
  }
  const requeue = spec.requeueOnImport;
  if (requeue && requeue.from.includes(applied[requeue.statusColumn] as string)) {
    applied[requeue.statusColumn] = requeue.to;
  }
  if (spec.role === 'fence') {
    const sessionId = applied.session_id;
    if (typeof sessionId === 'string' && typeof applied.session_incarnation === 'string') {
      const destination = incarnations[sessionId];
      if (destination) applied.session_incarnation = destination;
    }
  }
  return applied;
}

/** Reject payload row keys outside the table's registered schema. */
function assertClosureRowColumns(tableName: ExecutionClosureTableName, applied: Record<string, unknown>): string[] {
  const allowed = allowedColumnsFor(tableName);
  const columns = Object.keys(applied);
  for (const column of columns) {
    if (!allowed.has(column)) {
      throw closureError(
        'IMPORT_EXECUTION_CLOSURE',
        'INVALID_ROW_COLUMN',
        `Execution closure payload carries column ${column} outside the registered schema for ${tableName}`,
        { tableName, column },
      );
    }
  }
  return columns;
}

/**
 * Serialize a bound value for its column. node-pg writes JS arrays as
 * Postgres array literals, so `[]` lands in a `jsonb` column as `{}` — every
 * composite column in the closure registry is JSONB, so objects and arrays
 * are always sent as JSON text. `Date`/`Buffer` pass through to the driver.
 */
function bindClosureValue(table: ExecutionClosureTableName, column: string, value: unknown): unknown {
  if (value === null || value === undefined) return null;
  const declared = columnSchemaFor(table)?.[column]?.type;
  if (declared === 'jsonb') return JSON.stringify(value);
  if (
    typeof value === 'object' &&
    !(value instanceof Date) &&
    !Buffer.isBuffer(value) &&
    !(value instanceof Uint8Array)
  ) {
    return JSON.stringify(value);
  }
  return value;
}

/**
 * Insert one applied row and prove that any conflicting row is this import's
 * own earlier result. `ON CONFLICT DO NOTHING` alone cannot distinguish an
 * idempotent retry from a foreign collision that would silently merge a
 * different row into the closure — so a skipped insert reads the row back by
 * primary key and aborts unless it matches canonically.
 *
 * `shared-resource` rows pass `verifyConflict: false`: a live shared row is
 * authoritative regardless of content, so any conflict simply keeps it.
 */
async function insertClosureRow(
  t: TxClient,
  tableName: ExecutionClosureTableName,
  schemaName: string | undefined,
  applied: Record<string, unknown>,
  options?: { verifyConflict?: boolean },
): Promise<'inserted' | 'skipped'> {
  const columns = assertClosureRowColumns(tableName, applied);
  const result = await t.query(
    `INSERT INTO ${tableSql(tableName, schemaName)} (${columns.map(quoteIdent).join(', ')})
     VALUES (${columns.map((_, i) => `$${i + 1}`).join(', ')})
     ON CONFLICT DO NOTHING`,
    columns.map(c => bindClosureValue(tableName, c, applied[c])),
  );
  if ((result.rowCount ?? 0) > 0) return 'inserted';
  if (options?.verifyConflict === false) return 'skipped';

  const pk = primaryKeyColumnsFor(tableName);
  const stored =
    pk.length > 0
      ? await t.manyOrNone<Record<string, unknown>>(
          `SELECT * FROM ${tableSql(tableName, schemaName)}
           WHERE ${pk.map((column, i) => `${quoteIdent(column)} = $${i + 1}`).join(' AND ')}`,
          pk.map(column => applied[column] ?? null),
        )
      : [];
  if (stored.length === 1 && rowsMatch(stored[0]!, applied)) return 'skipped';
  // A lost-ack retry can meet the row it imported after a destination worker
  // already advanced it — claimed or settled an intent, applied a restaged
  // projection intent, delivered a receipt. Identity and payload columns must
  // still match canonically; only the registered lifecycle columns may
  // differ, so the same row's forward progress converges while a foreign row
  // still fails closed.
  const advanceable = ADVANCEABLE_COLUMNS[tableName];
  if (advanceable !== undefined && stored.length === 1 && rowsMatchExcept(stored[0]!, applied, advanceable)) {
    return 'skipped';
  }
  throw closureError(
    'IMPORT_EXECUTION_CLOSURE',
    'DESTINATION_ROW_CONFLICT',
    `Execution closure row for ${tableName} conflicts with a different destination row`,
    { tableName, primaryKey: pk.join(', ') },
  );
}

/**
 * Stage and apply an exported execution closure.
 *
 * Verification is the staging gate: a corrupt, incomplete, or unsupported
 * payload fails closed before a single row is written. The apply runs in one
 * transaction, so a crash either commits nothing or the whole unit — there is
 * no partially-applied state to reconcile, and a lost acknowledgement can
 * simply re-import.
 *
 * Authority is re-allocated, never restored:
 * - every imported session row receives a `session_incarnation` — reusing the
 *   persisted one on retry, persisting a fresh one onto legacy rows, and
 *   adopting the winner's after a concurrent import — while `owner_id` and
 *   `lease_expires_at` are cleared, so an old lease or in-flight callback
 *   under the exported incarnation is fenced by the runtime;
 * - a destination row that already exists must equal the row this import
 *   would write — or be that same row legitimately advanced by a destination
 *   worker (claimed/acked/settled lifecycle columns on a strictly newer
 *   session version) — otherwise the transaction aborts instead of silently
 *   merging a foreign row into the closure;
 * - `authority` rows (wakeups, outbox, inbox, tokens, bindings, thread-delete
 *   leases, projection intents, pending attachment operations, pressure
 *   counters) are counted as skipped — the runtime re-establishes its own;
 * - `fence` rows are rewritten onto the destination session incarnation so
 *   tombstones and paid attempts still apply to the session that was stored;
 * - live terminal intents and channel receipts lose their stale source claim
 *   so a destination worker can claim them under a fresh claim id;
 * - pending session-record projection work is restaged from the imported
 *   session rows, and the terminal/projection pressure counters are rebuilt
 *   from the live rows actually restored;
 * - `shared-resource` rows insert only when absent — an archive can never
 *   overwrite a live shared resource row.
 */
export async function importExecutionClosure(
  db: DbClient,
  payload: ExecutionClosurePayload,
  options?: ImportExecutionClosureOptions,
): Promise<ExecutionClosureImportResult> {
  const { manifest, rows } = payload;

  const verified = verifyExecutionClosurePayload(manifest, rows);
  if (!verified.ok) {
    throw closureError(
      'IMPORT_EXECUTION_CLOSURE',
      'PAYLOAD_MISMATCH',
      `Execution closure payload failed manifest verification: ${verified.mismatches.join('; ')}`,
      { mismatchCount: verified.mismatches.length },
    );
  }

  const schemaName = getSchemaName(options?.schemaName);
  const harnessName = manifest.key.harnessName;
  const sessionsTable = tableSql(TABLE_HARNESS_SESSIONS, schemaName);
  const incarnationColumn = 'session_incarnation';

  return db.tx(async t => {
    const tableNames = Object.keys(EXECUTION_CLOSURE_TABLES);
    const absentTables = await missingClosureTables(t, schemaName, tableNames);
    const tablePresent = (table: string) => !absentTables.has(regclassName(table, schemaName));
    const requireTable = (table: ExecutionClosureTableName, rowCount: number) => {
      if (rowCount > 0 && !tablePresent(table)) {
        throw closureError(
          'IMPORT_EXECUTION_CLOSURE',
          'MISSING_TABLE',
          `Execution closure table ${table} does not exist — enable its storage domain before importing`,
          { tableName: table, schemaName: schemaName ?? 'search_path' },
        );
      }
    };

    const inserted: Record<string, number> = {};
    const skipped: Record<string, number> = {};
    for (const tableName of tableNames) {
      inserted[tableName] = 0;
      skipped[tableName] = 0;
    }

    // --- Sessions first: the destination incarnation every fence row rebinds
    // to is decided here. Locking the existing rows serializes concurrent
    // imports of the same closure so all of them converge on one incarnation.
    const sessionRows = rows[TABLE_HARNESS_SESSIONS] ?? [];
    requireTable(TABLE_HARNESS_SESSIONS, sessionRows.length);
    const sessionById = new Map(sessionRows.map(row => [String(row.id), row]));
    const existingSessions = manifest.sessionIds.length
      ? await t.manyOrNone<Record<string, unknown>>(
          `SELECT * FROM ${sessionsTable} WHERE harness_name = $1 AND id = ANY($2::text[]) FOR UPDATE`,
          [harnessName, manifest.sessionIds],
        )
      : [];
    const existingById = new Map(existingSessions.map(row => [String(row.id), row]));

    const incarnations: Record<string, string> = {};
    const appliedSessionRows = new Map<string, Record<string, unknown>>();
    const incarnationExcluded = new Set([incarnationColumn]);

    for (const sessionId of manifest.sessionIds) {
      const sourceRow = sessionById.get(sessionId);
      if (!sourceRow) {
        throw closureError(
          'IMPORT_EXECUTION_CLOSURE',
          'SESSION_METADATA_MISMATCH',
          `Manifest sessionIds entry ${sessionId} has no exported session row`,
          { sessionId },
        );
      }
      const base = applyImportTransforms(TABLE_HARNESS_SESSIONS, sourceRow, incarnations);
      assertRowHarnessName(TABLE_HARNESS_SESSIONS, base, harnessName);
      const stored = existingById.get(sessionId);
      if (stored) {
        // A pre-existing row must be this import's earlier result (or a
        // legacy row carrying identical content): the incarnation itself is
        // the only field the import is allowed to rewrite.
        const identical = rowsMatchExcept(stored, base, incarnationExcluded);
        // A lost-ack retry can instead meet the session after a destination
        // worker advanced it — the same creation identity at a strictly newer
        // version. The stored row is the newer truth, so the retry converges
        // and restages from it; a regressed or diverged row stays a conflict.
        const advanced =
          !identical &&
          Number(stored.version) > Number(base.version) &&
          columnsMatchOn(stored, base, SESSION_IDENTITY_COLUMNS);
        if (!identical && !advanced) {
          throw closureError(
            'IMPORT_EXECUTION_CLOSURE',
            'DESTINATION_ROW_CONFLICT',
            `Harness session ${sessionId} already exists with different content`,
            { tableName: TABLE_HARNESS_SESSIONS, sessionId },
          );
        }
        let incarnation =
          typeof stored.session_incarnation === 'string' && stored.session_incarnation.length > 0
            ? stored.session_incarnation
            : undefined;
        if (incarnation === undefined) {
          // Legacy row mint: the FOR UPDATE lock above makes this update the
          // single writer, so a concurrent import cannot mint a second value.
          incarnation = randomUUID();
          await t.none(`UPDATE ${sessionsTable} SET session_incarnation = $1 WHERE harness_name = $2 AND id = $3`, [
            incarnation,
            harnessName,
            sessionId,
          ]);
          stored.session_incarnation = incarnation;
        }
        incarnations[sessionId] = incarnation;
        // Restage from the row actually persisted — for an advanced session
        // that is the stored row, not the older payload image.
        appliedSessionRows.set(sessionId, stored);
        skipped[TABLE_HARNESS_SESSIONS]! += 1;
        continue;
      }

      const minted = randomUUID();
      const applied: Record<string, unknown> = { ...base, session_incarnation: minted };
      appliedSessionRows.set(sessionId, applied);
      const columns = assertClosureRowColumns(TABLE_HARNESS_SESSIONS, applied);
      const result = await t.query(
        `INSERT INTO ${sessionsTable} (${columns.map(quoteIdent).join(', ')})
         VALUES (${columns.map((_, i) => `$${i + 1}`).join(', ')})
         ON CONFLICT DO NOTHING`,
        columns.map(c => bindClosureValue(TABLE_HARNESS_SESSIONS, c, applied[c])),
      );
      if ((result.rowCount ?? 0) > 0) {
        inserted[TABLE_HARNESS_SESSIONS]! += 1;
        incarnations[sessionId] = minted;
        continue;
      }
      // A raced insert lost to a concurrent import that committed its own
      // incarnation between our FOR UPDATE read and this insert: adopt the
      // persisted winner when the rest of the row is this same session, so
      // the returned incarnation is always the one actually stored.
      const winner = await t.manyOrNone<Record<string, unknown>>(
        `SELECT * FROM ${sessionsTable} WHERE harness_name = $1 AND id = $2`,
        [harnessName, sessionId],
      );
      const storedWinner = winner[0];
      const winnerIsImport =
        storedWinner !== undefined &&
        (rowsMatchExcept(storedWinner, base, incarnationExcluded) ||
          // Same raced-import case as the pre-locked path above: the winner
          // can be the closure's own session row after a destination worker
          // already advanced it.
          (Number(storedWinner.version) > Number(base.version) &&
            columnsMatchOn(storedWinner, base, SESSION_IDENTITY_COLUMNS)));
      if (
        winnerIsImport &&
        typeof storedWinner.session_incarnation === 'string' &&
        storedWinner.session_incarnation.length > 0
      ) {
        incarnations[sessionId] = storedWinner.session_incarnation;
        appliedSessionRows.set(sessionId, storedWinner);
        skipped[TABLE_HARNESS_SESSIONS]! += 1;
        continue;
      }
      throw closureError(
        'IMPORT_EXECUTION_CLOSURE',
        'DESTINATION_ROW_CONFLICT',
        `Harness session ${sessionId} already exists with different content`,
        { tableName: TABLE_HARNESS_SESSIONS, sessionId },
      );
    }

    // --- Workflow snapshot handoffs are imported as evidence only: their
    // snapshot is materialized as canonical state and the fence row itself is
    // never restored, so the destination run stays writable.
    const handoffPairs = new Map<string, Record<string, unknown>>();
    for (const row of rows[TABLE_WORKFLOW_SNAPSHOT_HANDOFF] ?? []) {
      const pair = pairKeyOf(row.workflow_name, row.run_id);
      if (pair) handoffPairs.set(pair, row);
    }

    const liveTerminalIntents: Record<string, unknown>[] = [];

    for (const [table, spec] of Object.entries(EXECUTION_CLOSURE_TABLES)) {
      const tableName = table as ExecutionClosureTableName;
      if (tableName === TABLE_HARNESS_SESSIONS) continue;
      const tableRows = rows[tableName] ?? [];
      const s = spec!;
      if (s.role === 'authority') {
        skipped[tableName] = tableRows.length;
        continue;
      }
      requireTable(
        tableName,
        // Handoff materialization also writes snapshot rows.
        tableRows.length + (tableName === TABLE_WORKFLOW_SNAPSHOT ? handoffPairs.size : 0),
      );

      for (const row of tableRows) {
        // A snapshot row superseded by a pending/completed handoff must not
        // overwrite the newer handoff snapshot.
        if (tableName === TABLE_WORKFLOW_SNAPSHOT) {
          const pair = pairKeyOf(row.workflow_name, row.run_id);
          if (pair && handoffPairs.has(pair)) {
            skipped[tableName]! += 1;
            continue;
          }
        }
        const applied = applyImportTransforms(tableName, row, incarnations);
        assertRowHarnessName(tableName, applied, harnessName);
        const outcome = await insertClosureRow(t, tableName, schemaName, applied, {
          verifyConflict: s.role !== 'shared-resource',
        });
        if (outcome === 'inserted') {
          inserted[tableName]! += 1;
          if (
            tableName === TABLE_HARNESS_TERMINAL_INTENTS &&
            LIVE_TERMINAL_INTENT_STATUSES.has(String(applied.status))
          ) {
            liveTerminalIntents.push(applied);
          }
        } else {
          skipped[tableName]! += 1;
        }
      }

      // Materialize handoff snapshots as canonical state for their run.
      if (tableName === TABLE_WORKFLOW_SNAPSHOT) {
        for (const [pair, handoff] of handoffPairs) {
          const [workflowName, runId] = splitPairKey(pair);
          const createdAt = Number(handoff.created_at);
          const updatedAt = Number(handoff.updated_at);
          const materialized: Record<string, unknown> = {
            workflow_name: workflowName,
            run_id: runId,
            resourceId: handoff.resource_id ?? null,
            snapshot: handoff.snapshot,
            createdAt: new Date(createdAt),
            updatedAt: new Date(updatedAt),
            createdAtZ: new Date(createdAt),
            updatedAtZ: new Date(updatedAt),
          };
          const outcome = await insertClosureRow(t, TABLE_WORKFLOW_SNAPSHOT, schemaName, materialized);
          if (outcome === 'inserted') {
            inserted[TABLE_WORKFLOW_SNAPSHOT]! += 1;
          } else {
            skipped[TABLE_WORKFLOW_SNAPSHOT]! += 1;
          }
        }
      }
    }

    // --- Rebuild the destination terminal pressure counter from the live
    // intents this import actually restored; the source's harness-global
    // counter is evidence only.
    if (liveTerminalIntents.length > 0 && tablePresent(TABLE_HARNESS_TERMINAL_PRESSURE)) {
      const pendingBytes = liveTerminalIntents.reduce((sum, row) => sum + Number(row.payload_bytes ?? 0), 0);
      await t.none(
        `INSERT INTO ${tableSql(TABLE_HARNESS_TERMINAL_PRESSURE, schemaName)}
           (id, harness_name, pending_intents, pending_bytes, updated_at)
         VALUES ($1, $1, $2, $3, $4)
         ON CONFLICT (id) DO UPDATE SET
           pending_intents = ${tableSql(TABLE_HARNESS_TERMINAL_PRESSURE, schemaName)}.pending_intents + $2,
           pending_bytes = ${tableSql(TABLE_HARNESS_TERMINAL_PRESSURE, schemaName)}.pending_bytes + $3,
           updated_at = $4`,
        [harnessName, liveTerminalIntents.length, pendingBytes, Date.now()],
      );
    }

    // --- Restage pending session-record projection work: the imported fence
    // plus a fresh intent at the session's current revision lets the
    // destination claim the projection instead of leaving the migrated
    // session stale in the read model.
    const projectionTablesPresent =
      tablePresent(TABLE_HARNESS_SESSION_PROJECTION_INTENTS) &&
      tablePresent(TABLE_HARNESS_SESSION_PROJECTION_FENCES) &&
      tablePresent(TABLE_HARNESS_SESSION_PROJECTION_PRESSURE);
    if (projectionTablesPresent) {
      const activeFenceSessions = new Set(
        (rows[TABLE_HARNESS_SESSION_PROJECTION_FENCES] ?? [])
          .filter(row => row.state === 'active')
          .map(row => String(row.session_id)),
      );
      let staged = 0;
      let stagedBytes = 0;
      for (const sessionId of activeFenceSessions) {
        const appliedRow = appliedSessionRows.get(sessionId);
        if (!appliedRow) continue;
        let intent;
        try {
          // The staged row must be deterministic across retries: its
          // createdAt is the persisted session's own last_activity_at — a
          // wall-clock timestamp would make a re-import read back a different
          // row and misreport the retry as a foreign conflict. (Session rows
          // carry created_at/last_activity_at, never an updated_at.)
          intent = buildHarnessSessionRecordProjectionIntent(rowToSession(appliedRow), {
            sessionIncarnation: incarnations[sessionId]!,
            revision: Number(appliedRow.version),
            createdAt: Number(appliedRow.last_activity_at),
            maxPayloadBytes: DEFAULT_HARNESS_SESSION_RECORD_PROJECTION_MAX_PAYLOAD_BYTES,
          });
        } catch (error) {
          // An oversized post-image could never have been built by the runtime
          // either — skip staging rather than failing the import.
          if (error instanceof RangeError) continue;
          throw error;
        }
        // bigint columns are passed as strings so the row the read-back
        // compares against an idempotent retry canonicalizes identically —
        // the pg driver returns int8 as text, and `1234` !== `'1234'` in the
        // canonical row comparison.
        const outcome = await insertClosureRow(t, TABLE_HARNESS_SESSION_PROJECTION_INTENTS, schemaName, {
          id: intent.id,
          operation_id: intent.operationId,
          harness_name: intent.harnessName,
          session_id: intent.sessionId,
          session_incarnation: intent.sessionIncarnation,
          resource_id: intent.resourceId,
          thread_id: intent.threadId,
          revision: intent.revision,
          payload_digest: intent.payloadDigest,
          payload_bytes: String(intent.payloadBytes),
          payload: intent.payload,
          status: intent.status,
          attempts: intent.attempts,
          claim_id: null,
          claim_expires_at: null,
          next_attempt_at: null,
          applied_at: null,
          failed_at: null,
          dead_at: null,
          last_error: null,
          created_at: String(intent.createdAt),
          updated_at: String(intent.updatedAt),
        });
        if (outcome === 'inserted') {
          staged += 1;
          stagedBytes += intent.payloadBytes;
          inserted[TABLE_HARNESS_SESSION_PROJECTION_INTENTS]! += 1;
        } else {
          skipped[TABLE_HARNESS_SESSION_PROJECTION_INTENTS]! += 1;
        }
      }
      if (staged > 0) {
        await t.none(
          `INSERT INTO ${tableSql(TABLE_HARNESS_SESSION_PROJECTION_PRESSURE, schemaName)}
             (harness_name, pending_intents, pending_bytes, updated_at)
           VALUES ($1, $2, $3, $4)
           ON CONFLICT (harness_name) DO UPDATE SET
             pending_intents = ${tableSql(TABLE_HARNESS_SESSION_PROJECTION_PRESSURE, schemaName)}.pending_intents + $2,
             pending_bytes = ${tableSql(TABLE_HARNESS_SESSION_PROJECTION_PRESSURE, schemaName)}.pending_bytes + $3,
             updated_at = $4`,
          [harnessName, staged, stagedBytes, Date.now()],
        );
      }
    }

    return {
      status: manifest.completeness === 'complete' ? 'imported' : 'pinned',
      incarnations,
      inserted,
      skipped,
      pins: manifest.pins,
    };
  });
}
