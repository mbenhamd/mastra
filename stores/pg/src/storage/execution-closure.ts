import { randomUUID } from 'node:crypto';

import { MastraError, ErrorDomain, ErrorCategory } from '@mastra/core/error';
import {
  EXECUTION_CLOSURE_TABLES,
  TABLE_HARNESS_SESSIONS,
  TABLE_RESOURCES,
  TABLE_SCHEMAS,
  TABLE_THREADS,
  TABLE_WORKFLOW_SNAPSHOT,
  buildExecutionClosureManifest,
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
import type { DbClient, TxClient } from './client';
import { getSchemaName, getTableName } from './db';
import { parseJsonResilient } from './domains/utils';

export interface ExportExecutionClosureOptions {
  schemaName?: string;
}

type Dimension = 'session' | 'thread' | 'run' | 'resource';
type DimensionSets = Record<Dimension, Set<string>>;

function tableSql(table: ExecutionClosureTableName, schemaName?: string) {
  return getTableName({ indexName: table, schemaName });
}

function hasHarnessName(table: ExecutionClosureTableName): boolean {
  const schema = (TABLE_SCHEMAS as Record<string, Record<string, unknown> | undefined>)[table];
  return schema !== undefined && 'harness_name' in schema;
}

/**
 * Read every registered closure row for one table. Rows bind through
 * OR-combined (column -> dimension) scope filters; harness tables also carry a
 * `harness_name` predicate. `harnessScoped` tables bind by namespace alone.
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
      .map(({ column, dimension }) => `"${column}" = ANY(${push([...dims[dimension]])}::text[])`)
      .join(' OR ');
    conditions.push(`(${ors})`);
  }
  // A registry entry with no scope filters and no harness_name column would
  // emit an unbounded SELECT — fail closed instead of dumping the whole table.
  if (conditions.length === 0) return [];
  const where = ` WHERE ${conditions.join(' AND ')}`;
  return t.manyOrNone<Record<string, unknown>>(`SELECT * FROM ${tableSql(table, schemaName)}${where}`, args);
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
 * Unknown ancestry/ownership does not abort the export — it records a pin so
 * the unit can never be imported as `complete`.
 */
export async function exportExecutionClosure(
  db: DbClient,
  key: ExecutionClosureKey,
  options?: ExportExecutionClosureOptions,
): Promise<ExecutionClosurePayload> {
  const schemaName = getSchemaName(options?.schemaName);
  const sessionsTable = tableSql(TABLE_HARNESS_SESSIONS, schemaName);

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
      throw new MastraError({
        id: createStorageErrorId('PG', 'EXPORT_EXECUTION_CLOSURE', 'ROOT_SESSION_NOT_FOUND'),
        domain: ErrorDomain.STORAGE,
        category: ErrorCategory.USER,
        text: `Harness session ${key.sessionId} not found in namespace ${key.harnessName}`,
        details: { harnessName: key.harnessName, sessionId: key.sessionId },
      });
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

    for (const row of sessionRows) {
      const id = row.id as string;
      incarnations[id] = typeof row.session_incarnation === 'string' ? row.session_incarnation : '';
      if (typeof row.thread_id === 'string') dims.thread.add(row.thread_id);
      if (typeof row.resource_id === 'string') dims.resource.add(row.resource_id);
      const currentRun = parseJsonResilient(row.current_run) as { runId?: unknown } | undefined;
      if (typeof currentRun?.runId === 'string') dims.run.add(currentRun.runId);
    }
    const rootRow = sessionRows.find(r => r.id === key.sessionId);
    if (rootRow && typeof rootRow.parent_session_id === 'string' && !dims.session.has(rootRow.parent_session_id)) {
      pins.push({
        reason: 'root-parent-outside-closure',
        detail: { sessionId: key.sessionId, parentSessionId: rootRow.parent_session_id },
      });
    }

    // --- Session/thread/resource-scoped tables. `run` waits for run ids
    // collected from the first pass.
    const rows: Partial<Record<ExecutionClosureTableName, Record<string, unknown>[]>> = {
      [TABLE_HARNESS_SESSIONS]: sessionRows,
    };
    for (const [table, spec] of Object.entries(EXECUTION_CLOSURE_TABLES)) {
      const tableName = table as ExecutionClosureTableName;
      if (tableName === TABLE_HARNESS_SESSIONS) continue;
      if (spec!.scope.some(s => s.dimension === 'run')) continue;
      rows[tableName] = await readClosureTable(t, tableName, schemaName, key.harnessName, dims);
    }

    // --- Run ids: current_run refs plus every run_id column already read.
    for (const tableRows of Object.values(rows)) {
      for (const row of tableRows ?? []) {
        if (typeof row.run_id === 'string') dims.run.add(row.run_id);
      }
    }

    for (const [table, spec] of Object.entries(EXECUTION_CLOSURE_TABLES)) {
      const tableName = table as ExecutionClosureTableName;
      if (!spec!.scope.some(s => s.dimension === 'run')) continue;
      rows[tableName] = await readClosureTable(t, tableName, schemaName, key.harnessName, dims);
    }

    // --- Pins: unknown ancestry/ownership marks the unit incomplete instead of
    // silently exporting a broken continuation.
    const threadRows = new Set((rows[TABLE_THREADS] ?? []).map(r => r.id));
    const resourceRows = new Set((rows[TABLE_RESOURCES] ?? []).map(r => r.id));
    const snapshotRuns = new Set((rows[TABLE_WORKFLOW_SNAPSHOT] ?? []).map(r => r.run_id));
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
      const currentRun = parseJsonResilient(row.current_run) as { runId?: unknown } | undefined;
      if (typeof currentRun?.runId === 'string' && !snapshotRuns.has(currentRun.runId)) {
        pins.push({
          reason: 'current-run-without-snapshot',
          detail: { sessionId: row.id, runId: currentRun.runId },
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

/**
 * Stage and apply an exported execution closure.
 *
 * Verification is the staging gate: a corrupt, incomplete, or unsupported
 * payload fails closed before a single row is written. The apply runs in one
 * transaction, so a crash either commits nothing (retry converges through
 * `ON CONFLICT DO NOTHING`) or the whole unit — there is no partially-applied
 * state to reconcile, and a lost acknowledgement can simply re-import.
 *
 * Authority is re-allocated, never restored:
 * - every imported session row receives a fresh `session_incarnation` and has
 *   `owner_id`/`lease_expires_at` cleared, so an old lease or in-flight
 *   callback under the exported incarnation is fenced by the runtime;
 * - `authority` rows (wakeups, outbox, inbox, tokens, bindings, claims,
 *   projection intents, pending attachment operations, pressure counters) are
 *   counted as skipped — the post-import runtime re-establishes its own;
 * - `fence` rows are rewritten onto the destination session incarnation so
 *   tombstones and paid attempts still apply to the session that was stored;
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
    throw new MastraError({
      id: createStorageErrorId('PG', 'IMPORT_EXECUTION_CLOSURE', 'PAYLOAD_MISMATCH'),
      domain: ErrorDomain.STORAGE,
      category: ErrorCategory.USER,
      text: `Execution closure payload failed manifest verification: ${verified.mismatches.join('; ')}`,
      details: { mismatchCount: verified.mismatches.length },
    });
  }

  const schemaName = getSchemaName(options?.schemaName);

  return db.tx(async t => {
    // Fresh incarnation per imported session — allocated inside the apply tx
    // so a retried import of the same payload converges only when the first
    // attempt actually committed.
    const incarnations: Record<string, string> = {};
    const existingSessions = manifest.sessionIds.length
      ? await t.any<{ id: string; session_incarnation: string | null }>(
          `SELECT id, session_incarnation FROM ${tableSql(TABLE_HARNESS_SESSIONS, schemaName)} WHERE id = ANY($1::text[])`,
          [manifest.sessionIds],
        )
      : [];
    const storedIncarnation = new Map(
      existingSessions.map(row => [row.id, row.session_incarnation] as const),
    );
    for (const sessionId of manifest.sessionIds) {
      const stored = storedIncarnation.get(sessionId);
      incarnations[sessionId] = typeof stored === 'string' && stored.length > 0 ? stored : randomUUID();
    }

    const inserted: Record<string, number> = {};
    const skipped: Record<string, number> = {};

    for (const [table, spec] of Object.entries(EXECUTION_CLOSURE_TABLES)) {
      const tableName = table as ExecutionClosureTableName;
      const tableRows = rows[tableName] ?? [];
      inserted[tableName] = 0;
      skipped[tableName] = 0;

      if (spec!.role === 'authority') {
        skipped[tableName] = tableRows.length;
        continue;
      }

      for (const row of tableRows) {
        const applied: Record<string, unknown> = { ...row };
        for (const column of spec!.clearOnImport ?? []) {
          applied[column] = null;
        }
        if (tableName === TABLE_HARNESS_SESSIONS) {
          applied['session_incarnation'] = incarnations[row.id as string] ?? null;
        }
        if (spec!.role === 'fence') {
          const sessionId = applied.session_id;
          if (typeof sessionId === 'string' && typeof applied.session_incarnation === 'string') {
            const destination = incarnations[sessionId];
            if (destination) applied.session_incarnation = destination;
          }
        }

        const columns = Object.keys(applied);
        const result = await t.query(
          `INSERT INTO ${tableSql(tableName, schemaName)} (${columns.map(c => `"${c}"`).join(', ')})
           VALUES (${columns.map((_, i) => `$${i + 1}`).join(', ')})
           ON CONFLICT DO NOTHING`,
          columns.map(c => applied[c]),
        );
        if ((result.rowCount ?? 0) > 0) {
          inserted[tableName]! += 1;
        } else {
          skipped[tableName]! += 1;
        }
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
