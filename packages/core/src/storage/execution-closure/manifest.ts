import { createHash } from 'node:crypto';

import { stableStringify } from '../../agent/message-list/cache/stable-stringify';
import { TABLE_HARNESS_SESSIONS } from '../constants';
import { EXECUTION_CLOSURE_TABLES } from './tables';
import type {
  ExecutionClosureKey,
  ExecutionClosureManifest,
  ExecutionClosurePin,
  ExecutionClosureTableDigest,
  ExecutionClosureTableName,
} from './types';

/**
 * Canonical encoding of a raw column value for digests. Storage drivers return
 * `Date`, `Buffer`/`Uint8Array`, and `bigint` for timestamptz/bytea/bigint
 * columns; each normalizes to the same JSON value its transport representation
 * converges to (ISO string, base64 string, decimal string). Digests therefore
 * survive a JSON round-trip of the payload — a `Date` and the ISO string it
 * serializes to digest identically.
 *
 * Type tags are deliberately omitted: a column's type is fixed by its schema,
 * so the same column can never legitimately hold both a timestamptz and a text
 * ISO string across stores — the untagged form cannot produce a real collision.
 */
export function canonicalClosureValue(value: unknown): unknown {
  if (value === null || value === undefined) return null;
  if (value instanceof Date) return value.toISOString();
  if (typeof value === 'bigint') return value.toString();
  if (Buffer.isBuffer(value) || value instanceof Uint8Array) {
    return Buffer.from(value).toString('base64');
  }
  if (Array.isArray(value)) return value.map(canonicalClosureValue);
  if (typeof value === 'object') {
    const out: Record<string, unknown> = Object.create(null);
    for (const key of Object.keys(value)) {
      out[key] = canonicalClosureValue((value as Record<string, unknown>)[key]);
    }
    return out;
  }
  return value;
}

export function canonicalClosureRow(row: Record<string, unknown>): string {
  return stableStringify(canonicalClosureValue(row));
}

/**
 * Digest a table's exported rows: canonical-serialize each row, sort, hash.
 * Sorting makes the digest independent of read order; identical row sets digest
 * identically regardless of driver-returned key order or column types.
 */
export function digestClosureRows(rows: Record<string, unknown>[]): `sha256:${string}` {
  const hash = createHash('sha256');
  for (const line of rows.map(canonicalClosureRow).sort()) {
    hash.update(line);
    hash.update('\n');
  }
  return `sha256:${hash.digest('hex')}`;
}

export interface BuildExecutionClosureManifestInput {
  key: ExecutionClosureKey;
  source?: ExecutionClosureManifest['source'];
  sessionIds: string[];
  incarnations: Record<string, string>;
  threadIds: string[];
  runIds: string[];
  resourceIds: string[];
  /** Channel ids the exported channel bindings actually own. */
  channelIds?: string[];
  /** Encoded `encodeThreadStateScope` keys covered by the closure. */
  threadStateKeys?: string[];
  /** `(workflow_name, run_id)` pairs in scope for run-pair keyed tables. */
  runPairs?: { workflowName: string; runId: string }[];
  rows: Partial<Record<ExecutionClosureTableName, Record<string, unknown>[]>>;
  pins: ExecutionClosurePin[];
}

/**
 * Build the manifest from collected dimensions and exported rows. Every
 * registered closure table appears in `tables` — a zero-row table still
 * records an empty-set digest so a missing read cannot masquerade as an
 * empty table at import.
 */
export function buildExecutionClosureManifest(input: BuildExecutionClosureManifestInput): ExecutionClosureManifest {
  const tables: ExecutionClosureTableDigest[] = [];
  for (const [table, spec] of Object.entries(EXECUTION_CLOSURE_TABLES)) {
    const tableName = table as ExecutionClosureTableName;
    const rows = input.rows[tableName] ?? [];
    tables.push({
      table: tableName,
      role: spec!.role,
      rowCount: rows.length,
      sha256: digestClosureRows(rows),
    });
  }
  tables.sort((a, b) => (a.table < b.table ? -1 : a.table > b.table ? 1 : 0));

  const sortUnique = (ids: Iterable<string>) => [...new Set(ids)].sort();
  return {
    format: 'mastra-execution-closure',
    version: 1,
    key: input.key,
    ...(input.source ? { source: input.source } : {}),
    sessionIds: sortUnique(input.sessionIds),
    incarnations: input.incarnations,
    threadIds: sortUnique(input.threadIds),
    runIds: sortUnique(input.runIds),
    resourceIds: sortUnique(input.resourceIds),
    channelIds: sortUnique(input.channelIds ?? []),
    threadStateKeys: sortUnique(input.threadStateKeys ?? []),
    runPairs: (input.runPairs ?? [])
      .map(pair => ({ workflowName: pair.workflowName, runId: pair.runId }))
      .sort((a, b) => {
        const left = `${a.workflowName}${a.runId}`;
        const right = `${b.workflowName}${b.runId}`;
        return left < right ? -1 : left > right ? 1 : 0;
      }),
    tables,
    completeness: input.pins.length === 0 ? 'complete' : 'pinned',
    pins: input.pins,
  };
}

export interface VerifyExecutionClosureResult {
  ok: boolean;
  /** Human-readable mismatch descriptions, one per failing table/check. */
  mismatches: string[];
}

/**
 * Verify an exported payload against its manifest: registered table coverage,
 * per-table row counts and digests, plus the builder invariants the manifest
 * cannot disclaim. Import calls this before staging; a corrupt, incomplete,
 * or hand-built payload fails closed instead of staging partial data.
 *
 * The manifest is not authenticated, so the verifier rejects manifests the
 * builder cannot produce: duplicate or missing table entries, a `complete`
 * completeness that still carries pins, `sessionIds` that do not match the
 * exported session rows, and payload rows whose scope bindings name ids the
 * manifest never declared.
 */
export function verifyExecutionClosurePayload(
  manifest: ExecutionClosureManifest,
  rows: Partial<Record<ExecutionClosureTableName, Record<string, unknown>[]>>,
): VerifyExecutionClosureResult {
  const mismatches: string[] = [];

  if (manifest.format !== 'mastra-execution-closure') {
    mismatches.push(`unsupported manifest format ${JSON.stringify(manifest.format)}`);
  }
  if (manifest.version !== 1) {
    mismatches.push(`unsupported manifest version ${JSON.stringify(manifest.version)}`);
  }

  const registered = new Set(Object.keys(EXECUTION_CLOSURE_TABLES));
  const listed = new Set(manifest.tables.map(t => t.table));
  if (listed.size !== manifest.tables.length) {
    mismatches.push('manifest lists a table more than once');
  }
  for (const table of listed) {
    if (!registered.has(table)) {
      mismatches.push(`manifest lists unregistered table ${table}`);
    }
  }
  for (const table of Object.keys(rows)) {
    if (!listed.has(table as ExecutionClosureTableName)) {
      mismatches.push(`payload carries rows for table ${table} absent from the manifest`);
    }
  }
  // The builder emits every registered table (empty tables included), so an
  // omitted entry means the manifest was not built by this exporter — a
  // missing read must never masquerade as an empty table.
  for (const table of Object.keys(EXECUTION_CLOSURE_TABLES)) {
    if (!listed.has(table as ExecutionClosureTableName)) {
      mismatches.push(`manifest omits registered table ${table}`);
    }
  }

  const expectedCompleteness = manifest.pins.length === 0 ? 'complete' : 'pinned';
  if (manifest.completeness !== expectedCompleteness) {
    mismatches.push(
      `manifest completeness ${JSON.stringify(manifest.completeness)} does not match ${manifest.pins.length} pin(s)`,
    );
  }

  // `sessionIds` is the import's authority-fence contract: every imported
  // session row must appear there so it receives the destination incarnation.
  const payloadSessionIds = (rows[TABLE_HARNESS_SESSIONS] ?? [])
    .map(row => row.id)
    .filter((id): id is string => typeof id === 'string');
  const sortJoin = (ids: string[]) => [...new Set(ids)].sort().join('\x00');
  if (sortJoin(payloadSessionIds) !== sortJoin(manifest.sessionIds)) {
    mismatches.push('manifest sessionIds do not match the exported session rows');
  }

  // The declared id sets are the row-level scope contract: a payload row whose
  // binding column names an id outside the declared set is data the exporter
  // never claimed — the digest alone cannot reject it because a hand-built
  // manifest can recompute digests over forged rows. Each registered table is
  // checked with the same OR/`when` semantics the exporter applies, and
  // run-pair keyed tables bind against the declared (workflow_name, run_id)
  // pairs rather than bare run ids.
  const dimensionSets = {
    session: new Set(manifest.sessionIds),
    thread: new Set(manifest.threadIds),
    run: new Set(manifest.runIds),
    resource: new Set(manifest.resourceIds),
    channel: new Set(manifest.channelIds ?? []),
    threadState: new Set(manifest.threadStateKeys ?? []),
  };
  // NUL joins the pair so distinct (workflow_name, run_id) boundaries cannot
  // collide (`['ab','c']` vs `['a','bc']`); the exporter uses the same
  // separator and Postgres text cannot carry NUL, so real values never do.
  const runPairSet = new Set((manifest.runPairs ?? []).map(pair => `${pair.workflowName}\u0000${pair.runId}`));
  for (const [table, spec] of Object.entries(EXECUTION_CLOSURE_TABLES)) {
    if (!spec) continue;
    const tableRows = rows[table as ExecutionClosureTableName] ?? [];
    if (tableRows.length === 0) continue;
    if (spec.runPairScope) {
      const { workflowNameColumn, runIdColumn } = spec.runPairScope;
      for (const row of tableRows) {
        const workflowName = row[workflowNameColumn];
        const runId = row[runIdColumn];
        if (
          typeof workflowName !== 'string' ||
          typeof runId !== 'string' ||
          !runPairSet.has(`${workflowName}\u0000${runId}`)
        ) {
          mismatches.push(`row in ${table} binds a run pair outside the declared closure scope`);
        }
      }
      continue;
    }
    if (spec.scope.length === 0) continue; // harness-scoped rows carry no row-level binding
    for (const row of tableRows) {
      const inScope = spec.scope.some(filter => {
        if (filter.when && row[filter.when.column] !== filter.when.equals) return false;
        const value = row[filter.column];
        return typeof value === 'string' && dimensionSets[filter.dimension].has(value);
      });
      if (!inScope) {
        mismatches.push(`row in ${table} is outside the declared closure scope`);
      }
    }
  }

  for (const entry of manifest.tables) {
    const spec = EXECUTION_CLOSURE_TABLES[entry.table];
    if (spec && spec.role !== entry.role) {
      mismatches.push(`manifest role for ${entry.table} (${entry.role}) does not match the registry (${spec.role})`);
    }
    const tableRows = rows[entry.table] ?? [];
    if (tableRows.length !== entry.rowCount) {
      mismatches.push(`table ${entry.table} row count ${tableRows.length} != manifest ${entry.rowCount}`);
      continue;
    }
    const digest = digestClosureRows(tableRows);
    if (digest !== entry.sha256) {
      mismatches.push(`table ${entry.table} digest ${digest} != manifest ${entry.sha256}`);
    }
  }

  return { ok: mismatches.length === 0, mismatches };
}
