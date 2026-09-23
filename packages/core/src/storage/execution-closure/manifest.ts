import { createHash } from 'node:crypto';

import { stableStringify } from '../../agent/message-list/cache/stable-stringify';
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
 * per-table row counts and digests. Import calls this before staging; a
 * corrupt or incomplete payload fails closed instead of staging partial data.
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
