import { getStoragePersistenceFailure } from '@mastra/core/storage';
import type { StoragePersistenceFailure } from '@mastra/core/storage';
import { getPgTransactionFailurePhase } from '../client';

/**
 * Postgres error classifiers for concurrent DDL.
 *
 * `CREATE SCHEMA / TABLE / INDEX IF NOT EXISTS` checks aren't atomic against
 * concurrent backends. Two callers racing past the existence probe can both
 * reach the catalog insert; the loser sees a duplicate-object error. These
 * helpers identify the exact codes we treat as "already exists by the time we
 * look" so every other error surfaces normally.
 */

interface PgErrorLike {
  code?: string;
  constraint?: string;
  message?: string;
}

function asPgError(error: unknown): PgErrorLike {
  return (error ?? {}) as PgErrorLike;
}

/**
 * True when `error` says a relation with this name already exists in this
 * schema. Covers `42P07` (clean case from `CREATE TABLE`) and `23505`
 * unique-violation races on the relevant pg_catalog indexes, plus a final
 * `/already exists/i` regex fallback for drivers that don't surface a code.
 */
export function isDuplicateRelationError(error: unknown): boolean {
  const { code, constraint, message = '' } = asPgError(error);
  if (code === '42P07') return true;
  if (code === '23505' && (constraint === 'pg_type_typname_nsp_index' || constraint === 'pg_class_relname_nsp_index')) {
    return true;
  }
  return /already exists/i.test(message);
}

/**
 * True when `error` says a schema with this name already exists. Covers
 * `42P06` and the `23505` race on `pg_namespace_nspname_index`, plus a
 * narrower regex fallback than `isDuplicateRelationError` so this helper
 * only swallows schema-existence errors.
 */
export function isDuplicateSchemaError(error: unknown): boolean {
  const { code, constraint, message = '' } = asPgError(error);
  if (code === '42P06') return true;
  if (code === '23505' && constraint === 'pg_namespace_nspname_index') return true;
  return /schema .* already exists/i.test(message);
}

/**
 * SQLSTATE values whose server response does not settle a write that was
 * already executing or committing: the statement completion is unknown
 * (40003), the backend was shut down or crashed around the commit (57P01,
 * 57P02), the connection failed (class 08) or the server hit an internal error
 * (XX000). A read or a write that never reached COMMIT still did not apply.
 */
function isAmbiguousWriteSqlState(code: string): boolean {
  return code === '40003' || code === '57P01' || code === '57P02' || code === 'XX000' || code.startsWith('08');
}

/**
 * SQLSTATE values a later attempt can get past: transaction rollback
 * conflicts (class 40), exhausted resources (class 53), lock and object-in-use
 * waits, statement or session timeouts and cancellations, server shutdown,
 * connection failures (class 08) and server I/O errors.
 */
function isTransientSqlState(code: string): boolean {
  return (
    code.startsWith('40') ||
    code.startsWith('53') ||
    code.startsWith('08') ||
    code === '55P03' ||
    code === '55006' ||
    code === '57014' ||
    code === '57P01' ||
    code === '57P02' ||
    code === '57P03' ||
    code === '57P05' ||
    code === '25P03' ||
    code === '25P04' ||
    code === '58030'
  );
}

/** A PostgreSQL server error response carries a five-character SQLSTATE and a severity. */
function pgSqlState(error: unknown): string | undefined {
  if (typeof error !== 'object' || error === null) return undefined;
  const { code, severity } = error as { code?: unknown; severity?: unknown };
  return typeof code === 'string' && /^[0-9A-Z]{5}$/.test(code) && typeof severity === 'string' ? code : undefined;
}

/** Socket errors raised before a statement reached the server. */
const UNSENT_SOCKET_CODES = new Set(['ECONNREFUSED', 'ENOTFOUND', 'EAI_AGAIN', 'EHOSTUNREACH', 'ENETUNREACH']);
/** Socket errors that can interrupt a statement the server already received. */
const IN_FLIGHT_SOCKET_CODES = new Set(['ECONNRESET', 'ETIMEDOUT', 'EPIPE', 'ECONNABORTED']);
/**
 * node-postgres and pg-pool raise these without a code. The first group fails
 * before a statement is written to the connection; the second ends a
 * connection or abandons a statement that may already be executing.
 */
const UNSENT_DRIVER_MESSAGES = new Set([
  'timeout exceeded when trying to connect',
  'Connection terminated due to connection timeout',
  'Client has encountered a connection error and is not queryable',
  'Client was closed and is not queryable',
  'timeout expired',
]);
const IN_FLIGHT_DRIVER_MESSAGES = new Set([
  'Connection terminated unexpectedly',
  'Connection terminated',
  'Query read timeout',
]);

type TransportFailure = 'unsent' | 'in_flight';

const MAX_CLASSIFIED_CAUSE_DEPTH = 8;

function causeChain(error: unknown): unknown[] {
  const chain: unknown[] = [];
  let candidate = error;
  while (chain.length < MAX_CLASSIFIED_CAUSE_DEPTH && typeof candidate === 'object' && candidate !== null) {
    chain.push(candidate);
    candidate = (candidate as { cause?: unknown }).cause;
  }
  return chain;
}

function pgTransportFailure(error: unknown): TransportFailure | undefined {
  if (typeof error !== 'object' || error === null) return undefined;
  const { code, message } = error as { code?: unknown; message?: unknown };
  if (typeof code === 'string') {
    if (UNSENT_SOCKET_CODES.has(code)) return 'unsent';
    if (IN_FLIGHT_SOCKET_CODES.has(code)) return 'in_flight';
  }
  if (typeof message === 'string') {
    if (UNSENT_DRIVER_MESSAGES.has(message)) return 'unsent';
    if (IN_FLIGHT_DRIVER_MESSAGES.has(message)) return 'in_flight';
  }
  return undefined;
}

/**
 * Classify a failed PostgreSQL storage operation for the shared storage
 * persistence failure contract (`@mastra/core/storage`).
 *
 * `write` says whether the operation mutates durable state; a read-only
 * operation (even one inside a transaction) is never `commit_unknown`. A failure is
 * `commit_unknown` only when a write may have reached its commit point: the
 * COMMIT of a `tx()` failed without a definitive server rejection, or an
 * autocommit write statement lost its connection or got an ambiguous server
 * response. A `tx()` failure before COMMIT was sent, a read, or a definitive
 * server rejection never committed, and is `transient` or `permanent` by its
 * cause. Errors without a server or connection cause (validation, contract
 * and programming errors) are `permanent`. A classification already present
 * on the error or its causes is kept.
 */
export function classifyPgPersistenceFailure(error: unknown, { write }: { write: boolean }): StoragePersistenceFailure {
  const existing = getStoragePersistenceFailure(error);
  if (existing) return existing;

  // Domain code can wrap the driver error before it leaves a transaction, so
  // the phase and the driver cause are each taken from the nearest link of the
  // bounded cause chain that carries one.
  const chain = causeChain(error);
  const phase = chain.map(getPgTransactionFailurePhase).find(value => value !== undefined);
  const mayHaveCommitted = write && (phase === 'commit' || phase === undefined);
  for (const link of chain) {
    const sqlState = pgSqlState(link);
    if (sqlState !== undefined) {
      if (mayHaveCommitted && isAmbiguousWriteSqlState(sqlState)) return 'commit_unknown';
      return isTransientSqlState(sqlState) ? 'transient' : 'permanent';
    }
    const transport = pgTransportFailure(link);
    if (transport !== undefined) {
      return mayHaveCommitted && transport === 'in_flight' ? 'commit_unknown' : 'transient';
    }
  }
  // Only the driver can fail a COMMIT; an unrecognized failure of a write's
  // COMMIT is not a definitive rollback.
  return write && phase === 'commit' ? 'commit_unknown' : 'permanent';
}
