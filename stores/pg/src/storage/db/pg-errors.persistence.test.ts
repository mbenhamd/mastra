import type { Pool } from 'pg';
import { describe, expect, it } from 'vitest';
import { PoolAdapter } from '../client';
import { classifyPgPersistenceFailure } from './pg-errors';

/** A server error response as node-postgres surfaces it. */
function serverError(code: string, severity = 'ERROR'): Error {
  return Object.assign(new Error(`server ${code}`), { code, severity });
}

function socketError(code: string): Error {
  return Object.assign(new Error(`socket ${code}`), { code });
}

/**
 * A pool whose transaction client fails the named statement (`BEGIN`, `WORK`
 * for the callback's statement, or `COMMIT`) with `error`.
 */
function failingPool(failAt: 'BEGIN' | 'WORK' | 'COMMIT', error: unknown): Pool {
  const client = {
    query: async (text: string) => {
      const statement = text === 'BEGIN' || text === 'COMMIT' || text === 'ROLLBACK' ? text : 'WORK';
      if (statement === failAt) throw error;
      return { rows: [], rowCount: 0 };
    },
    release: () => undefined,
  };
  return { connect: async () => client, query: async () => Promise.reject(error) } as unknown as Pool;
}

async function txFailure(failAt: 'BEGIN' | 'WORK' | 'COMMIT', error: unknown): Promise<unknown> {
  const adapter = new PoolAdapter(failingPool(failAt, error));
  return adapter
    .tx(t => t.none('UPDATE workflow SET snapshot = $1', [1]))
    .then(
      () => {
        throw new Error('expected the transaction to fail');
      },
      (failure: unknown) => failure,
    );
}

async function statementFailure(error: unknown): Promise<unknown> {
  const adapter = new PoolAdapter(failingPool('WORK', error));
  return adapter.none('UPDATE workflow SET snapshot = $1', [1]).then(
    () => {
      throw new Error('expected the statement to fail');
    },
    (failure: unknown) => failure,
  );
}

describe('classifyPgPersistenceFailure', () => {
  it.each([
    ['a serialization conflict', serverError('40001'), 'transient'],
    ['a lock timeout', serverError('55P03'), 'transient'],
    ['a terminated backend', serverError('57P01', 'FATAL'), 'transient'],
    ['a dropped connection', socketError('ECONNRESET'), 'transient'],
    ['a unique violation', serverError('23505'), 'permanent'],
    ['a missing relation', serverError('42P01'), 'permanent'],
    ['a contract error', new TypeError('Workflow snapshot is missing parent revision evidence'), 'permanent'],
  ])('classifies %s before COMMIT as never committed', async (_name, error, expected) => {
    const failure = await txFailure('WORK', error);
    expect(classifyPgPersistenceFailure(failure, { write: true })).toBe(expected);
  });

  it('classifies a BEGIN failure as never committed', async () => {
    const failure = await txFailure('BEGIN', socketError('ECONNRESET'));
    expect(classifyPgPersistenceFailure(failure, { write: true })).toBe('transient');
  });

  it.each([
    ['the connection dropped', socketError('ECONNRESET'), 'commit_unknown'],
    ['the driver lost the connection', new Error('Connection terminated unexpectedly'), 'commit_unknown'],
    ['the backend was terminated', serverError('57P01', 'FATAL'), 'commit_unknown'],
    ['statement completion is unknown', serverError('40003'), 'commit_unknown'],
    [
      'the driver raised an unrecognized error',
      new Error('Received unexpected commandComplete message'),
      'commit_unknown',
    ],
    ['the server rejected a serialization conflict', serverError('40001'), 'transient'],
    ['the server rejected a deferred constraint', serverError('23505'), 'permanent'],
    ['the client was unusable before sending it', new Error('Client was closed and is not queryable'), 'transient'],
  ])('classifies a write COMMIT failure when %s', async (_name, error, expected) => {
    const failure = await txFailure('COMMIT', error);
    expect(classifyPgPersistenceFailure(failure, { write: true })).toBe(expected);
  });

  it('never classifies a read-only transaction as commit_unknown', async () => {
    const failure = await txFailure('COMMIT', socketError('ECONNRESET'));
    expect(classifyPgPersistenceFailure(failure, { write: false })).toBe('transient');
  });

  it.each([
    ['lost its connection mid-statement', socketError('ECONNRESET'), true, 'commit_unknown'],
    ['timed out reading the response', new Error('Query read timeout'), true, 'commit_unknown'],
    ['could not connect', socketError('ECONNREFUSED'), true, 'transient'],
    ['timed out acquiring a connection', new Error('timeout exceeded when trying to connect'), true, 'transient'],
    ['was rejected by a definitive server error', serverError('23505'), true, 'permanent'],
    ['lost its connection while reading', socketError('ECONNRESET'), false, 'transient'],
  ])('classifies an autocommit statement that %s', async (_name, error, write, expected) => {
    const failure = await statementFailure(error);
    expect(classifyPgPersistenceFailure(failure, { write })).toBe(expected);
  });

  it('finds the driver failure behind a wrapping error', async () => {
    const failure = await txFailure('COMMIT', socketError('ECONNRESET'));
    const wrapped = new Error('domain wrapper', { cause: failure });
    expect(classifyPgPersistenceFailure(wrapped, { write: true })).toBe('commit_unknown');
  });
});
