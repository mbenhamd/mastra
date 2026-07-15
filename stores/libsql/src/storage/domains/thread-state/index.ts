import type { Client } from '@libsql/client';
import { ErrorCategory, ErrorDomain, MastraError } from '@mastra/core/error';
import {
  ThreadStateStorage,
  encodeThreadStateScope,
  createStorageErrorId,
  TABLE_THREAD_STATE,
  THREAD_STATE_SCHEMA,
} from '@mastra/core/storage';
import type {
  PruneOptions,
  PruneResult,
  RetentionTablesDescriptor,
  TableRetentionPolicy,
  ThreadStateKey,
  ThreadStateMutation,
} from '@mastra/core/storage';

import { LibSQLDB, resolveClient } from '../../db';
import type { LibSQLDomainConfig } from '../../db';
import { createExecuteWriteOperationWithRetry } from '../../db/utils';
import { withClientWriteLock } from '../../db/write-lock';
import { runPrune, resolveTargets } from '../../retention';

function decodeValue<T>(value: unknown): T {
  return (typeof value === 'string' ? JSON.parse(value) : value) as T;
}

const MAX_MUTATION_CAS_ATTEMPTS = 100;

/**
 * LibSQL implementation of {@link ThreadStateStorage}.
 *
 * Stores per-thread, per-type state in `mastra_thread_state`, keyed by the
 * composite primary key `(threadId, type)`. The physical `threadId` value is a
 * collision-free encoding of resource + thread identity; `value` holds the
 * JSON payload (e.g. the task list for `type = 'task'`).
 */
export class ThreadStateLibSQL extends ThreadStateStorage {
  /**
   * `thread_state` grows as a side effect of thread activity (one row per
   * thread per state type). It anchors on `updatedAt` (last activity), so state
   * for a thread that is still being appended to is not pruned by creation age.
   */
  static readonly retentionTables: RetentionTablesDescriptor = {
    threadState: { table: TABLE_THREAD_STATE, column: 'updatedAt', indexed: true },
  };

  #db: LibSQLDB;
  #client: Client;
  private readonly executeWithRetry: <T>(operation: () => Promise<T>, description: string) => Promise<T>;

  constructor(config: LibSQLDomainConfig) {
    super();
    const client = resolveClient(config);
    const maxRetries = config.maxRetries ?? 5;
    const initialBackoffMs = config.initialBackoffMs ?? 100;
    this.#client = client;
    this.#db = new LibSQLDB({ client, maxRetries, initialBackoffMs });
    this.executeWithRetry = createExecuteWriteOperationWithRetry({
      logger: this.logger,
      maxRetries,
      initialBackoffMs,
    });
  }

  async init(): Promise<void> {
    await this.#db.createTable({
      tableName: TABLE_THREAD_STATE,
      schema: THREAD_STATE_SCHEMA,
      compositePrimaryKey: ['threadId', 'type'],
    });
  }

  /** Delete thread state older than the `threadState` policy's `maxAge`, batched. */
  async prune(policies: Record<string, TableRetentionPolicy>, options?: PruneOptions): Promise<PruneResult[]> {
    const targets = resolveTargets({
      policies,
      descriptor: ThreadStateLibSQL.retentionTables,
      order: ['threadState'],
    });
    return runPrune({ db: this.#db, domain: 'threadState', targets, options, logger: this.logger });
  }

  async dangerouslyClearAll(): Promise<void> {
    try {
      await this.executeWithRetry(
        () => withClientWriteLock(this.#client, () => this.#client.execute(`DELETE FROM "${TABLE_THREAD_STATE}"`)),
        'clear thread state',
      );
    } catch (error) {
      throw new MastraError(
        {
          id: createStorageErrorId('LIBSQL', 'THREAD_STATE_CLEAR_ALL', 'FAILED'),
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
        },
        error,
      );
    }
  }

  async getState<T = unknown>(args: ThreadStateKey): Promise<T | undefined> {
    const scopedThreadId = encodeThreadStateScope(args);
    try {
      const result = await this.#client.execute({
        sql: `SELECT "value" FROM "${TABLE_THREAD_STATE}" WHERE "threadId" = ? AND "type" = ? LIMIT 1`,
        args: [scopedThreadId, args.type],
      });
      const raw = result.rows?.[0]?.value;
      if (raw === undefined || raw === null) return undefined;
      return decodeValue<T>(raw);
    } catch (error) {
      throw new MastraError(
        {
          id: createStorageErrorId('LIBSQL', 'THREAD_STATE_GET', 'FAILED'),
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
          details: { resourceId: args.resourceId, threadId: args.threadId, type: args.type },
        },
        error,
      );
    }
  }

  async setState<T = unknown>(args: ThreadStateKey & { value: T }): Promise<void> {
    const scopedThreadId = encodeThreadStateScope(args);
    const now = new Date().toISOString();
    const serialized = JSON.stringify(args.value ?? null);
    try {
      await this.executeWithRetry(
        () =>
          withClientWriteLock(this.#client, () =>
            this.#client.execute({
              sql: `INSERT INTO "${TABLE_THREAD_STATE}" ("threadId", "type", "value", "createdAt", "updatedAt")
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT ("threadId", "type")
                    DO UPDATE SET "value" = excluded."value", "updatedAt" = excluded."updatedAt"`,
              args: [scopedThreadId, args.type, serialized, now, now],
            }),
          ),
        'set thread state',
      );
    } catch (error) {
      throw new MastraError(
        {
          id: createStorageErrorId('LIBSQL', 'THREAD_STATE_SET', 'FAILED'),
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
          details: { resourceId: args.resourceId, threadId: args.threadId, type: args.type },
        },
        error,
      );
    }
  }

  async deleteState(args: ThreadStateKey): Promise<void> {
    const scopedThreadId = encodeThreadStateScope(args);
    try {
      await this.executeWithRetry(
        () =>
          withClientWriteLock(this.#client, () =>
            this.#client.execute({
              sql: `DELETE FROM "${TABLE_THREAD_STATE}" WHERE "threadId" = ? AND "type" = ?`,
              args: [scopedThreadId, args.type],
            }),
          ),
        'delete thread state',
      );
    } catch (error) {
      throw new MastraError(
        {
          id: createStorageErrorId('LIBSQL', 'THREAD_STATE_DELETE', 'FAILED'),
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
          details: { resourceId: args.resourceId, threadId: args.threadId, type: args.type },
        },
        error,
      );
    }
  }

  override async mutateState<T = unknown, TResult = void>(
    args: ThreadStateKey & {
      mutate: (current: T | undefined) => ThreadStateMutation<T, TResult>;
    },
  ): Promise<TResult> {
    const scopedThreadId = encodeThreadStateScope(args);
    try {
      return await this.executeWithRetry(
        () =>
          withClientWriteLock(this.#client, async () => {
            // Interactive local LibSQL write transactions can block the Node
            // event loop while another client owns SQLite's writer lock. Use
            // optimistic compare-and-swap instead: the conditional write is
            // one atomic statement, and a changed predecessor retries the
            // complete read/transform/write operation.
            for (let attempt = 0; attempt < MAX_MUTATION_CAS_ATTEMPTS; attempt++) {
              const currentResult = await this.#client.execute({
                sql: `SELECT json("value") AS "serializedValue" FROM "${TABLE_THREAD_STATE}" WHERE "threadId" = ? AND "type" = ? LIMIT 1`,
                args: [scopedThreadId, args.type],
              });
              const serializedCurrent = currentResult.rows?.[0]?.serializedValue;
              const hasCurrent = serializedCurrent !== undefined && serializedCurrent !== null;
              const current = hasCurrent ? (JSON.parse(String(serializedCurrent)) as T) : undefined;
              const mutation = args.mutate(current);

              if (mutation.operation === 'keep') return mutation.result;

              let rowsAffected: number;
              if (mutation.operation === 'set') {
                const now = new Date().toISOString();
                const serializedNext = JSON.stringify(mutation.value ?? null);
                if (hasCurrent) {
                  const updated = await this.#client.execute({
                    sql: `UPDATE "${TABLE_THREAD_STATE}"
                          SET "value" = ?, "updatedAt" = ?
                          WHERE "threadId" = ? AND "type" = ? AND json("value") = ?`,
                    args: [serializedNext, now, scopedThreadId, args.type, String(serializedCurrent)],
                  });
                  rowsAffected = updated.rowsAffected;
                } else {
                  const inserted = await this.#client.execute({
                    sql: `INSERT INTO "${TABLE_THREAD_STATE}" ("threadId", "type", "value", "createdAt", "updatedAt")
                          VALUES (?, ?, ?, ?, ?)
                          ON CONFLICT ("threadId", "type") DO NOTHING`,
                    args: [scopedThreadId, args.type, serializedNext, now, now],
                  });
                  rowsAffected = inserted.rowsAffected;
                }
              } else if (!hasCurrent) {
                return mutation.result;
              } else {
                const deleted = await this.#client.execute({
                  sql: `DELETE FROM "${TABLE_THREAD_STATE}"
                        WHERE "threadId" = ? AND "type" = ? AND json("value") = ?`,
                  args: [scopedThreadId, args.type, String(serializedCurrent)],
                });
                rowsAffected = deleted.rowsAffected;
              }

              if (rowsAffected === 1) return mutation.result;
              // Yield so another contending client can complete before the
              // next optimistic attempt.
              await new Promise(resolve => setTimeout(resolve, Math.min(attempt + 1, 10)));
            }

            throw new Error(
              `Thread state mutation did not converge after ${MAX_MUTATION_CAS_ATTEMPTS} compare-and-swap attempts`,
            );
          }),
        'mutate thread state',
      );
    } catch (error) {
      throw new MastraError(
        {
          id: createStorageErrorId('LIBSQL', 'THREAD_STATE_MUTATE', 'FAILED'),
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
          details: { resourceId: args.resourceId, threadId: args.threadId, type: args.type },
        },
        error,
      );
    }
  }
}
