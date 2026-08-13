import { randomUUID } from 'node:crypto';
import { MessageList } from '@mastra/core/agent';
import type { MastraMessageContentV2 } from '@mastra/core/agent';
import { ErrorCategory, ErrorDomain, MastraError } from '@mastra/core/error';
import type { MastraMessageV1, MastraDBMessage, StorageThreadType } from '@mastra/core/memory';
import {
  MemoryStorage,
  normalizePerPage,
  calculatePagination,
  OBSERVATIONAL_MEMORY_TABLE_SCHEMA,
  TABLE_MESSAGES,
  TABLE_RESOURCES,
  TABLE_THREADS,
  TABLE_SCHEMAS,
  createStorageErrorId,
  storageMessageMatchesMetadataFilter,
  validateStorageMetadataFilter,
  applyWorkingMemorySnapshotUpdate,
  assertWorkingMemorySnapshotUnchanged,
  hasWorkingMemorySnapshotControls,
  preserveWorkingMemorySnapshotControls,
  readWorkingMemorySnapshot,
  retractObserverWorkingMemorySnapshot,
  writeWorkingMemorySnapshotMetadata,
  WorkingMemoryRevisionConflictError,
  WorkingMemoryValidationError,
} from '@mastra/core/storage';

/**
 * Local constant for the observational memory table name.
 * Defined locally to avoid a static import that crashes on older @mastra/core
 * versions that don't export TABLE_OBSERVATIONAL_MEMORY.
 */
const OM_TABLE = 'mastra_observational_memory' as const;
const POSTGRES_MAX_BIND_PARAMETERS = 65535;
// Keep in sync with the message INSERT column list in saveMessages.
const MESSAGE_INSERT_BIND_PARAMETERS = 8;
const MAX_MESSAGES_PER_INSERT = Math.floor(POSTGRES_MAX_BIND_PARAMETERS / MESSAGE_INSERT_BIND_PARAMETERS);
const POSTGRES_SQLSTATE_PATTERN = /^[0-9A-Z]{5}$/;
const SAFE_NETWORK_ERROR_CODES = new Set([
  'EAI_AGAIN',
  'ECONNREFUSED',
  'ECONNRESET',
  'EHOSTUNREACH',
  'ENETUNREACH',
  'ENOTFOUND',
  'ETIMEDOUT',
]);
const SAFE_PAGINATION_ERROR_MESSAGES = new Set([
  'page must be >= 0',
  'page must be 0 when perPage is false',
  'page value too large',
  'perPage must be >= 0',
  'perPage must be false or a safe integer',
]);

class ObservationalMemoryGenerationConflictError extends Error {
  constructor() {
    super('Observational memory generation is no longer current.');
    this.name = 'ObservationalMemoryGenerationConflictError';
  }
}

/**
 * Columns added to the OM table after its initial release.
 * Used in `alterTable({ ifNotExists })` so that databases created on older
 * versions get the new columns automatically.
 *
 * When you add a column to OBSERVATIONAL_MEMORY_SCHEMA in @mastra/core,
 * you MUST also add it here — the unit test `om-migration-columns.test.ts`
 * will fail otherwise.
 */
export const OM_MIGRATION_COLUMNS: string[] = [
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
  'metadata',
];

/**
 * The OM schema is imported statically above: the peer dependency range
 * (`@mastra/core >= 1.58.0-alpha.11`) guarantees the export exists. This used to be a
 * dynamic `require` guarded by `typeof require === 'function'` for older core
 * versions, but esbuild rewrites the bare `require` identifier in the ESM
 * bundle to a shim that always throws, and the silent catch meant the
 * published ESM build skipped creating the OM table entirely (#18954).
 */
const _omTableSchema: Record<string, Record<string, any>> = OBSERVATIONAL_MEMORY_TABLE_SCHEMA;
import type {
  StorageResourceType,
  StorageListMessagesInput,
  StorageListMessagesByResourceIdInput,
  StorageListMessagesOutput,
  StorageListThreadsInput,
  StorageListThreadsOutput,
  CreateIndexOptions,
  StorageCloneThreadInput,
  StorageCloneThreadOutput,
  ThreadCloneMetadata,
  ObservationalMemoryRecord,
  ObservationalMemoryHistoryOptions,
  BufferedObservationChunk,
  CreateObservationalMemoryInput,
  UpdateActiveObservationsInput,
  UpdateBufferedObservationsInput,
  SwapBufferedToActiveInput,
  SwapBufferedToActiveResult,
  UpdateBufferedReflectionInput,
  SwapBufferedReflectionToActiveInput,
  CreateReflectionGenerationInput,
  ObservationalMemoryWriteGuard,
  ObservationalMemoryRetractionReceipt,
  RetractObservationalMemoryInput,
  RetractObservationalMemoryResult,
  UpdateObservationalMemoryConfigInput,
  PruneOptions,
  PruneResult,
  RetentionTablesDescriptor,
  TableRetentionPolicy,
  TABLE_NAMES,
  ApplyWorkingMemoryUpdateInput,
  WorkingMemorySnapshot,
  WorkingMemorySnapshotInput,
} from '@mastra/core/storage';
import { parseSqlIdentifier } from '@mastra/core/utils';
import type { TxClient } from '../../client';
import {
  PgDB,
  resolvePgConfig,
  generateTableSQL,
  generateIndexSQL,
  getSchemaName as dbGetSchemaName,
  getTableName as dbGetTableName,
} from '../../db';
import type { PgDomainConfig } from '../../db';
import { runPrune, runBatchedDelete, resolveTargets } from '../../retention';

// Database row type that includes timezone-aware columns
type MessageRowFromDB = {
  id: string;
  content: string | any;
  role: string;
  type?: string;
  createdAt: Date | string;
  createdAtZ?: Date | string;
  threadId: string;
  resourceId: string;
};

function getManagedWorkingMemoryScope(config: unknown): 'thread' | 'resource' | undefined {
  let parsed = config;
  if (typeof parsed === 'string') {
    try {
      parsed = JSON.parse(parsed);
    } catch {
      return undefined;
    }
  }
  if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) {
    return undefined;
  }
  const scope = (parsed as Record<string, unknown>)._managedWorkingMemoryScope;
  return scope === 'thread' || scope === 'resource' ? scope : undefined;
}

function parseMetadata(value: unknown): Record<string, unknown> {
  if (value === null || value === undefined) return {};
  if (typeof value === 'string') {
    try {
      const parsed: unknown = JSON.parse(value);
      if (typeof parsed === 'object' && parsed !== null && !Array.isArray(parsed)) {
        return parsed as Record<string, unknown>;
      }
    } catch {
      throw new WorkingMemoryValidationError('Stored metadata is invalid.');
    }
    throw new WorkingMemoryValidationError('Stored metadata is invalid.');
  }
  if (typeof value === 'object' && !Array.isArray(value)) return value as Record<string, unknown>;
  throw new WorkingMemoryValidationError('Stored metadata is invalid.');
}

function asRecord(value: unknown): Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value) ? (value as Record<string, unknown>) : {};
}

function mergeThreadMetadataPreservingWorkingMemory(
  current: Record<string, unknown>,
  update: Record<string, unknown>,
): Record<string, unknown> {
  const merged = { ...current, ...update };
  const currentMastra = asRecord(current.mastra);
  const updateMastra = asRecord(update.mastra);
  if (Object.keys(currentMastra).length > 0 || Object.keys(updateMastra).length > 0) {
    merged.mastra = { ...currentMastra, ...updateMastra };
  }
  const currentValue = typeof current.workingMemory === 'string' ? current.workingMemory : null;
  assertWorkingMemorySnapshotUnchanged({
    currentValue,
    currentMetadata: current,
    proposedValue: update.workingMemory,
    proposedValueProvided: Object.prototype.hasOwnProperty.call(update, 'workingMemory'),
    proposedMetadata: update,
  });
  if (!hasWorkingMemorySnapshotControls(current)) return merged;

  const preserved = preserveWorkingMemorySnapshotControls(current, merged);
  if (Object.prototype.hasOwnProperty.call(current, 'workingMemory')) preserved.workingMemory = current.workingMemory;
  else delete preserved.workingMemory;
  return preserved;
}

function replaceThreadMetadataPreservingWorkingMemory(
  current: Record<string, unknown>,
  proposed: Record<string, unknown> | undefined,
): Record<string, unknown> {
  const next = proposed ?? {};
  const currentValue = typeof current.workingMemory === 'string' ? current.workingMemory : null;
  assertWorkingMemorySnapshotUnchanged({
    currentValue,
    currentMetadata: current,
    proposedValue: proposed?.workingMemory,
    proposedValueProvided: proposed !== undefined && Object.prototype.hasOwnProperty.call(proposed, 'workingMemory'),
    proposedMetadata: proposed,
  });
  if (!hasWorkingMemorySnapshotControls(current)) return next;
  const preserved = preserveWorkingMemorySnapshotControls(current, next);
  if (Object.prototype.hasOwnProperty.call(current, 'workingMemory')) preserved.workingMemory = current.workingMemory;
  return preserved;
}

function mergeObservationalThreadMetadata(
  current: Record<string, unknown>,
  update: Record<string, unknown>,
): Record<string, unknown> {
  const currentMastra = asRecord(current.mastra);
  const updateMastra = asRecord(update.mastra);
  if (!Object.prototype.hasOwnProperty.call(updateMastra, 'om')) return current;
  return { ...current, mastra: { ...currentMastra, om: updateMastra.om } };
}

function getSchemaName(schema?: string) {
  return schema ? `"${schema}"` : '"public"';
}

function getTableName({ indexName, schemaName }: { indexName: string; schemaName?: string }) {
  const quotedIndexName = `"${indexName}"`;
  return schemaName ? `${schemaName}.${quotedIndexName}` : quotedIndexName;
}

/**
 * Generate SQL placeholder string for IN clauses.
 * @param count - Number of placeholders to generate
 * @param startIndex - Starting index for placeholders (default: 1)
 * @returns Comma-separated placeholder string, e.g. "$1, $2, $3"
 */
function inPlaceholders(count: number, startIndex = 1): string {
  return Array.from({ length: count }, (_, i) => `$${i + startIndex}`).join(', ');
}

/**
 * Bind dates as UTC strings because node-postgres serializes Date parameters
 * for TIMESTAMP columns using the process's local timezone.
 */
function toUtcISOString(date: Date): string {
  return date.toISOString();
}

function dedupeMessagesForSave(messages: MastraDBMessage[]): MastraDBMessage[] {
  const deduped = new Map<string, MastraDBMessage>();
  for (const message of messages) {
    const existing = deduped.get(message.id);
    if (existing) {
      deduped.set(message.id, {
        ...message,
        createdAt: existing.createdAt,
      });
    } else {
      deduped.set(message.id, {
        ...message,
        createdAt: message.createdAt || new Date(),
      });
    }
  }
  return Array.from(deduped.values());
}

export class MemoryPG extends MemoryStorage {
  readonly supportsObservationalMemory = true;
  readonly supportsAtomicObservationalMemoryRetraction = true;
  readonly supportsRevisionedWorkingMemory = true;
  readonly supportsThreadUpdatedBeforeFilter = true;

  /**
   * Retention-eligible tables. `threads`, `messages`, and `resources` all anchor
   * on the timezone-aware `createdAtZ` mirror column (kept in sync by triggers),
   * and are indexed for fast batched deletes. Cascade order is enforced in
   * `prune()` (children before threads), not here. Observational memory has no
   * timestamp anchor and is deliberately excluded.
   */
  static override readonly retentionTables: RetentionTablesDescriptor = {
    messages: { table: TABLE_MESSAGES, column: 'createdAtZ', indexed: true },
    resources: { table: TABLE_RESOURCES, column: 'createdAtZ', indexed: true },
    threads: { table: TABLE_THREADS, column: 'createdAtZ', indexed: true },
  };

  #db: PgDB;
  #schema: string;
  #skipDefaultIndexes?: boolean;
  #indexes?: CreateIndexOptions[];

  /** Tables managed by this domain */
  static readonly MANAGED_TABLES = [TABLE_THREADS, TABLE_MESSAGES, TABLE_RESOURCES, OM_TABLE] as const;

  constructor(config: PgDomainConfig) {
    super();
    const { client, schemaName, skipDefaultIndexes, indexes } = resolvePgConfig(config);
    this.#db = new PgDB({ client, schemaName, skipDefaultIndexes });
    this.#schema = schemaName || 'public';
    this.#skipDefaultIndexes = skipDefaultIndexes;
    // Filter indexes to only those for tables managed by this domain
    this.#indexes = indexes?.filter(idx => (MemoryPG.MANAGED_TABLES as readonly string[]).includes(idx.table));
  }

  async init(): Promise<void> {
    await this.#db.createTable({ tableName: TABLE_THREADS, schema: TABLE_SCHEMAS[TABLE_THREADS] });
    await this.#db.createTable({ tableName: TABLE_MESSAGES, schema: TABLE_SCHEMAS[TABLE_MESSAGES] });
    await this.#db.createTable({ tableName: TABLE_RESOURCES, schema: TABLE_SCHEMAS[TABLE_RESOURCES] });

    // Reuse the module-level `_omTableSchema` (static import). Don't switch
    // this to `await import('@mastra/core/storage')`: that used to deadlock
    // `mastra build` output, because bundlers rewrite the dynamic import to
    // point at the entry chunk that statically depends on this file, so the
    // cycle never resolves when storage initializes during module
    // evaluation (#18298).
    const omSchema = _omTableSchema?.[OM_TABLE];

    if (omSchema) {
      await this.#db.createTable({
        tableName: OM_TABLE as any,
        schema: omSchema,
      });
      // Add new OM columns for backwards compatibility with existing databases
      await this.#db.alterTable({
        tableName: OM_TABLE as any,
        schema: omSchema,
        ifNotExists: OM_MIGRATION_COLUMNS,
      });
    }
    await this.#db.alterTable({
      tableName: TABLE_MESSAGES,
      schema: TABLE_SCHEMAS[TABLE_MESSAGES],
      ifNotExists: ['resourceId'],
    });
    if (omSchema) {
      // Create index on lookupKey for efficient OM queries
      const omTableName = getTableName({
        indexName: OM_TABLE,
        schemaName: getSchemaName(this.#schema),
      });
      await this.#db.createIndexFromStatement(
        'idx_om_lookup_key',
        `CREATE INDEX IF NOT EXISTS idx_om_lookup_key ON ${omTableName} ("lookupKey")`,
      );
    }
    await this.createDefaultIndexes();
    await this.createCustomIndexes();
  }

  /**
   * Lazily ensures a btree index exists on each configured policy's retention
   * anchor column so age-based `prune()` deletes stay fast on large tables.
   * Called from the prune path (not init) so only deployments that configure
   * retention pay the index's write/disk overhead. Best-effort: failures are
   * logged and pruning proceeds (correct, just slower).
   * Created even with `skipDefaultIndexes` — retention is an explicit opt-in,
   * so its supporting index is not part of the default index set.
   */
  private async ensureRetentionIndexes(policies: Record<string, TableRetentionPolicy>): Promise<void> {
    const prefix = this.#schema && this.#schema !== 'public' ? `${this.#schema}_` : '';
    for (const [key, entry] of Object.entries(MemoryPG.retentionTables)) {
      if (!entry.indexed || !policies[key]) continue;
      try {
        await this.#db.ensureIndex({
          indexName: `${prefix}mastra_${key}_retention_idx`,
          tableName: entry.table as TABLE_NAMES,
          column: entry.column,
        });
      } catch (error) {
        this.logger?.warn?.(`Failed to create retention index for ${entry.table}:`, error);
      }
    }
  }

  /**
   * Returns default index definitions for the memory domain tables.
   * @param schemaPrefix - Prefix for index names (e.g. "my_schema_" or "")
   */
  static getDefaultIndexDefs(schemaPrefix: string): CreateIndexOptions[] {
    return [
      {
        name: `${schemaPrefix}mastra_threads_resourceid_createdat_idx`,
        table: TABLE_THREADS,
        columns: ['resourceId', 'createdAt DESC'],
      },
      {
        name: `${schemaPrefix}mastra_messages_thread_id_createdat_idx`,
        table: TABLE_MESSAGES,
        columns: ['thread_id', 'createdAt DESC'],
      },
    ];
  }

  /**
   * Returns all DDL statements for this domain: tables (threads, messages, resources, OM), indexes.
   * Used by exportSchemas to produce a complete, reproducible schema export.
   */
  static getExportDDL(schemaName?: string): string[] {
    const statements: string[] = [];
    const parsedSchema = schemaName ? parseSqlIdentifier(schemaName, 'schema name') : '';
    const schemaPrefix = parsedSchema && parsedSchema !== 'public' ? `${parsedSchema}_` : '';
    const quotedSchemaName = dbGetSchemaName(schemaName);

    // Tables: threads, messages, resources
    for (const tableName of [TABLE_THREADS, TABLE_MESSAGES, TABLE_RESOURCES] as const) {
      statements.push(
        generateTableSQL({
          tableName,
          schema: TABLE_SCHEMAS[tableName],
          schemaName,
          includeAllConstraints: true,
        }),
      );
    }

    // Observational memory table (if schema available in this version of core)
    const omSchema = _omTableSchema?.[OM_TABLE];
    if (omSchema) {
      statements.push(
        generateTableSQL({
          tableName: OM_TABLE as any,
          schema: omSchema,
          schemaName,
          includeAllConstraints: true,
        }),
      );
      // idx_om_lookup_key index
      const fullOmTableName = dbGetTableName({ indexName: OM_TABLE, schemaName: quotedSchemaName });
      const idxPrefix = schemaPrefix ? `${schemaPrefix}` : '';
      statements.push(
        `CREATE INDEX IF NOT EXISTS "${idxPrefix}idx_om_lookup_key" ON ${fullOmTableName} ("lookupKey");`,
      );
    }

    // Default indexes
    for (const idx of MemoryPG.getDefaultIndexDefs(schemaPrefix)) {
      statements.push(generateIndexSQL(idx, schemaName));
    }

    return statements;
  }

  /**
   * Returns default index definitions for this instance's schema.
   */
  getDefaultIndexDefinitions(): CreateIndexOptions[] {
    const schemaPrefix = this.#schema !== 'public' ? `${this.#schema}_` : '';
    return MemoryPG.getDefaultIndexDefs(schemaPrefix);
  }

  /**
   * Creates default indexes for optimal query performance.
   */
  async createDefaultIndexes(): Promise<void> {
    if (this.#skipDefaultIndexes) {
      return;
    }

    for (const indexDef of this.getDefaultIndexDefinitions()) {
      try {
        await this.#db.createIndex(indexDef);
      } catch (error) {
        // Log but continue - indexes are performance optimizations
        this.logger?.warn?.(`Failed to create index ${indexDef.name}:`, error);
      }
    }
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
    await this.#db.clearTable({ tableName: TABLE_MESSAGES });
    await this.#db.clearTable({ tableName: TABLE_THREADS });
    await this.#db.clearTable({ tableName: TABLE_RESOURCES });
  }

  /**
   * Deletes rows older than the configured `maxAge` per table, in bounded,
   * batched, cancellable chunks. Tables are pruned children-first (messages and
   * resources before threads) since PostgreSQL has no FK cascade in this schema.
   * Unset tables are kept forever.
   *
   * When a `messages` policy is set, semantic-recall embeddings for pruned
   * messages are also swept from same-schema `memory_messages*` vector tables
   * (best-effort, mirroring `deleteThread`). Embeddings held in an external
   * vector store are out of reach and must be pruned by the operator.
   */
  async prune(policies: Record<string, TableRetentionPolicy>, options?: PruneOptions): Promise<PruneResult[]> {
    await this.ensureRetentionIndexes(policies);
    const targets = resolveTargets({
      policies,
      descriptor: MemoryPG.retentionTables,
      order: ['messages', 'resources', 'threads'],
    });
    const results = await runPrune({ db: this.#db, domain: 'memory', targets, options });
    if (policies['messages']) {
      await this.pruneOrphanedVectorRows(policies['messages'], options);
    }
    return results;
  }

  /**
   * Best-effort sweep of semantic-recall vector rows whose source message no
   * longer exists (e.g. it was just pruned), so recall doesn't keep returning
   * embeddings that resolve to nothing. Only same-schema default vector tables
   * (`memory_messages*`) are covered — the same set `deleteThread` cleans up.
   * Failures are logged, never thrown: vector cleanup must not fail the prune.
   */
  private async pruneOrphanedVectorRows(policy: TableRetentionPolicy, options?: PruneOptions): Promise<void> {
    try {
      const schemaName = this.#schema || 'public';
      const vectorTables = await this.#db.client.manyOrNone<{ tablename: string }>(
        `
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = $1
        AND (tablename = 'memory_messages' OR tablename LIKE 'memory_messages_%')
      `,
        [schemaName],
      );

      const messagesTable = getTableName({ indexName: TABLE_MESSAGES, schemaName: getSchemaName(this.#schema) });
      for (const { tablename } of vectorTables) {
        const vectorTableName = getTableName({ indexName: tablename, schemaName: getSchemaName(this.#schema) });
        await runBatchedDelete({
          deleteBatch: async limit => {
            const result = await this.#db.client.query(
              `
              DELETE FROM ${vectorTableName}
              WHERE ctid IN (
                SELECT v.ctid FROM ${vectorTableName} v
                WHERE v.metadata->>'message_id' IS NOT NULL
                AND NOT EXISTS (SELECT 1 FROM ${messagesTable} m WHERE m.id = v.metadata->>'message_id')
                LIMIT $1
              )
            `,
              [limit],
            );
            return result.rowCount ?? 0;
          },
          batchSize: policy.batchSize ?? 1000,
          options,
        });
      }
    } catch (error) {
      this.logger?.warn?.('Failed to sweep orphaned semantic-recall vector rows after prune:', error);
    }
  }

  /**
   * Normalizes message row from database by applying createdAtZ fallback
   */
  private normalizeMessageRow(row: MessageRowFromDB): Omit<MessageRowFromDB, 'createdAtZ'> {
    return {
      id: row.id,
      content: row.content,
      role: row.role,
      type: row.type,
      createdAt: row.createdAtZ || row.createdAt,
      threadId: row.threadId,
      resourceId: row.resourceId,
    };
  }

  async getThreadById({
    threadId,
    resourceId,
  }: {
    threadId: string;
    resourceId?: string;
  }): Promise<StorageThreadType | null> {
    try {
      const tableName = getTableName({ indexName: TABLE_THREADS, schemaName: getSchemaName(this.#schema) });

      let query = `SELECT * FROM ${tableName} WHERE id = $1`;
      let params: any[] = [threadId];

      if (resourceId !== undefined) {
        query += ` AND "resourceId" = $2`;
        params.push(resourceId);
      }

      const thread = await this.#db.client.oneOrNone<StorageThreadType & { createdAtZ: Date; updatedAtZ: Date }>(
        query,
        params,
      );

      if (!thread) {
        return null;
      }

      return {
        id: thread.id,
        resourceId: thread.resourceId,
        title: thread.title,
        metadata: typeof thread.metadata === 'string' ? JSON.parse(thread.metadata) : thread.metadata,
        createdAt: thread.createdAtZ || thread.createdAt,
        updatedAt: thread.updatedAtZ || thread.updatedAt,
      };
    } catch (error) {
      throw new MastraError(
        {
          id: createStorageErrorId('PG', 'GET_THREAD_BY_ID', 'FAILED'),
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
          details: {
            threadId,
          },
        },
        error,
      );
    }
  }

  public async listThreads(args: StorageListThreadsInput): Promise<StorageListThreadsOutput> {
    const { page = 0, perPage: perPageInput, orderBy, filter } = args;

    try {
      // Validate pagination input before normalization
      // This ensures page === 0 when perPageInput === false
      this.validatePaginationInput(page, perPageInput === undefined ? 100 : perPageInput);
    } catch (error) {
      throw new MastraError({
        id: createStorageErrorId('PG', 'LIST_THREADS', 'INVALID_PAGE'),
        domain: ErrorDomain.STORAGE,
        category: ErrorCategory.USER,
        text: this.getSafePaginationErrorText(error),
        details: this.getSafePaginationDetails(page, perPageInput),
      });
    }

    const perPage = normalizePerPage(perPageInput, 100);

    if (
      filter?.updatedBefore !== undefined &&
      (!(filter.updatedBefore instanceof Date) || Number.isNaN(filter.updatedBefore.getTime()))
    ) {
      throw new MastraError({
        id: createStorageErrorId('PG', 'LIST_THREADS', 'INVALID_UPDATED_BEFORE'),
        domain: ErrorDomain.STORAGE,
        category: ErrorCategory.USER,
        text: 'updatedBefore must be a valid Date',
        details: { hasUpdatedBeforeFilter: true },
      });
    }

    // Validate metadata keys to prevent SQL injection
    try {
      this.validateMetadataKeys(filter?.metadata);
    } catch {
      throw new MastraError({
        id: createStorageErrorId('PG', 'LIST_THREADS', 'INVALID_METADATA_KEY'),
        domain: ErrorDomain.STORAGE,
        category: ErrorCategory.USER,
        text: 'Invalid metadata filter',
        details: { hasMetadataFilter: filter?.metadata !== undefined },
      });
    }

    const { field, direction } = this.parseOrderBy(orderBy);
    const { offset, perPage: perPageForResponse } = calculatePagination(page, perPageInput, perPage);

    try {
      const tableName = getTableName({ indexName: TABLE_THREADS, schemaName: getSchemaName(this.#schema) });
      const whereClauses: string[] = [];
      const queryParams: any[] = [];
      let paramIndex = 1;

      // Add resourceId filter if provided
      if (filter?.resourceId) {
        whereClauses.push(`"resourceId" = $${paramIndex}`);
        queryParams.push(filter.resourceId);
        paramIndex++;
      }

      if (filter?.updatedBefore) {
        whereClauses.push(`COALESCE("updatedAtZ", "updatedAt") < $${paramIndex}`);
        queryParams.push(filter.updatedBefore);
        paramIndex++;
      }

      // Add metadata filters if provided (AND logic)
      // Uses JSONB containment (@>) to avoid SQL injection and correctly match all value types including null
      // metadata column is TEXT type storing JSON, so we need to cast to jsonb first
      if (filter?.metadata && Object.keys(filter.metadata).length > 0) {
        for (const [key, value] of Object.entries(filter.metadata)) {
          // Use JSONB containment operator - no key interpolation needed
          whereClauses.push(`metadata::jsonb @> $${paramIndex}::jsonb`);
          // Build a small JSON object for each key-value pair
          queryParams.push(JSON.stringify({ [key]: value }));
          paramIndex++;
        }
      }

      const whereClause = whereClauses.length > 0 ? `WHERE ${whereClauses.join(' AND ')}` : '';
      const baseQuery = `FROM ${tableName} ${whereClause}`;

      const countQuery = `SELECT COUNT(*) ${baseQuery}`;
      const countResult = await this.#db.client.one(countQuery, queryParams);
      const total = parseInt(countResult.count, 10);

      if (total === 0) {
        return {
          threads: [],
          total: 0,
          page,
          perPage: perPageForResponse,
          hasMore: false,
        };
      }

      const limitValue = perPageInput === false ? total : perPage;
      // Select both standard and timezone-aware columns (*Z) for proper UTC timestamp handling
      const dataQuery = `SELECT id, "resourceId", title, metadata, "createdAt", "createdAtZ", "updatedAt", "updatedAtZ" ${baseQuery} ORDER BY COALESCE("${field}Z", "${field}") ${direction} LIMIT $${paramIndex} OFFSET $${paramIndex + 1}`;
      const rows = await this.#db.client.manyOrNone<StorageThreadType & { createdAtZ: Date; updatedAtZ: Date }>(
        dataQuery,
        [...queryParams, limitValue, offset],
      );

      const threads = (rows || []).map(thread => ({
        id: thread.id,
        resourceId: thread.resourceId,
        title: thread.title,
        metadata: typeof thread.metadata === 'string' ? JSON.parse(thread.metadata) : thread.metadata,
        // Use timezone-aware columns (*Z) for correct UTC timestamps, with fallback for legacy data
        createdAt: thread.createdAtZ || thread.createdAt,
        updatedAt: thread.updatedAtZ || thread.updatedAt,
      }));

      return {
        threads,
        total,
        page,
        perPage: perPageForResponse,
        hasMore: perPageInput === false ? false : offset + perPage < total,
      };
    } catch (rawError) {
      throw this.createAndTrackSafeReadError({
        operation: 'LIST_THREADS',
        text: 'Failed to list PostgreSQL threads',
        details: {
          hasResourceIdFilter: Boolean(filter?.resourceId),
          hasMetadataFilter: Boolean(filter?.metadata),
          page,
        },
        rawError,
      });
    }
  }

  async saveThread({ thread }: { thread: StorageThreadType }): Promise<StorageThreadType> {
    const tableName = getTableName({ indexName: TABLE_THREADS, schemaName: getSchemaName(this.#schema) });
    try {
      const createdAt = toUtcISOString(thread.createdAt);
      const updatedAt = toUtcISOString(thread.updatedAt);
      return await this.#db.client.tx(async t => {
        await this.lockWorkingMemoryTarget(t, 'thread', thread.id);
        const currentRow = await t.oneOrNone<{ metadata: unknown }>(
          `SELECT metadata FROM ${tableName} WHERE id = $1 FOR UPDATE`,
          [thread.id],
        );
        const currentMetadata = currentRow ? parseMetadata(currentRow.metadata) : undefined;
        const metadata = currentMetadata
          ? replaceThreadMetadataPreservingWorkingMemory(currentMetadata, thread.metadata)
          : (thread.metadata ?? {});
        const row = await t.one<StorageThreadType & { createdAtZ: Date | string; updatedAtZ: Date | string }>(
          `INSERT INTO ${tableName} (
            id, "resourceId", title, metadata, "createdAt", "createdAtZ", "updatedAt", "updatedAtZ"
          ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
          ON CONFLICT (id) DO UPDATE SET
            "resourceId" = EXCLUDED."resourceId",
            title = EXCLUDED.title,
            metadata = EXCLUDED.metadata,
            "createdAt" = EXCLUDED."createdAt",
            "createdAtZ" = EXCLUDED."createdAtZ",
            "updatedAt" = EXCLUDED."updatedAt",
            "updatedAtZ" = EXCLUDED."updatedAtZ"
          RETURNING *`,
          [
            thread.id,
            thread.resourceId,
            thread.title,
            JSON.stringify(metadata),
            createdAt,
            createdAt,
            updatedAt,
            updatedAt,
          ],
        );
        return {
          id: row.id,
          resourceId: row.resourceId,
          title: row.title,
          metadata: parseMetadata(row.metadata),
          createdAt: new Date(row.createdAtZ || row.createdAt),
          updatedAt: new Date(row.updatedAtZ || row.updatedAt),
        };
      });
    } catch (error) {
      if (error instanceof WorkingMemoryValidationError) throw error;
      throw new MastraError(
        {
          id: createStorageErrorId('PG', 'SAVE_THREAD', 'FAILED'),
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
          details: {
            threadId: thread.id,
          },
        },
        error,
      );
    }
  }

  async updateThread({
    id,
    title,
    metadata,
  }: {
    id: string;
    title?: string;
    metadata?: Record<string, unknown>;
  }): Promise<StorageThreadType> {
    const threadTableName = getTableName({ indexName: TABLE_THREADS, schemaName: getSchemaName(this.#schema) });
    try {
      return await this.#db.client.tx(async t => {
        await this.lockWorkingMemoryTarget(t, 'thread', id);
        const existingThread = await t.oneOrNone<StorageThreadType & { createdAtZ: Date; updatedAtZ: Date }>(
          `SELECT * FROM ${threadTableName} WHERE id = $1 FOR UPDATE`,
          [id],
        );
        if (!existingThread) {
          throw new MastraError({
            id: createStorageErrorId('PG', 'UPDATE_THREAD', 'FAILED'),
            domain: ErrorDomain.STORAGE,
            category: ErrorCategory.USER,
            text: `Thread ${id} not found`,
            details: { threadId: id, title: title ?? null },
          });
        }
        const currentMetadata = parseMetadata(existingThread.metadata);
        const mergedMetadata =
          metadata === undefined
            ? currentMetadata
            : mergeThreadMetadataPreservingWorkingMemory(currentMetadata, metadata);
        const nowStr = toUtcISOString(new Date());
        const thread = await t.one<StorageThreadType & { createdAtZ: Date; updatedAtZ: Date }>(
          `UPDATE ${threadTableName}
                    SET
                        title = COALESCE($1, title),
                        metadata = $2,
                        "updatedAt" = $3,
                        "updatedAtZ" = $4
                    WHERE id = $5
                    RETURNING *
                `,
          [title ?? null, mergedMetadata, nowStr, nowStr, id],
        );

        return {
          id: thread.id,
          resourceId: thread.resourceId,
          title: thread.title,
          metadata: parseMetadata(thread.metadata),
          createdAt: thread.createdAtZ || thread.createdAt,
          updatedAt: thread.updatedAtZ || thread.updatedAt,
        };
      });
    } catch (error) {
      if (error instanceof MastraError) throw error;
      if (error instanceof WorkingMemoryValidationError) throw error;
      throw new MastraError(
        {
          id: createStorageErrorId('PG', 'UPDATE_THREAD', 'FAILED'),
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
          details: {
            threadId: id,
            title: title ?? null,
          },
        },
        error,
      );
    }
  }

  async deleteThread({
    threadId,
    observationalMemoryRetractions,
  }: {
    threadId: string;
    observationalMemoryRetractions?: ObservationalMemoryRetractionReceipt[];
  }): Promise<void> {
    try {
      const tableName = getTableName({ indexName: TABLE_MESSAGES, schemaName: getSchemaName(this.#schema) });
      const threadTableName = getTableName({ indexName: TABLE_THREADS, schemaName: getSchemaName(this.#schema) });
      let committedRetraction: ObservationalMemoryRetractionReceipt | undefined;
      await this.#db.client.tx(async t => {
        const thread = await t.oneOrNone<{ resourceId: string }>(
          `SELECT "resourceId" FROM ${threadTableName} WHERE id = $1`,
          [threadId],
        );
        const schemaName = this.#schema || 'public';
        const omTableExists = await t.oneOrNone<{ tablename: string }>(
          `SELECT tablename FROM pg_tables WHERE schemaname = $1 AND tablename = $2`,
          [schemaName, OM_TABLE],
        );
        let retraction: RetractObservationalMemoryResult | undefined;
        if (thread?.resourceId) {
          if (omTableExists !== null) {
            const input = {
              resourceId: thread.resourceId,
              threadId,
            };
            retraction = await this.retractObservationalMemoryInTransaction(t, input);
            committedRetraction = { input, result: retraction };
          } else {
            await this.lockObservationalMemoryResource(t, thread.resourceId);
          }
        }

        await t.none(`DELETE FROM ${tableName} WHERE thread_id = $1`, [threadId]);

        const vectorTables = await t.manyOrNone<{ tablename: string }>(
          `
          SELECT tablename
          FROM pg_tables
          WHERE schemaname = $1
          AND (
            tablename = 'memory_messages'
            OR tablename LIKE 'memory_messages_%'
            OR tablename = 'memory_observations'
            OR tablename LIKE 'memory_observations_%'
          )
        `,
          [schemaName],
        );

        for (const { tablename } of vectorTables) {
          const vectorTableName = getTableName({ indexName: tablename, schemaName: getSchemaName(this.#schema) });
          const isObservationTable =
            tablename === 'memory_observations' || tablename.startsWith('memory_observations_');
          const clearedResourceId = retraction?.clearedScopes.includes('resource') ? thread?.resourceId : undefined;
          if (isObservationTable && clearedResourceId) {
            await t.none(`DELETE FROM ${vectorTableName} WHERE metadata->>'resource_id' = $1`, [clearedResourceId]);
          } else {
            await t.none(`DELETE FROM ${vectorTableName} WHERE metadata->>'thread_id' = $1`, [threadId]);
          }
        }

        await t.none(`DELETE FROM ${threadTableName} WHERE id = $1`, [threadId]);
      });
      if (committedRetraction) {
        observationalMemoryRetractions?.push(committedRetraction);
      }
    } catch (error) {
      throw new MastraError(
        {
          id: createStorageErrorId('PG', 'DELETE_THREAD', 'FAILED'),
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
          details: {
            threadId,
          },
        },
        error,
      );
    }
  }

  /**
   * Fetches messages around target messages using cursor-based pagination.
   *
   * This replaces the previous ROW_NUMBER() approach which caused severe performance
   * issues on large tables (see GitHub issue #11150). The old approach required
   * scanning and sorting ALL messages in a thread to assign row numbers.
   *
   * The current approach uses two phases for optimal performance:
   * 1. Batch-fetch all target messages' metadata (thread_id, createdAt) in one query
   * 2. Build cursor subqueries using "createdAt" directly (not COALESCE) so that
   *    the existing (thread_id, createdAt DESC) index can be used for index scans
   *    instead of sequential scans. This fixes GitHub issue #11702 where semantic
   *    recall latency scaled linearly with message count (~30s for 7.4k messages).
   */
  private _sortMessages(messages: MastraDBMessage[], field: string, direction: string): MastraDBMessage[] {
    return messages.sort((a, b) => {
      const aValue = field === 'createdAt' ? new Date(a.createdAt).getTime() : (a as any)[field];
      const bValue = field === 'createdAt' ? new Date(b.createdAt).getTime() : (b as any)[field];

      if (aValue == null && bValue == null) return a.id.localeCompare(b.id);
      if (aValue == null) return 1;
      if (bValue == null) return -1;

      if (aValue === bValue) {
        return a.id.localeCompare(b.id);
      }

      if (typeof aValue === 'number' && typeof bValue === 'number') {
        return direction === 'ASC' ? aValue - bValue : bValue - aValue;
      }
      return direction === 'ASC'
        ? String(aValue).localeCompare(String(bValue))
        : String(bValue).localeCompare(String(aValue));
    });
  }

  /**
   * Fetches included messages by ID, discovering their thread automatically.
   * This handles cross-thread includes where the include item doesn't specify a threadId.
   * When a resourceId is given, both the target lookup and the surrounding window stay
   * inside that resource, so an include never leaks another resource's messages.
   */
  private async _getIncludedMessages({
    include,
    resourceId,
  }: {
    include: StorageListMessagesInput['include'];
    resourceId?: string;
  }) {
    if (!include || include.length === 0) return null;

    const tableName = getTableName({ indexName: TABLE_MESSAGES, schemaName: getSchemaName(this.#schema) });
    const selectColumns = `id, content, role, type, "createdAt", "createdAtZ", thread_id AS "threadId", "resourceId"`;

    // Phase 1: Batch-fetch metadata for all target messages in a single query.
    // This eliminates the correlated subselects that previously ran per-subquery.
    const targetIds = include.map(inc => inc.id).filter(Boolean);
    if (targetIds.length === 0) return null;

    const idPlaceholders = targetIds.map((_, i) => '$' + (i + 1)).join(', ');
    const targetResourceCondition = resourceId ? ` AND "resourceId" = $${targetIds.length + 1}` : '';
    const targetRows = await this.#db.client.manyOrNone<{
      id: string;
      thread_id: string;
      createdAt: Date | string;
    }>(
      `SELECT id, thread_id, "createdAt" FROM ${tableName} WHERE id IN (${idPlaceholders})${targetResourceCondition}`,
      resourceId ? [...targetIds, resourceId] : targetIds,
    );

    if (targetRows.length === 0) return null;

    const targetMap = new Map(targetRows.map(r => [r.id, { threadId: r.thread_id, createdAt: r.createdAt }]));

    // Phase 2: Build cursor subqueries using materialized constants from Phase 1.
    // Uses "createdAt" directly instead of COALESCE("createdAtZ", "createdAt") so
    // the (thread_id, createdAt DESC) composite index covers the query.
    // createdAt and createdAtZ always store the same instant (createdAtZ is a TIMESTAMPTZ
    // copy for timezone-correctness), so using createdAt for ordering is safe.
    const unionQueries: string[] = [];
    const params: any[] = [];
    // resourceId is the same for every subquery, so bind it once as $1 and reference
    // that placeholder from each subquery instead of re-binding it per include item.
    let resourceCondition = '';
    if (resourceId) {
      params.push(resourceId);
      resourceCondition = ` AND m."resourceId" = $1`;
    }
    let paramIdx = params.length + 1;

    for (const inc of include) {
      const { id, withPreviousMessages = 0, withNextMessages = 0 } = inc;
      const target = targetMap.get(id);
      if (!target) continue;

      // Fetch the target message itself plus previous messages.
      // Uses createdAt <= target's createdAt, ordered DESC, limited to withPreviousMessages + 1
      const p1 = '$' + paramIdx;
      const p2 = '$' + (paramIdx + 1);
      const p3 = '$' + (paramIdx + 2);
      unionQueries.push(`(
        SELECT ${selectColumns}
        FROM ${tableName} m
        WHERE m.thread_id = ${p1}
          AND m."createdAt" <= ${p2}${resourceCondition}
        ORDER BY m."createdAt" DESC, m.id DESC
        LIMIT ${p3}
      )`);
      params.push(target.threadId, target.createdAt, withPreviousMessages + 1);
      paramIdx += 3;

      // Fetch messages after the target (only if requested)
      if (withNextMessages > 0) {
        const p4 = '$' + paramIdx;
        const p5 = '$' + (paramIdx + 1);
        const p6 = '$' + (paramIdx + 2);
        unionQueries.push(`(
          SELECT ${selectColumns}
          FROM ${tableName} m
          WHERE m.thread_id = ${p4}
            AND m."createdAt" > ${p5}${resourceCondition}
          ORDER BY m."createdAt" ASC, m.id ASC
          LIMIT ${p6}
        )`);
        params.push(target.threadId, target.createdAt, withNextMessages);
        paramIdx += 3;
      }
    }

    if (unionQueries.length === 0) return null;

    // When there's only one subquery, we don't need UNION ALL or an outer ORDER BY
    // (the subquery already has its own ORDER BY)
    // When there are multiple subqueries, we join them and sort the combined result
    let finalQuery: string;
    if (unionQueries.length === 1) {
      // Single query - just use it directly (remove outer parentheses for cleaner SQL)
      finalQuery = unionQueries[0]!.slice(1, -1); // Remove ( and )
    } else {
      // Multiple queries - UNION ALL and sort the result
      finalQuery = `SELECT * FROM (${unionQueries.join(' UNION ALL ')}) AS combined ORDER BY "createdAt" ASC, id ASC`;
    }
    const includedRows = await this.#db.client.manyOrNone(finalQuery, params);

    // Deduplicate results (messages may appear in multiple context windows)
    const seen = new Set<string>();
    const dedupedRows = includedRows.filter(row => {
      if (seen.has(row.id)) return false;
      seen.add(row.id);
      return true;
    });
    return dedupedRows;
  }

  private parseRow(row: MessageRowFromDB): MastraDBMessage {
    const normalized = this.normalizeMessageRow(row);
    let content = normalized.content;
    try {
      content = JSON.parse(normalized.content);
    } catch {
      // use content as is if it's not JSON
    }
    return {
      id: normalized.id,
      content,
      role: normalized.role as MastraDBMessage['role'],
      createdAt: new Date(normalized.createdAt as string),
      threadId: normalized.threadId,
      resourceId: normalized.resourceId,
      ...(normalized.type && normalized.type !== 'v2' ? { type: normalized.type } : {}),
    } satisfies MastraDBMessage;
  }

  private createAndTrackSafeReadError({
    operation,
    text,
    details,
    rawError,
  }: {
    // Pinned Prettier and Oxfmt disagree on this union's line wrapping.
    // prettier-ignore
    operation:
      | 'LIST_THREADS'
      | 'LIST_MESSAGES_BY_ID'
      | 'HAS_MESSAGES'
      | 'LIST_MESSAGES'
      | 'LIST_MESSAGES_BY_RESOURCE_ID';
    text: string;
    details: Record<string, string | number | boolean>;
    rawError: unknown;
  }): MastraError {
    const failureCode = this.getSafeReadFailureCode(rawError);
    const definition = {
      id: createStorageErrorId('PG', operation, 'FAILED'),
      domain: ErrorDomain.STORAGE,
      category: ErrorCategory.THIRD_PARTY,
      text,
      details: {
        ...details,
        ...(failureCode && { failureCode }),
      },
    } as const;
    // Do not retain the driver error as `cause`: Mastra error normalization can later add `toJSON` to a
    // plain Error, which would make driver messages, query text, or connection details serializable.
    const error = new MastraError(definition, new Error(text));
    this.logger?.error?.(error.toString());
    this.logger?.trackException(error);
    return error;
  }

  private getSafeReadFailureCode(error: unknown): string | undefined {
    try {
      if ((typeof error !== 'object' && typeof error !== 'function') || error === null) return undefined;
      const code = (error as { code?: unknown }).code;
      if (typeof code !== 'string') return undefined;
      if (SAFE_NETWORK_ERROR_CODES.has(code)) return code;
      if (POSTGRES_SQLSTATE_PATTERN.test(code)) return `SQLSTATE_${code.slice(0, 2)}`;
    } catch {
      // Ignore error-like values with throwing property accessors.
    }
    return undefined;
  }

  private getSafePaginationDetails(page: unknown, perPage: unknown) {
    const hasValidPage = typeof page === 'number' && Number.isFinite(page) && Number.isSafeInteger(page) && page >= 0;
    const hasValidPerPage =
      perPage === undefined ||
      perPage === false ||
      (typeof perPage === 'number' && Number.isFinite(perPage) && Number.isSafeInteger(perPage) && perPage >= 0);

    return {
      hasValidPage,
      hasValidPerPage,
      hasValidPaginationCombination: perPage !== false || page === 0,
    };
  }

  private getSafePaginationErrorText(error: unknown): string {
    try {
      if ((typeof error !== 'object' && typeof error !== 'function') || error === null) {
        return 'Invalid pagination parameters';
      }
      const message = (error as { message?: unknown }).message;
      return typeof message === 'string' && SAFE_PAGINATION_ERROR_MESSAGES.has(message)
        ? message
        : 'Invalid pagination parameters';
    } catch {
      // Error-like values may expose throwing accessors. Never reflect them.
      return 'Invalid pagination parameters';
    }
  }

  public async listMessagesById({ messageIds }: { messageIds: string[] }): Promise<{ messages: MastraDBMessage[] }> {
    if (messageIds.length === 0) return { messages: [] };
    const selectStatement = `SELECT id, content, role, type, "createdAt", "createdAtZ", thread_id AS "threadId", "resourceId"`;

    try {
      const tableName = getTableName({ indexName: TABLE_MESSAGES, schemaName: getSchemaName(this.#schema) });
      const query = `
        ${selectStatement} FROM ${tableName}
        WHERE id IN (${inPlaceholders(messageIds.length)})
        ORDER BY "createdAt" DESC
      `;
      const resultRows = await this.#db.client.manyOrNone(query, messageIds);

      const list = new MessageList().add(
        resultRows.map(row => this.parseRow(row)) as (MastraMessageV1 | MastraDBMessage)[],
        'memory',
      );
      return { messages: list.get.all.db() };
    } catch (rawError) {
      throw this.createAndTrackSafeReadError({
        operation: 'LIST_MESSAGES_BY_ID',
        text: 'Failed to list PostgreSQL messages by ID',
        details: { messageIdCount: messageIds.length },
        rawError,
      });
    }
  }

  public override async hasMessages({
    threadId,
    resourceId,
  }: Pick<StorageListMessagesInput, 'threadId' | 'resourceId'>): Promise<boolean> {
    const threadIds = (Array.isArray(threadId) ? threadId : [threadId]).filter(
      (id): id is string => typeof id === 'string',
    );

    if (threadIds.length === 0 || threadIds.some(id => !id.trim())) {
      throw new MastraError(
        {
          id: createStorageErrorId('PG', 'HAS_MESSAGES', 'INVALID_THREAD_ID'),
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
          details: {
            hasThreadId: threadId !== undefined && threadId !== null,
            threadIdCount: Array.isArray(threadId) ? threadId.length : 1,
          },
        },
        new Error('threadId must be a non-empty string or array of non-empty strings'),
      );
    }

    try {
      const tableName = getTableName({ indexName: TABLE_MESSAGES, schemaName: getSchemaName(this.#schema) });
      const conditions = [`thread_id IN (${inPlaceholders(threadIds.length)})`];
      const queryParams: unknown[] = [...threadIds];
      if (resourceId !== undefined) {
        conditions.push(`"resourceId" = $${queryParams.length + 1}`);
        queryParams.push(resourceId);
      }
      const result = await this.#db.client.one<{ exists: boolean }>(
        `SELECT EXISTS (SELECT 1 FROM ${tableName} WHERE ${conditions.join(' AND ')}) AS "exists"`,
        queryParams,
      );
      return result.exists;
    } catch (rawError) {
      throw this.createAndTrackSafeReadError({
        operation: 'HAS_MESSAGES',
        text: 'Failed to check for PostgreSQL messages',
        details: { hasResourceId: resourceId !== undefined, threadIdCount: threadIds.length },
        rawError,
      });
    }
  }

  /**
   * Reads one page of messages together with the total row count.
   *
   * `COUNT(*) OVER ()` reports the count over the whole WHERE result on the same
   * statement as the page, so the page costs one database round-trip instead of
   * two. The page and the count also come from one snapshot, so the count always
   * describes the returned rows. A separate `COUNT(*)` runs only when the page is
   * empty and the caller asked for a page after the last row, because a window
   * function has no row to carry the count on.
   */
  async #fetchMessagePage({
    selectStatement,
    tableName,
    whereClause,
    orderByStatement,
    queryParams,
    perPageInput,
    perPage,
    offset,
  }: {
    selectStatement: string;
    tableName: string;
    whereClause: string;
    orderByStatement: string;
    queryParams: any[];
    perPageInput: number | false | undefined;
    perPage: number;
    offset: number;
  }): Promise<{ total: number; messages: MessageRowFromDB[] }> {
    // `perPageInput === false` means "every row", so no LIMIT is applied.
    const limitClause =
      perPageInput === false ? '' : ` LIMIT $${queryParams.length + 1} OFFSET $${queryParams.length + 2}`;
    const dataParams = perPageInput === false ? queryParams : [...queryParams, perPage, offset];
    const rows =
      (await this.#db.client.manyOrNone<MessageRowFromDB & { __total?: string | number }>(
        `${selectStatement}, COUNT(*) OVER () AS "__total" FROM ${tableName} ${whereClause} ${orderByStatement}${limitClause}`,
        dataParams,
      )) || [];

    if (rows.length > 0) {
      return { total: Number(rows[0]!.__total), messages: rows };
    }
    if (offset === 0) {
      return { total: 0, messages: [] };
    }
    const countResult = await this.#db.client.one(`SELECT COUNT(*) FROM ${tableName} ${whereClause}`, queryParams);
    return { total: parseInt(countResult.count, 10), messages: [] };
  }

  public async listMessages(args: StorageListMessagesInput): Promise<StorageListMessagesOutput> {
    const { threadId, resourceId, include, filter, perPage: perPageInput, page = 0, orderBy } = args;

    const threadIds = (Array.isArray(threadId) ? threadId : [threadId]).filter(
      (id): id is string => typeof id === 'string',
    );

    if (threadIds.length === 0 || threadIds.some(id => !id.trim())) {
      throw new MastraError(
        {
          id: createStorageErrorId('PG', 'LIST_MESSAGES', 'INVALID_THREAD_ID'),
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
          details: {
            hasThreadId: threadId !== undefined && threadId !== null,
            threadIdCount: Array.isArray(threadId) ? threadId.length : 1,
          },
        },
        new Error('threadId must be a non-empty string or array of non-empty strings'),
      );
    }

    try {
      this.validatePaginationInput(page, perPageInput === undefined ? 40 : perPageInput);
    } catch (error) {
      throw new MastraError({
        id: createStorageErrorId('PG', 'LIST_MESSAGES', 'INVALID_PAGE'),
        domain: ErrorDomain.STORAGE,
        category: ErrorCategory.USER,
        text: this.getSafePaginationErrorText(error),
        details: {
          hasThreadId: true,
          threadIdCount: threadIds.length,
          ...this.getSafePaginationDetails(page, perPageInput),
        },
      });
    }

    const perPage = normalizePerPage(perPageInput, 40);
    const { offset, perPage: perPageForResponse } = calculatePagination(page, perPageInput, perPage);
    const metadataFilter = validateStorageMetadataFilter(filter?.metadata);

    try {
      const { field, direction } = this.parseOrderBy(orderBy, 'ASC');
      const orderByStatement = `ORDER BY COALESCE("${field}Z", "${field}") ${direction}`;

      const selectStatement = `SELECT id, content, role, type, "createdAt", "createdAtZ", thread_id AS "threadId", "resourceId"`;
      const tableName = getTableName({ indexName: TABLE_MESSAGES, schemaName: getSchemaName(this.#schema) });

      const conditions: string[] = [`thread_id IN (${inPlaceholders(threadIds.length)})`];
      const queryParams: any[] = [...threadIds];
      let paramIndex = threadIds.length + 1;

      if (resourceId) {
        conditions.push(`"resourceId" = $${paramIndex++}`);
        queryParams.push(resourceId);
      }

      if (filter?.dateRange?.start) {
        const startOp = filter.dateRange.startExclusive ? '>' : '>=';
        conditions.push(`COALESCE("createdAtZ", "createdAt") ${startOp} $${paramIndex++}`);
        queryParams.push(filter.dateRange.start);
      }

      if (filter?.dateRange?.end) {
        const endOp = filter.dateRange.endExclusive ? '<' : '<=';
        conditions.push(`COALESCE("createdAtZ", "createdAt") ${endOp} $${paramIndex++}`);
        queryParams.push(filter.dateRange.end);
      }

      const whereClause = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';

      // When perPage is 0 with no includes, there's nothing to return.
      if (perPage === 0 && (!include || include.length === 0)) {
        return { messages: [], total: 0, page, perPage: perPageForResponse, hasMore: false };
      }

      // When perPage is 0 and we have include targets, skip COUNT(*) and data queries.
      // This is the semantic recall path where we only need the included messages.
      if (perPage === 0 && include && include.length > 0) {
        const includeMessages = await this._getIncludedMessages({ include, resourceId });
        if (!includeMessages || includeMessages.length === 0) {
          return { messages: [], total: 0, page, perPage: perPageForResponse, hasMore: false };
        }
        const messagesWithParsedContent = includeMessages.map(row => this.parseRow(row));
        const list = new MessageList().add(messagesWithParsedContent, 'memory');
        return {
          messages: this._sortMessages(list.get.all.db(), field, direction),
          total: 0,
          page,
          perPage: perPageForResponse,
          hasMore: false,
        };
      }

      // The included messages do not depend on the page, so start that read now and
      // let it overlap the page read. The rejection is captured here so a failure of
      // the page read cannot leave this promise unhandled.
      let includeFailure: unknown;
      const includePromise =
        include && include.length > 0
          ? this._getIncludedMessages({ include, resourceId }).catch((error: unknown) => {
              includeFailure = error;
              return null;
            })
          : null;

      let total: number;
      let messages: MessageRowFromDB[];
      if (metadataFilter) {
        const rows = await this.#db.client.manyOrNone(
          `${selectStatement} FROM ${tableName} ${whereClause} ${orderByStatement}`,
          queryParams,
        );
        const filteredRows = (rows || []).filter(row =>
          storageMessageMatchesMetadataFilter(row.content, metadataFilter),
        );
        total = filteredRows.length;
        messages = perPageInput === false ? filteredRows : filteredRows.slice(offset, offset + perPage);
      } else {
        ({ total, messages } = await this.#fetchMessagePage({
          selectStatement,
          tableName,
          whereClause,
          orderByStatement,
          queryParams,
          perPageInput,
          perPage,
          offset,
        }));
      }
      const primaryPageCount = messages.length;

      if (total === 0 && messages.length === 0 && (!include || include.length === 0)) {
        return {
          messages: [],
          total: 0,
          page,
          perPage: perPageForResponse,
          hasMore: false,
        };
      }

      const messageIds = new Set(messages.map(m => m.id));
      if (include && include.length > 0) {
        const includeMessages = await includePromise;
        if (includeFailure) throw includeFailure;
        if (includeMessages) {
          for (const includeMsg of includeMessages) {
            if (!messageIds.has(includeMsg.id)) {
              messages.push(includeMsg);
              messageIds.add(includeMsg.id);
            }
          }
        }
      }

      const messagesWithParsedContent = messages.map(row => this.parseRow(row));

      const list = new MessageList().add(messagesWithParsedContent, 'memory');
      const finalMessages = this._sortMessages(list.get.all.db(), field, direction);

      const threadIdSet = new Set(threadIds);
      const returnedThreadMessageIds = new Set(
        finalMessages.filter(m => m.threadId && threadIdSet.has(m.threadId)).map(m => m.id),
      );
      const allThreadMessagesReturned = returnedThreadMessageIds.size >= total;
      const hasMore = metadataFilter
        ? perPageInput !== false && offset + primaryPageCount < total
        : perPageInput !== false && !allThreadMessagesReturned && offset + perPage < total;

      return {
        messages: finalMessages,
        total,
        page,
        perPage: perPageForResponse,
        hasMore,
      };
    } catch (rawError) {
      throw this.createAndTrackSafeReadError({
        operation: 'LIST_MESSAGES',
        text: 'Failed to list PostgreSQL messages',
        details: {
          threadIdCount: threadIds.length,
          hasResourceId: resourceId !== undefined,
          hasIncludeTargets: Boolean(include?.length),
          page,
        },
        rawError,
      });
    }
  }

  public async listMessagesByResourceId(
    args: StorageListMessagesByResourceIdInput,
  ): Promise<StorageListMessagesOutput> {
    const { resourceId, include, filter, perPage: perPageInput, page = 0, orderBy } = args;

    // Validate that resourceId is provided
    const hasResourceId = typeof resourceId === 'string' && resourceId.trim() !== '';
    if (!hasResourceId) {
      throw new MastraError(
        {
          id: createStorageErrorId('PG', 'LIST_MESSAGES_BY_RESOURCE_ID', 'INVALID_QUERY'),
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.USER,
          details: {
            hasResourceId,
          },
        },
        new Error('resourceId is required'),
      );
    }

    try {
      this.validatePaginationInput(page, perPageInput === undefined ? 40 : perPageInput);
    } catch (error) {
      throw new MastraError({
        id: createStorageErrorId('PG', 'LIST_MESSAGES_BY_RESOURCE_ID', 'INVALID_PAGE'),
        domain: ErrorDomain.STORAGE,
        category: ErrorCategory.USER,
        text: this.getSafePaginationErrorText(error),
        details: {
          hasResourceId,
          ...this.getSafePaginationDetails(page, perPageInput),
        },
      });
    }

    const perPage = normalizePerPage(perPageInput, 40);
    const { offset, perPage: perPageForResponse } = calculatePagination(page, perPageInput, perPage);
    const metadataFilter = validateStorageMetadataFilter(filter?.metadata);

    try {
      const { field, direction } = this.parseOrderBy(orderBy, 'ASC');
      const orderByStatement = `ORDER BY COALESCE("${field}Z", "${field}") ${direction}`;

      const selectStatement = `SELECT id, content, role, type, "createdAt", "createdAtZ", thread_id AS "threadId", "resourceId"`;
      const tableName = getTableName({ indexName: TABLE_MESSAGES, schemaName: getSchemaName(this.#schema) });

      // Build WHERE conditions
      const conditions: string[] = [];
      const queryParams: any[] = [];
      let paramIndex = 1;

      // Add resourceId filter
      conditions.push(`"resourceId" = $${paramIndex++}`);
      queryParams.push(resourceId);

      if (filter?.dateRange?.start) {
        const startOp = filter.dateRange.startExclusive ? '>' : '>=';
        conditions.push(`COALESCE("createdAtZ", "createdAt") ${startOp} $${paramIndex++}`);
        queryParams.push(filter.dateRange.start);
      }

      if (filter?.dateRange?.end) {
        const endOp = filter.dateRange.endExclusive ? '<' : '<=';
        conditions.push(`COALESCE("createdAtZ", "createdAt") ${endOp} $${paramIndex++}`);
        queryParams.push(filter.dateRange.end);
      }

      const whereClause = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';

      // When perPage is 0 with no includes, there's nothing to return.
      if (perPage === 0 && (!include || include.length === 0)) {
        return { messages: [], total: 0, page, perPage: perPageForResponse, hasMore: false };
      }

      // Fast path: when perPage is 0 and include is provided, skip COUNT(*) and the
      // main data query entirely. This is the semantic recall path where only included
      // (vector-matched) messages are needed. Skipping the COUNT(*) avoids scanning
      // the entire thread which was a major source of latency for large threads.
      if (perPage === 0 && include && include.length > 0) {
        const includeMessages = await this._getIncludedMessages({ include, resourceId });
        if (!includeMessages || includeMessages.length === 0) {
          return {
            messages: [],
            total: 0,
            page,
            perPage: perPageForResponse,
            hasMore: false,
          };
        }

        const messagesWithParsedContent = includeMessages.map(row => this.parseRow(row));
        const list = new MessageList().add(messagesWithParsedContent, 'memory');

        return {
          messages: this._sortMessages(list.get.all.db(), field, direction),
          total: 0,
          page,
          perPage: perPageForResponse,
          hasMore: false,
        };
      }

      // The included messages do not depend on the page, so start that read now and
      // let it overlap the page read. The rejection is captured here so a failure of
      // the page read cannot leave this promise unhandled.
      let includeFailure: unknown;
      const includePromise =
        include && include.length > 0
          ? this._getIncludedMessages({ include, resourceId }).catch((error: unknown) => {
              includeFailure = error;
              return null;
            })
          : null;

      let total: number;
      let messages: MessageRowFromDB[];
      if (metadataFilter) {
        const rows = await this.#db.client.manyOrNone(
          `${selectStatement} FROM ${tableName} ${whereClause} ${orderByStatement}`,
          queryParams,
        );
        const filteredRows = (rows || []).filter(row =>
          storageMessageMatchesMetadataFilter(row.content, metadataFilter),
        );
        total = filteredRows.length;
        messages = perPageInput === false ? filteredRows : filteredRows.slice(offset, offset + perPage);
      } else {
        ({ total, messages } = await this.#fetchMessagePage({
          selectStatement,
          tableName,
          whereClause,
          orderByStatement,
          queryParams,
          perPageInput,
          perPage,
          offset,
        }));
      }

      if (total === 0 && messages.length === 0 && (!include || include.length === 0)) {
        return {
          messages: [],
          total: 0,
          page,
          perPage: perPageForResponse,
          hasMore: false,
        };
      }

      const messageIds = new Set(messages.map(m => m.id));
      if (include && include.length > 0) {
        const includeMessages = await includePromise;
        if (includeFailure) throw includeFailure;
        if (includeMessages) {
          for (const includeMsg of includeMessages) {
            if (!messageIds.has(includeMsg.id)) {
              messages.push(includeMsg);
              messageIds.add(includeMsg.id);
            }
          }
        }
      }

      const messagesWithParsedContent = messages.map(row => this.parseRow(row));

      const list = new MessageList().add(messagesWithParsedContent, 'memory');
      const finalMessages = this._sortMessages(list.get.all.db(), field, direction);

      const hasMore = perPageInput !== false && offset + perPage < total;

      return {
        messages: finalMessages,
        total,
        page,
        perPage: perPageForResponse,
        hasMore,
      };
    } catch (rawError) {
      throw this.createAndTrackSafeReadError({
        operation: 'LIST_MESSAGES_BY_RESOURCE_ID',
        text: 'Failed to list PostgreSQL messages by resource ID',
        details: {
          hasResourceId: Boolean(resourceId),
          hasIncludeTargets: Boolean(include?.length),
          page,
        },
        rawError,
      });
    }
  }

  async saveMessages({ messages }: { messages: MastraDBMessage[] }): Promise<{ messages: MastraDBMessage[] }> {
    if (messages.length === 0) return { messages: [] };

    const threadId = messages[0]?.threadId;
    if (!threadId) {
      throw new MastraError({
        id: createStorageErrorId('PG', 'SAVE_MESSAGES', 'FAILED'),
        domain: ErrorDomain.STORAGE,
        category: ErrorCategory.THIRD_PARTY,
        text: `Thread ID is required`,
      });
    }

    try {
      const tableName = getTableName({ indexName: TABLE_MESSAGES, schemaName: getSchemaName(this.#schema) });
      const threadIds = new Set<string>();
      for (const message of messages) {
        if (!message.threadId) {
          throw new Error(
            `Expected to find a threadId for message, but couldn't find one. An unexpected error has occurred.`,
          );
        }
        if (!message.resourceId) {
          throw new Error(
            `Expected to find a resourceId for message, but couldn't find one. An unexpected error has occurred.`,
          );
        }
        threadIds.add(message.threadId);
      }

      for (const threadIdToCheck of threadIds) {
        const thread = await this.getThreadById({ threadId: threadIdToCheck });
        if (!thread) {
          throw new MastraError({
            id: createStorageErrorId('PG', 'SAVE_MESSAGES', 'FAILED'),
            domain: ErrorDomain.STORAGE,
            category: ErrorCategory.THIRD_PARTY,
            text: `Thread ${threadIdToCheck} not found`,
            details: {
              threadId: threadIdToCheck,
            },
          });
        }
      }

      const messagesToSave = dedupeMessagesForSave(messages);
      await this.#db.client.tx(async t => {
        for (let offset = 0; offset < messagesToSave.length; offset += MAX_MESSAGES_PER_INSERT) {
          const batch = messagesToSave.slice(offset, offset + MAX_MESSAGES_PER_INSERT);
          const values: unknown[] = [];
          const valuePlaceholders = batch
            .map((message, messageIndex) => {
              const createdAt = toUtcISOString(message.createdAt || new Date());
              values.push(
                message.id,
                message.threadId,
                typeof message.content === 'string' ? message.content : JSON.stringify(message.content),
                createdAt,
                createdAt,
                message.role,
                message.type || 'v2',
                message.resourceId,
              );

              const paramOffset = messageIndex * MESSAGE_INSERT_BIND_PARAMETERS;
              return `(${Array.from(
                { length: MESSAGE_INSERT_BIND_PARAMETERS },
                (_, paramIndex) => `$${paramOffset + paramIndex + 1}`,
              ).join(', ')})`;
            })
            .join(', ');

          await t.none(
            `INSERT INTO ${tableName} (id, thread_id, content, "createdAt", "createdAtZ", role, type, "resourceId")
             VALUES ${valuePlaceholders}
             ON CONFLICT (id) DO UPDATE SET
              thread_id = EXCLUDED.thread_id,
              content = EXCLUDED.content,
              role = EXCLUDED.role,
              type = EXCLUDED.type,
              "resourceId" = EXCLUDED."resourceId"`,
            values,
          );
        }

        const threadTableName = getTableName({ indexName: TABLE_THREADS, schemaName: getSchemaName(this.#schema) });
        const now = toUtcISOString(new Date());
        for (const threadIdToUpdate of threadIds) {
          await t.none(
            `UPDATE ${threadTableName}
              SET
                "updatedAt" = $1,
                "updatedAtZ" = $2
              WHERE id = $3`,
            [now, now, threadIdToUpdate],
          );
        }
      });

      const messagesWithParsedContent = messages.map(message => {
        if (typeof message.content === 'string') {
          try {
            return { ...message, content: JSON.parse(message.content) };
          } catch {
            return message;
          }
        }
        return message;
      });

      const list = new MessageList().add(messagesWithParsedContent as (MastraMessageV1 | MastraDBMessage)[], 'memory');
      return { messages: list.get.all.db() };
    } catch (error) {
      if (error instanceof MastraError) {
        throw error;
      }
      throw new MastraError(
        {
          id: createStorageErrorId('PG', 'SAVE_MESSAGES', 'FAILED'),
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
          details: {
            threadId,
          },
        },
        error,
      );
    }
  }

  async updateMessages({
    messages,
    retractObservationalMemory,
    observationalMemoryRetractions,
  }: {
    messages: (Partial<Omit<MastraDBMessage, 'createdAt'>> & {
      id: string;
      content?: {
        metadata?: MastraMessageContentV2['metadata'];
        content?: MastraMessageContentV2['content'];
      };
    })[];
    retractObservationalMemory?: boolean;
    observationalMemoryRetractions?: ObservationalMemoryRetractionReceipt[];
  }): Promise<MastraDBMessage[]> {
    if (messages.length === 0) {
      return [];
    }

    const messageIds = messages.map(m => m.id);

    const selectQuery = `SELECT id, content, role, type, "createdAt", "createdAtZ", thread_id AS "threadId", "resourceId" FROM ${getTableName({ indexName: TABLE_MESSAGES, schemaName: getSchemaName(this.#schema) })} WHERE id IN (${inPlaceholders(messageIds.length)})`;

    const existingMessagesDb = await this.#db.client.manyOrNone(selectQuery, messageIds);

    if (existingMessagesDb.length === 0) {
      return [];
    }

    const existingMessages: MastraDBMessage[] = existingMessagesDb.map(msg => {
      if (typeof msg.content === 'string') {
        try {
          msg.content = JSON.parse(msg.content);
        } catch {
          // ignore if not valid json
        }
      }
      return msg as MastraDBMessage;
    });

    const threadIdsToUpdate = new Set<string>();
    const committedRetractions: ObservationalMemoryRetractionReceipt[] = [];

    await this.#db.client.tx(async t => {
      if (retractObservationalMemory) {
        const updatesById = new Map(messages.map(message => [message.id, message]));
        const retractionRows = existingMessages.flatMap(existingMessage => {
          const update = updatesById.get(existingMessage.id);
          const destinationThreadId = update?.threadId ?? existingMessage.threadId;
          return [
            existingMessage,
            {
              threadId: destinationThreadId,
              resourceId:
                update?.resourceId ??
                (destinationThreadId === existingMessage.threadId ? existingMessage.resourceId : undefined),
            },
          ];
        });
        committedRetractions.push(
          ...(await this.retractObservationalMemoryForMessageRowsInTransaction(t, retractionRows)),
        );
      }

      const queries = [];
      const columnMapping: Record<string, string> = {
        threadId: 'thread_id',
      };

      for (const existingMessage of existingMessages) {
        const updatePayload = messages.find(m => m.id === existingMessage.id);
        if (!updatePayload) continue;

        const { id, ...fieldsToUpdate } = updatePayload;
        if (Object.keys(fieldsToUpdate).length === 0) continue;

        threadIdsToUpdate.add(existingMessage.threadId!);
        if (updatePayload.threadId && updatePayload.threadId !== existingMessage.threadId) {
          threadIdsToUpdate.add(updatePayload.threadId);
        }

        const setClauses: string[] = [];
        const values: any[] = [];
        let paramIndex = 1;

        const updatableFields = { ...fieldsToUpdate };

        if (updatableFields.content) {
          const newContent = {
            ...existingMessage.content,
            ...updatableFields.content,
            ...(existingMessage.content?.metadata && updatableFields.content.metadata
              ? {
                  metadata: {
                    ...existingMessage.content.metadata,
                    ...updatableFields.content.metadata,
                  },
                }
              : {}),
          };
          setClauses.push(`content = $${paramIndex++}`);
          values.push(newContent);
          delete updatableFields.content;
        }

        for (const key in updatableFields) {
          if (Object.prototype.hasOwnProperty.call(updatableFields, key)) {
            const dbColumn = columnMapping[key] || key;
            setClauses.push(`"${dbColumn}" = $${paramIndex++}`);
            values.push(updatableFields[key as keyof typeof updatableFields]);
          }
        }

        if (setClauses.length > 0) {
          values.push(id);
          const sql = `UPDATE ${getTableName({ indexName: TABLE_MESSAGES, schemaName: getSchemaName(this.#schema) })} SET ${setClauses.join(', ')} WHERE id = $${paramIndex}`;
          queries.push(t.none(sql, values));
        }
      }

      if (threadIdsToUpdate.size > 0) {
        const threadIds = Array.from(threadIdsToUpdate);
        queries.push(
          t.none(
            `UPDATE ${getTableName({ indexName: TABLE_THREADS, schemaName: getSchemaName(this.#schema) })} SET "updatedAt" = NOW(), "updatedAtZ" = NOW() WHERE id IN (${inPlaceholders(threadIds.length)})`,
            threadIds,
          ),
        );
      }

      if (queries.length > 0) {
        await t.batch(queries);
      }
    });
    observationalMemoryRetractions?.push(...committedRetractions);

    const updatedMessages = await this.#db.client.manyOrNone<MessageRowFromDB>(selectQuery, messageIds);

    return (updatedMessages || []).map((row: MessageRowFromDB) => {
      const message = this.normalizeMessageRow(row);
      if (typeof message.content === 'string') {
        try {
          return { ...message, content: JSON.parse(message.content) } as MastraDBMessage;
        } catch {
          /* ignore */
        }
      }
      return message as MastraDBMessage;
    });
  }

  async deleteMessages(
    messageIds: string[],
    options?: {
      retractObservationalMemory?: boolean;
      observationalMemoryRetractions?: ObservationalMemoryRetractionReceipt[];
    },
  ): Promise<void> {
    if (!messageIds || messageIds.length === 0) {
      return;
    }

    try {
      const messageTableName = getTableName({ indexName: TABLE_MESSAGES, schemaName: getSchemaName(this.#schema) });
      const threadTableName = getTableName({ indexName: TABLE_THREADS, schemaName: getSchemaName(this.#schema) });
      const committedRetractions: ObservationalMemoryRetractionReceipt[] = [];

      await this.#db.client.tx(async t => {
        const placeholders = messageIds.map((_, idx) => `$${idx + 1}`).join(',');
        const messages = await t.manyOrNone<{ threadId: string; resourceId: string | null }>(
          `SELECT DISTINCT
             messages.thread_id AS "threadId",
             COALESCE(messages."resourceId", threads."resourceId") AS "resourceId"
           FROM ${messageTableName} AS messages
           LEFT JOIN ${threadTableName} AS threads ON threads.id = messages.thread_id
           WHERE messages.id IN (${placeholders})`,
          messageIds,
        );

        if (options?.retractObservationalMemory) {
          committedRetractions.push(...(await this.retractObservationalMemoryForMessageRowsInTransaction(t, messages)));
        }

        const threadIds = messages?.map(message => message.threadId).filter(Boolean) || [];

        await t.none(`DELETE FROM ${messageTableName} WHERE id IN (${placeholders})`, messageIds);

        if (threadIds.length > 0) {
          await t.none(
            `UPDATE ${threadTableName} SET "updatedAt" = NOW(), "updatedAtZ" = NOW() WHERE id IN (${inPlaceholders(threadIds.length)})`,
            threadIds,
          );
        }
      });
      options?.observationalMemoryRetractions?.push(...committedRetractions);
    } catch (error) {
      throw new MastraError(
        {
          id: createStorageErrorId('PG', 'DELETE_MESSAGES', 'FAILED'),
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
          details: { messageIds: messageIds.join(', ') },
        },
        error,
      );
    }
  }

  async getResourceById({ resourceId }: { resourceId: string }): Promise<StorageResourceType | null> {
    const tableName = getTableName({ indexName: TABLE_RESOURCES, schemaName: getSchemaName(this.#schema) });
    const result = await this.#db.client.oneOrNone<StorageResourceType & { createdAtZ: Date; updatedAtZ: Date }>(
      `SELECT * FROM ${tableName} WHERE id = $1`,
      [resourceId],
    );

    if (!result) {
      return null;
    }

    return {
      id: result.id,
      createdAt: result.createdAtZ || result.createdAt,
      updatedAt: result.updatedAtZ || result.updatedAt,
      workingMemory: result.workingMemory,
      metadata: typeof result.metadata === 'string' ? JSON.parse(result.metadata) : result.metadata,
    };
  }

  async saveResource({ resource }: { resource: StorageResourceType }): Promise<StorageResourceType> {
    const createdAt = toUtcISOString(resource.createdAt);
    const updatedAt = toUtcISOString(resource.updatedAt);
    const tableName = getTableName({ indexName: TABLE_RESOURCES, schemaName: getSchemaName(this.#schema) });
    return await this.#db.client.tx(async t => {
      await this.lockWorkingMemoryTarget(t, 'resource', resource.id);
      const current = await t.oneOrNone<{ workingMemory: string | null; metadata: unknown }>(
        `SELECT "workingMemory", metadata FROM ${tableName} WHERE id = $1 FOR UPDATE`,
        [resource.id],
      );
      const currentMetadata = current ? parseMetadata(current.metadata) : undefined;
      assertWorkingMemorySnapshotUnchanged({
        currentValue: current?.workingMemory,
        currentMetadata,
        proposedValue: resource.workingMemory,
        proposedValueProvided: Object.prototype.hasOwnProperty.call(resource, 'workingMemory'),
        proposedMetadata: resource.metadata,
      });
      const governed = currentMetadata && hasWorkingMemorySnapshotControls(currentMetadata);
      const metadata = governed
        ? preserveWorkingMemorySnapshotControls(currentMetadata, resource.metadata ?? {})
        : (resource.metadata ?? {});
      const row = await t.one<StorageResourceType & { createdAtZ: Date | string; updatedAtZ: Date | string }>(
        `INSERT INTO ${tableName}
             (id, "workingMemory", metadata, "createdAt", "createdAtZ", "updatedAt", "updatedAtZ")
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (id) DO UPDATE SET
             "workingMemory" = EXCLUDED."workingMemory",
             metadata = EXCLUDED.metadata,
             "createdAt" = EXCLUDED."createdAt",
             "createdAtZ" = EXCLUDED."createdAtZ",
             "updatedAt" = EXCLUDED."updatedAt",
             "updatedAtZ" = EXCLUDED."updatedAtZ"
           RETURNING *`,
        [
          resource.id,
          governed ? current?.workingMemory : resource.workingMemory,
          JSON.stringify(metadata),
          createdAt,
          createdAt,
          updatedAt,
          updatedAt,
        ],
      );
      return {
        id: row.id,
        workingMemory: row.workingMemory,
        metadata: parseMetadata(row.metadata),
        createdAt: new Date(row.createdAtZ || row.createdAt),
        updatedAt: new Date(row.updatedAtZ || row.updatedAt),
      };
    });
  }

  async updateResource({
    resourceId,
    workingMemory,
    metadata,
  }: {
    resourceId: string;
    workingMemory?: string;
    metadata?: Record<string, unknown>;
  }): Promise<StorageResourceType> {
    const tableName = getTableName({ indexName: TABLE_RESOURCES, schemaName: getSchemaName(this.#schema) });
    return await this.#db.client.tx(async t => {
      await this.lockWorkingMemoryTarget(t, 'resource', resourceId);
      const current = await t.oneOrNone<StorageResourceType & { createdAtZ: Date | string; updatedAtZ: Date | string }>(
        `SELECT * FROM ${tableName} WHERE id = $1 FOR UPDATE`,
        [resourceId],
      );
      const currentMetadata = current ? parseMetadata(current.metadata) : undefined;
      assertWorkingMemorySnapshotUnchanged({
        currentValue: current?.workingMemory,
        currentMetadata,
        proposedValue: workingMemory,
        proposedValueProvided: workingMemory !== undefined,
        proposedMetadata: metadata,
      });
      const now = new Date();
      if (!current) {
        const row = await t.one<StorageResourceType & { createdAtZ: Date | string; updatedAtZ: Date | string }>(
          `INSERT INTO ${tableName}
               (id, "workingMemory", metadata, "createdAt", "createdAtZ", "updatedAt", "updatedAtZ")
             VALUES ($1, $2, $3, $4, $5, $6, $7)
             RETURNING *`,
          [
            resourceId,
            workingMemory,
            JSON.stringify(metadata ?? {}),
            now.toISOString(),
            now.toISOString(),
            now.toISOString(),
            now.toISOString(),
          ],
        );
        return {
          id: row.id,
          workingMemory: row.workingMemory,
          metadata: parseMetadata(row.metadata),
          createdAt: new Date(row.createdAtZ || row.createdAt),
          updatedAt: new Date(row.updatedAtZ || row.updatedAt),
        };
      }

      const governed = hasWorkingMemorySnapshotControls(currentMetadata);
      const mergedMetadata = preserveWorkingMemorySnapshotControls(currentMetadata, {
        ...currentMetadata,
        ...metadata,
      });
      const row = await t.one<StorageResourceType & { createdAtZ: Date | string; updatedAtZ: Date | string }>(
        `UPDATE ${tableName}
           SET "workingMemory" = $1, metadata = $2, "updatedAt" = $3, "updatedAtZ" = $4
           WHERE id = $5
           RETURNING *`,
        [
          governed || workingMemory === undefined ? current.workingMemory : workingMemory,
          JSON.stringify(mergedMetadata),
          now.toISOString(),
          now.toISOString(),
          resourceId,
        ],
      );
      return {
        id: row.id,
        workingMemory: row.workingMemory,
        metadata: parseMetadata(row.metadata),
        createdAt: new Date(row.createdAtZ || row.createdAt),
        updatedAt: new Date(row.updatedAtZ || row.updatedAt),
      };
    });
  }

  async updateResourceFromObservationalMemory({
    resourceId,
    workingMemory,
    guard,
  }: {
    resourceId: string;
    workingMemory: string;
    guard: ObservationalMemoryWriteGuard;
  }): Promise<StorageResourceType> {
    if (guard.resourceId !== resourceId) {
      throw new Error('Observational memory guard does not match the target resource.');
    }
    const tableName = getTableName({ indexName: TABLE_RESOURCES, schemaName: getSchemaName(this.#schema) });
    try {
      return await this.#db.client.tx(async t => {
        await this.lockObservationalMemoryResource(t, guard.resourceId);
        await this.lockWorkingMemoryTarget(t, 'resource', resourceId);
        await this.assertCurrentObservationalMemoryGeneration(t, guard);
        const currentRow = await t.oneOrNone<{ workingMemory: string | null; metadata: unknown }>(
          `SELECT "workingMemory", metadata FROM ${tableName} WHERE id = $1 FOR UPDATE`,
          [resourceId],
        );
        assertWorkingMemorySnapshotUnchanged({
          currentValue: currentRow?.workingMemory,
          currentMetadata: currentRow ? parseMetadata(currentRow.metadata) : undefined,
          proposedValue: workingMemory,
          proposedValueProvided: true,
          proposedMetadata: undefined,
        });
        const now = new Date();
        const nowStr = now.toISOString();
        const row = await t.one<StorageResourceType & { createdAtZ: Date | string; updatedAtZ: Date | string }>(
          `INSERT INTO ${tableName}
             (id, "workingMemory", metadata, "createdAt", "createdAtZ", "updatedAt", "updatedAtZ")
           VALUES ($1, $2, '{}'::jsonb, $3, $3, $3, $3)
           ON CONFLICT (id) DO UPDATE SET
             "workingMemory" = EXCLUDED."workingMemory",
             "updatedAt" = EXCLUDED."updatedAt",
             "updatedAtZ" = EXCLUDED."updatedAtZ"
           RETURNING *`,
          [resourceId, workingMemory, nowStr],
        );
        return {
          id: row.id,
          workingMemory: row.workingMemory,
          metadata: typeof row.metadata === 'string' ? JSON.parse(row.metadata) : row.metadata,
          createdAt: new Date(row.createdAtZ || row.createdAt),
          updatedAt: new Date(row.updatedAtZ || row.updatedAt),
        };
      });
    } catch (error) {
      if (error instanceof WorkingMemoryValidationError) throw error;
      throw new MastraError(
        {
          id: createStorageErrorId('PG', 'UPDATE_RESOURCE_FROM_OBSERVATIONAL_MEMORY', 'FAILED'),
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
          details: { resourceId, recordId: guard.recordId },
        },
        error,
      );
    }
  }

  async updateThreadFromObservationalMemory({
    id,
    title,
    metadata,
    guard,
  }: {
    id: string;
    title?: string;
    metadata: Record<string, unknown>;
    guard: ObservationalMemoryWriteGuard;
  }): Promise<StorageThreadType> {
    if (guard.threadId !== null && guard.threadId !== id) {
      throw new Error('Observational memory guard does not match the target thread.');
    }
    const tableName = getTableName({ indexName: TABLE_THREADS, schemaName: getSchemaName(this.#schema) });
    try {
      return await this.#db.client.tx(async t => {
        await this.lockObservationalMemoryResource(t, guard.resourceId);
        await this.lockWorkingMemoryTarget(t, 'thread', id);
        await this.assertCurrentObservationalMemoryGeneration(t, guard);
        const currentRow = await t.oneOrNone<{ metadata: unknown }>(
          `SELECT metadata FROM ${tableName} WHERE id = $1 AND "resourceId" = $2 FOR UPDATE`,
          [id, guard.resourceId],
        );
        if (!currentRow) {
          throw new Error('Observational memory guard does not match the target thread resource.');
        }
        const mergedMetadata = mergeObservationalThreadMetadata(parseMetadata(currentRow.metadata), metadata);
        const now = new Date();
        const row = await t.one<StorageThreadType & { createdAtZ: Date | string; updatedAtZ: Date | string }>(
          `UPDATE ${tableName}
           SET title = COALESCE($1, title), metadata = $2, "updatedAt" = $3, "updatedAtZ" = $4
           WHERE id = $5 AND "resourceId" = $6
           RETURNING *`,
          [title, JSON.stringify(mergedMetadata), now.toISOString(), now.toISOString(), id, guard.resourceId],
        );
        return {
          id: row.id,
          resourceId: row.resourceId,
          title: row.title,
          metadata: typeof row.metadata === 'string' ? JSON.parse(row.metadata) : row.metadata,
          createdAt: new Date(row.createdAtZ || row.createdAt),
          updatedAt: new Date(row.updatedAtZ || row.updatedAt),
        };
      });
    } catch (error) {
      throw new MastraError(
        {
          id: createStorageErrorId('PG', 'UPDATE_THREAD_FROM_OBSERVATIONAL_MEMORY', 'FAILED'),
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
          details: { threadId: id, recordId: guard.recordId },
        },
        error,
      );
    }
  }

  async getWorkingMemorySnapshot(input: WorkingMemorySnapshotInput): Promise<WorkingMemorySnapshot> {
    try {
      if (input.scope === 'resource') {
        const tableName = getTableName({ indexName: TABLE_RESOURCES, schemaName: getSchemaName(this.#schema) });
        const row = await this.#db.client.oneOrNone<{ workingMemory: string | null; metadata: unknown }>(
          `SELECT "workingMemory", metadata FROM ${tableName} WHERE id = $1`,
          [input.resourceId],
        );
        return readWorkingMemorySnapshot(row?.workingMemory, parseMetadata(row?.metadata));
      }

      const tableName = getTableName({ indexName: TABLE_THREADS, schemaName: getSchemaName(this.#schema) });
      const row = await this.#db.client.oneOrNone<{ metadata: unknown }>(
        `SELECT metadata FROM ${tableName} WHERE id = $1 AND "resourceId" = $2`,
        [input.threadId, input.resourceId],
      );
      if (!row)
        throw new WorkingMemoryValidationError('Working-memory thread was not found in the requested resource.');
      const metadata = parseMetadata(row.metadata);
      return readWorkingMemorySnapshot(
        typeof metadata.workingMemory === 'string' ? metadata.workingMemory : null,
        metadata,
      );
    } catch (error) {
      if (error instanceof MastraError) throw error;
      if (error instanceof WorkingMemoryValidationError) throw error;
      const text = 'Failed to read PostgreSQL working memory';
      const failureCode = this.getSafeReadFailureCode(error);
      const safeError = new MastraError(
        {
          id: createStorageErrorId('PG', 'GET_WORKING_MEMORY_SNAPSHOT', 'FAILED'),
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
          text,
          details: {
            scope: input.scope,
            ...(failureCode ? { failureCode } : {}),
          },
        },
        new Error(text),
      );
      this.logger?.error?.(safeError.toString());
      this.logger?.trackException(safeError);
      throw safeError;
    }
  }

  async applyWorkingMemoryUpdate(input: ApplyWorkingMemoryUpdateInput): Promise<WorkingMemorySnapshot> {
    try {
      return await this.#db.client.tx(async t => {
        await this.lockObservationalMemoryResource(t, input.resourceId);
        await this.lockWorkingMemoryTarget(
          t,
          input.scope,
          input.scope === 'resource' ? input.resourceId : input.threadId,
        );
        if (input.observationalMemoryGuard) {
          if (input.source !== 'observer') {
            throw new WorkingMemoryValidationError('Only observer updates may carry an observational-memory guard.');
          }
          const guard = input.observationalMemoryGuard;
          if (guard.resourceId !== input.resourceId || (guard.threadId !== null && guard.threadId !== input.threadId)) {
            throw new WorkingMemoryValidationError(
              'Observational memory guard does not match the working-memory target.',
            );
          }
          await this.assertCurrentObservationalMemoryGeneration(t, guard);
        }

        const now = new Date();
        const nowString = now.toISOString();
        if (input.scope === 'resource') {
          const tableName = getTableName({ indexName: TABLE_RESOURCES, schemaName: getSchemaName(this.#schema) });
          const row = await t.oneOrNone<{
            workingMemory: string | null;
            metadata: unknown;
            createdAt: Date | string;
            createdAtZ?: Date | string;
          }>(`SELECT "workingMemory", metadata, "createdAt", "createdAtZ" FROM ${tableName} WHERE id = $1 FOR UPDATE`, [
            input.resourceId,
          ]);
          const metadata = parseMetadata(row?.metadata);
          const current = readWorkingMemorySnapshot(row?.workingMemory, metadata);
          const next = applyWorkingMemorySnapshotUpdate(current, input, nowString);
          if (next === current) return current;
          const nextMetadata = writeWorkingMemorySnapshotMetadata(metadata, next);
          if (row) {
            await t.none(
              `UPDATE ${tableName}
               SET "workingMemory" = $1, metadata = $2, "updatedAt" = $3, "updatedAtZ" = $4
               WHERE id = $5`,
              [next.value, JSON.stringify(nextMetadata), nowString, nowString, input.resourceId],
            );
          } else {
            await t.none(
              `INSERT INTO ${tableName}
                 (id, "workingMemory", metadata, "createdAt", "createdAtZ", "updatedAt", "updatedAtZ")
               VALUES ($1, $2, $3, $4, $5, $6, $7)`,
              [input.resourceId, next.value, JSON.stringify(nextMetadata), nowString, nowString, nowString, nowString],
            );
          }
          return next;
        }

        const tableName = getTableName({ indexName: TABLE_THREADS, schemaName: getSchemaName(this.#schema) });
        const row = await t.oneOrNone<{ metadata: unknown }>(
          `SELECT metadata FROM ${tableName} WHERE id = $1 AND "resourceId" = $2 FOR UPDATE`,
          [input.threadId, input.resourceId],
        );
        if (!row)
          throw new WorkingMemoryValidationError('Working-memory thread was not found in the requested resource.');
        const metadata = parseMetadata(row.metadata);
        const current = readWorkingMemorySnapshot(
          typeof metadata.workingMemory === 'string' ? metadata.workingMemory : null,
          metadata,
        );
        const next = applyWorkingMemorySnapshotUpdate(current, input, nowString);
        if (next === current) return current;
        const nextMetadata = writeWorkingMemorySnapshotMetadata(metadata, next);
        if (next.value === null) delete nextMetadata.workingMemory;
        else nextMetadata.workingMemory = next.value;
        await t.none(
          `UPDATE ${tableName} SET metadata = $1, "updatedAt" = $2, "updatedAtZ" = $3
           WHERE id = $4 AND "resourceId" = $5`,
          [JSON.stringify(nextMetadata), nowString, nowString, input.threadId, input.resourceId],
        );
        return next;
      });
    } catch (error) {
      if (error instanceof WorkingMemoryRevisionConflictError) throw error;
      if (error instanceof WorkingMemoryValidationError || error instanceof ObservationalMemoryGenerationConflictError)
        throw error;
      const text = 'Failed to update PostgreSQL working memory';
      const failureCode = this.getSafeReadFailureCode(error);
      const safeError = new MastraError(
        {
          id: createStorageErrorId('PG', 'APPLY_WORKING_MEMORY_UPDATE', 'FAILED'),
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
          text,
          details: {
            scope: input.scope,
            source: input.source,
            ...(failureCode ? { failureCode } : {}),
          },
        },
        new Error(text),
      );
      this.logger?.error?.(safeError.toString());
      this.logger?.trackException(safeError);
      throw safeError;
    }
  }

  async deleteResource({
    resourceId,
    observationalMemoryRecordIds,
  }: {
    resourceId: string;
    observationalMemoryRecordIds?: string[];
  }): Promise<void> {
    try {
      const tableName = getTableName({ indexName: TABLE_RESOURCES, schemaName: getSchemaName(this.#schema) });
      let committedRecordIds: string[] = [];
      await this.#db.client.tx(async t => {
        await this.lockObservationalMemoryResource(t, resourceId);
        await t.none(`DELETE FROM ${tableName} WHERE id = $1`, [resourceId]);

        // Resource erasure must not orphan the resource-scoped observational
        // memory record. Thread-scoped rows stay with their threads (which
        // deleteResource deliberately preserves) — only the threadId-less
        // resource record goes.
        const schemaName = this.#schema || 'public';
        const omTableExists = await t.oneOrNone<{ tablename: string }>(
          `SELECT tablename FROM pg_tables WHERE schemaname = $1 AND tablename = $2`,
          [schemaName, OM_TABLE],
        );
        if (omTableExists !== null) {
          const omTableName = getTableName({
            indexName: OM_TABLE,
            schemaName: getSchemaName(this.#schema),
          });
          const erasedRows = await t.manyOrNone<{ id: string }>(
            `DELETE FROM ${omTableName} WHERE "resourceId" = $1 AND "threadId" IS NULL RETURNING id`,
            [resourceId],
          );
          committedRecordIds = erasedRows.map(row => row.id);
        }
      });
      observationalMemoryRecordIds?.push(...committedRecordIds);
    } catch (rawError) {
      const text = 'Failed to delete PostgreSQL memory resource';
      const failureCode = this.getSafeReadFailureCode(rawError);
      const error = new MastraError(
        {
          id: createStorageErrorId('PG', 'DELETE_RESOURCE', 'FAILED'),
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
          text,
          details: { operation: 'delete-resource', ...(failureCode && { failureCode }) },
        },
        // Keep the cause sanitized: retaining the driver error could serialize query or connection details.
        new Error(text),
      );
      this.logger?.error?.(error.toString());
      this.logger?.trackException(error);
      throw error;
    }
  }

  async cloneThread(args: StorageCloneThreadInput): Promise<StorageCloneThreadOutput> {
    const { sourceThreadId, newThreadId: providedThreadId, resourceId, title, metadata, options } = args;

    // Get the source thread
    const sourceThread = await this.getThreadById({ threadId: sourceThreadId });
    if (!sourceThread) {
      throw new MastraError({
        id: createStorageErrorId('PG', 'CLONE_THREAD', 'SOURCE_NOT_FOUND'),
        domain: ErrorDomain.STORAGE,
        category: ErrorCategory.USER,
        text: `Source thread with id ${sourceThreadId} not found`,
        details: { sourceThreadId },
      });
    }

    // Use provided ID or generate a new one
    const newThreadId = providedThreadId || crypto.randomUUID();

    // Check if the new thread ID already exists
    const existingThread = await this.getThreadById({ threadId: newThreadId });
    if (existingThread) {
      throw new MastraError({
        id: createStorageErrorId('PG', 'CLONE_THREAD', 'THREAD_EXISTS'),
        domain: ErrorDomain.STORAGE,
        category: ErrorCategory.USER,
        text: `Thread with id ${newThreadId} already exists`,
        details: { newThreadId },
      });
    }

    const threadTableName = getTableName({ indexName: TABLE_THREADS, schemaName: getSchemaName(this.#schema) });
    const messageTableName = getTableName({ indexName: TABLE_MESSAGES, schemaName: getSchemaName(this.#schema) });

    try {
      return await this.#db.client.tx(async t => {
        // Build message query with filters
        let messageQuery = `SELECT id, content, role, type, "createdAt", "createdAtZ", thread_id AS "threadId", "resourceId"
                            FROM ${messageTableName} WHERE thread_id = $1`;
        const messageParams: any[] = [sourceThreadId];
        let paramIndex = 2;

        // Apply date filters
        if (options?.messageFilter?.startDate) {
          messageQuery += ` AND COALESCE("createdAtZ", "createdAt") >= $${paramIndex++}`;
          messageParams.push(options.messageFilter.startDate);
        }
        if (options?.messageFilter?.endDate) {
          messageQuery += ` AND COALESCE("createdAtZ", "createdAt") <= $${paramIndex++}`;
          messageParams.push(options.messageFilter.endDate);
        }

        // Apply message ID filter
        if (options?.messageFilter?.messageIds && options.messageFilter.messageIds.length > 0) {
          messageQuery += ` AND id IN (${options.messageFilter.messageIds.map(() => `$${paramIndex++}`).join(', ')})`;
          messageParams.push(...options.messageFilter.messageIds);
        }

        messageQuery += ` ORDER BY "createdAt" ASC`;

        // Apply message limit (from most recent, so we need to reverse order for limit then sort back)
        if (options?.messageLimit && options.messageLimit > 0) {
          // Get messages ordered DESC to get most recent, limited, then we'll reverse
          const limitQuery = `SELECT * FROM (${messageQuery.replace('ORDER BY "createdAt" ASC', 'ORDER BY "createdAt" DESC')} LIMIT $${paramIndex}) AS limited ORDER BY "createdAt" ASC`;
          messageParams.push(options.messageLimit);
          messageQuery = limitQuery;
        }

        const sourceMessages = await t.manyOrNone<MessageRowFromDB>(messageQuery, messageParams);

        const now = new Date();
        const nowStr = toUtcISOString(now);

        // Determine the last message ID for clone metadata
        const lastMessageId = sourceMessages.length > 0 ? sourceMessages[sourceMessages.length - 1]!.id : undefined;

        // Create clone metadata
        const cloneMetadata: ThreadCloneMetadata = {
          sourceThreadId,
          clonedAt: now,
          ...(lastMessageId && { lastMessageId }),
        };

        // Create the new thread
        const newThread: StorageThreadType = {
          id: newThreadId,
          resourceId: resourceId || sourceThread.resourceId,
          title: title || (sourceThread.title ? `Clone of ${sourceThread.title}` : ''),
          metadata: {
            ...metadata,
            clone: cloneMetadata,
          },
          createdAt: now,
          updatedAt: now,
        };

        // Insert the new thread
        await t.none(
          `INSERT INTO ${threadTableName} (
            id,
            "resourceId",
            title,
            metadata,
            "createdAt",
            "createdAtZ",
            "updatedAt",
            "updatedAtZ"
          ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
          [
            newThread.id,
            newThread.resourceId,
            newThread.title,
            newThread.metadata ? JSON.stringify(newThread.metadata) : null,
            nowStr,
            nowStr,
            nowStr,
            nowStr,
          ],
        );

        // Clone messages with new IDs
        const clonedMessages: MastraDBMessage[] = [];
        const messageIdMap: Record<string, string> = {};
        const targetResourceId = resourceId || sourceThread.resourceId;

        for (const sourceMsg of sourceMessages) {
          const newMessageId = crypto.randomUUID();
          messageIdMap[sourceMsg.id] = newMessageId;
          const normalizedMsg = this.normalizeMessageRow(sourceMsg);
          let parsedContent = normalizedMsg.content;
          try {
            parsedContent = JSON.parse(normalizedMsg.content);
          } catch {
            // use content as is
          }
          const createdAt = toUtcISOString(new Date(normalizedMsg.createdAt));

          await t.none(
            `INSERT INTO ${messageTableName} (id, thread_id, content, "createdAt", "createdAtZ", role, type, "resourceId")
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
            [
              newMessageId,
              newThreadId,
              typeof normalizedMsg.content === 'string' ? normalizedMsg.content : JSON.stringify(normalizedMsg.content),
              createdAt,
              createdAt,
              normalizedMsg.role,
              normalizedMsg.type || 'v2',
              targetResourceId,
            ],
          );

          clonedMessages.push({
            id: newMessageId,
            threadId: newThreadId,
            content: parsedContent,
            role: normalizedMsg.role as MastraDBMessage['role'],
            type: normalizedMsg.type,
            createdAt: new Date(normalizedMsg.createdAt as string),
            resourceId: targetResourceId,
          });
        }

        return {
          thread: newThread,
          clonedMessages,
          messageIdMap,
        };
      });
    } catch (error) {
      if (error instanceof MastraError) {
        throw error;
      }
      throw new MastraError(
        {
          id: createStorageErrorId('PG', 'CLONE_THREAD', 'FAILED'),
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
          details: { sourceThreadId, newThreadId },
        },
        error,
      );
    }
  }

  // ============================================
  // Observational Memory Methods
  // ============================================

  private getOMKey(threadId: string | null, resourceId: string): string {
    return threadId ? `thread:${threadId}` : `resource:${resourceId}`;
  }

  private async lockObservationalMemoryResource(t: TxClient, resourceId: string): Promise<void> {
    await t.none('SELECT pg_advisory_xact_lock(hashtextextended($1, 0))', [
      `mastra:observational-memory:${resourceId}`,
    ]);
  }

  private async lockWorkingMemoryTarget(t: TxClient, scope: 'thread' | 'resource', id: string): Promise<void> {
    await t.none('SELECT pg_advisory_xact_lock(hashtextextended($1, 0))', [`mastra:working-memory:${scope}:${id}`]);
  }

  private async assertCurrentObservationalMemoryGeneration(
    t: TxClient,
    guard: ObservationalMemoryWriteGuard,
  ): Promise<void> {
    const tableName = getTableName({
      indexName: OM_TABLE,
      schemaName: getSchemaName(this.#schema),
    });
    const current = await t.oneOrNone<{ id: string }>(
      `SELECT id FROM ${tableName}
       WHERE "lookupKey" = $1
       ORDER BY "generationCount" DESC
       LIMIT 1`,
      [this.getOMKey(guard.threadId, guard.resourceId)],
    );
    if (current?.id !== guard.recordId) {
      throw new ObservationalMemoryGenerationConflictError();
    }
  }

  private parseOMRow(row: any): ObservationalMemoryRecord {
    // OM is a new table - use timezone-aware columns (*Z) directly (no legacy fallback needed)
    return {
      id: row.id,
      scope: row.scope,
      threadId: row.threadId || null,
      resourceId: row.resourceId,
      createdAt: new Date(row.createdAtZ),
      updatedAt: new Date(row.updatedAtZ),
      lastObservedAt: row.lastObservedAtZ ? new Date(row.lastObservedAtZ) : undefined,
      originType: row.originType || 'initial',
      generationCount: Number(row.generationCount || 0),
      activeObservations: row.activeObservations || '',
      // Handle new chunk-based structure
      bufferedObservationChunks: row.bufferedObservationChunks
        ? typeof row.bufferedObservationChunks === 'string'
          ? JSON.parse(row.bufferedObservationChunks)
          : row.bufferedObservationChunks
        : undefined,
      // Deprecated fields (for backward compatibility)
      bufferedObservations: row.activeObservationsPendingUpdate || undefined,
      bufferedObservationTokens: row.bufferedObservationTokens ? Number(row.bufferedObservationTokens) : undefined,
      bufferedMessageIds: undefined, // Use bufferedObservationChunks instead
      bufferedReflection: row.bufferedReflection || undefined,
      bufferedReflectionTokens: row.bufferedReflectionTokens ? Number(row.bufferedReflectionTokens) : undefined,
      bufferedReflectionInputTokens: row.bufferedReflectionInputTokens
        ? Number(row.bufferedReflectionInputTokens)
        : undefined,
      reflectedObservationLineCount: row.reflectedObservationLineCount
        ? Number(row.reflectedObservationLineCount)
        : undefined,
      totalTokensObserved: Number(row.totalTokensObserved || 0),
      observationTokenCount: Number(row.observationTokenCount || 0),
      pendingMessageTokens: Number(row.pendingMessageTokens || 0),
      isReflecting: Boolean(row.isReflecting),
      isObserving: Boolean(row.isObserving),
      isBufferingObservation: row.isBufferingObservation === true || row.isBufferingObservation === 'true',
      isBufferingReflection: row.isBufferingReflection === true || row.isBufferingReflection === 'true',
      lastBufferedAtTokens:
        typeof row.lastBufferedAtTokens === 'number'
          ? row.lastBufferedAtTokens
          : parseInt(String(row.lastBufferedAtTokens ?? '0'), 10) || 0,
      lastBufferedAtTime: row.lastBufferedAtTime ? new Date(String(row.lastBufferedAtTime)) : null,
      config: row.config ? (typeof row.config === 'string' ? JSON.parse(row.config) : row.config) : {},
      metadata: row.metadata ? (typeof row.metadata === 'string' ? JSON.parse(row.metadata) : row.metadata) : undefined,
      observedMessageIds: row.observedMessageIds
        ? typeof row.observedMessageIds === 'string'
          ? JSON.parse(row.observedMessageIds)
          : row.observedMessageIds
        : undefined,
      observedTimezone: row.observedTimezone || undefined,
    };
  }

  async getObservationalMemory(threadId: string | null, resourceId: string): Promise<ObservationalMemoryRecord | null> {
    try {
      const lookupKey = this.getOMKey(threadId, resourceId);
      const tableName = getTableName({
        indexName: OM_TABLE,
        schemaName: getSchemaName(this.#schema),
      });
      const result = await this.#db.client.oneOrNone(
        `SELECT * FROM ${tableName} WHERE "lookupKey" = $1 ORDER BY "generationCount" DESC LIMIT 1`,
        [lookupKey],
      );
      if (!result) return null;
      return this.parseOMRow(result);
    } catch (error) {
      throw new MastraError(
        {
          id: createStorageErrorId('PG', 'GET_OBSERVATIONAL_MEMORY', 'FAILED'),
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
          details: { threadId, resourceId },
        },
        error,
      );
    }
  }

  async getObservationalMemoryHistory(
    threadId: string | null,
    resourceId: string,
    limit: number = 10,
    options?: ObservationalMemoryHistoryOptions,
  ): Promise<ObservationalMemoryRecord[]> {
    try {
      const lookupKey = this.getOMKey(threadId, resourceId);
      const tableName = getTableName({
        indexName: OM_TABLE,
        schemaName: getSchemaName(this.#schema),
      });

      const conditions = [`"lookupKey" = $1`];
      const params: unknown[] = [lookupKey];
      let paramIndex = 2;

      if (options?.from) {
        conditions.push(`"createdAtZ" >= $${paramIndex}`);
        params.push(options.from.toISOString());
        paramIndex++;
      }
      if (options?.to) {
        conditions.push(`"createdAtZ" <= $${paramIndex}`);
        params.push(options.to.toISOString());
        paramIndex++;
      }

      params.push(limit);
      let sql = `SELECT * FROM ${tableName} WHERE ${conditions.join(' AND ')} ORDER BY "generationCount" DESC LIMIT $${paramIndex}`;
      paramIndex++;

      if (options?.offset != null) {
        params.push(options.offset);
        sql += ` OFFSET $${paramIndex}`;
      }

      const result = await this.#db.client.manyOrNone(sql, params);
      if (!result) return [];
      return result.map(row => this.parseOMRow(row));
    } catch (error) {
      throw new MastraError(
        {
          id: createStorageErrorId('PG', 'GET_OBSERVATIONAL_MEMORY_HISTORY', 'FAILED'),
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
          details: { threadId, resourceId, limit },
        },
        error,
      );
    }
  }

  async initializeObservationalMemory(input: CreateObservationalMemoryInput): Promise<ObservationalMemoryRecord> {
    try {
      const id = crypto.randomUUID();
      const now = new Date();
      const lookupKey = this.getOMKey(input.threadId, input.resourceId);

      const record: ObservationalMemoryRecord = {
        id,
        scope: input.scope,
        threadId: input.threadId,
        resourceId: input.resourceId,
        createdAt: now,
        updatedAt: now,
        lastObservedAt: undefined,
        originType: 'initial',
        generationCount: 0,
        activeObservations: '',
        totalTokensObserved: 0,
        observationTokenCount: 0,
        pendingMessageTokens: 0,
        isReflecting: false,
        isObserving: false,
        isBufferingObservation: false,
        isBufferingReflection: false,
        lastBufferedAtTokens: 0,
        lastBufferedAtTime: null,
        config: input.config,
        observedTimezone: input.observedTimezone,
      };

      const tableName = getTableName({
        indexName: OM_TABLE,
        schemaName: getSchemaName(this.#schema),
      });
      const nowStr = now.toISOString();
      const storedRecord = await this.#db.client.tx(async t => {
        await this.lockObservationalMemoryResource(t, input.resourceId);
        const existing = await t.oneOrNone(
          `SELECT * FROM ${tableName}
           WHERE "lookupKey" = $1
           ORDER BY "generationCount" DESC
           LIMIT 1`,
          [lookupKey],
        );
        if (existing !== null) return this.parseOMRow(existing);
        await t.none(
          `INSERT INTO ${tableName} (
          id, "lookupKey", scope, "resourceId", "threadId",
          "activeObservations", "activeObservationsPendingUpdate",
          "originType", config, "generationCount", "lastObservedAt", "lastObservedAtZ", "lastReflectionAt", "lastReflectionAtZ",
          "pendingMessageTokens", "totalTokensObserved", "observationTokenCount",
          "isObserving", "isReflecting", "isBufferingObservation", "isBufferingReflection", "lastBufferedAtTokens", "lastBufferedAtTime",
          "observedTimezone", "createdAt", "createdAtZ", "updatedAt", "updatedAtZ"
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28)`,
          [
            id,
            lookupKey,
            input.scope,
            input.resourceId,
            input.threadId || null,
            '',
            null,
            'initial',
            JSON.stringify(input.config),
            0,
            null, // lastObservedAt
            null, // lastObservedAtZ
            null, // lastReflectionAt
            null, // lastReflectionAtZ
            0,
            0,
            0,
            false,
            false,
            false, // isBufferingObservation
            false, // isBufferingReflection
            0, // lastBufferedAtTokens
            null, // lastBufferedAtTime
            input.observedTimezone || null,
            nowStr, // createdAt
            nowStr, // createdAtZ
            nowStr, // updatedAt
            nowStr, // updatedAtZ
          ],
        );
        return record;
      });

      return storedRecord;
    } catch (error) {
      throw new MastraError(
        {
          id: createStorageErrorId('PG', 'INITIALIZE_OBSERVATIONAL_MEMORY', 'FAILED'),
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
          details: { threadId: input.threadId, resourceId: input.resourceId },
        },
        error,
      );
    }
  }

  async insertObservationalMemoryRecord(record: ObservationalMemoryRecord): Promise<void> {
    try {
      const lookupKey = this.getOMKey(record.threadId, record.resourceId);
      const tableName = getTableName({
        indexName: OM_TABLE,
        schemaName: getSchemaName(this.#schema),
      });
      const lastObservedAtStr = record.lastObservedAt ? record.lastObservedAt.toISOString() : null;
      const lastBufferedAtTimeStr = record.lastBufferedAtTime ? record.lastBufferedAtTime.toISOString() : null;
      await this.#db.client.none(
        `INSERT INTO ${tableName} (
          id, "lookupKey", scope, "resourceId", "threadId",
          "activeObservations", "activeObservationsPendingUpdate",
          "originType", config, "generationCount", "lastObservedAt", "lastObservedAtZ", "lastReflectionAt", "lastReflectionAtZ",
          "pendingMessageTokens", "totalTokensObserved", "observationTokenCount",
          "observedMessageIds", "bufferedObservationChunks",
          "bufferedReflection", "bufferedReflectionTokens", "bufferedReflectionInputTokens",
          "reflectedObservationLineCount",
          "isObserving", "isReflecting", "isBufferingObservation", "isBufferingReflection",
          "lastBufferedAtTokens", "lastBufferedAtTime",
          "observedTimezone", metadata, "createdAt", "createdAtZ", "updatedAt", "updatedAtZ"
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35)`,
        [
          record.id,
          lookupKey,
          record.scope,
          record.resourceId,
          record.threadId || null,
          record.activeObservations || '',
          null,
          record.originType || 'initial',
          record.config ? JSON.stringify(record.config) : null,
          record.generationCount || 0,
          lastObservedAtStr,
          lastObservedAtStr,
          null, // lastReflectionAt
          null, // lastReflectionAtZ
          record.pendingMessageTokens || 0,
          record.totalTokensObserved || 0,
          record.observationTokenCount || 0,
          record.observedMessageIds ? JSON.stringify(record.observedMessageIds) : null,
          record.bufferedObservationChunks ? JSON.stringify(record.bufferedObservationChunks) : null,
          record.bufferedReflection || null,
          record.bufferedReflectionTokens ?? null,
          record.bufferedReflectionInputTokens ?? null,
          record.reflectedObservationLineCount ?? null,
          record.isObserving || false,
          record.isReflecting || false,
          record.isBufferingObservation || false,
          record.isBufferingReflection || false,
          record.lastBufferedAtTokens || 0,
          lastBufferedAtTimeStr,
          record.observedTimezone || null,
          record.metadata ? JSON.stringify(record.metadata) : null,
          record.createdAt.toISOString(),
          record.createdAt.toISOString(),
          record.updatedAt.toISOString(),
          record.updatedAt.toISOString(),
        ],
      );
    } catch (error) {
      throw new MastraError(
        {
          id: createStorageErrorId('PG', 'INSERT_OBSERVATIONAL_MEMORY_RECORD', 'FAILED'),
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
          details: { id: record.id, threadId: record.threadId, resourceId: record.resourceId },
        },
        error,
      );
    }
  }

  async updateActiveObservations(input: UpdateActiveObservationsInput): Promise<void> {
    try {
      const now = new Date();
      const tableName = getTableName({
        indexName: OM_TABLE,
        schemaName: getSchemaName(this.#schema),
      });

      const lastObservedAtStr = input.lastObservedAt.toISOString();
      const nowStr = now.toISOString();
      const observedMessageIdsJson = input.observedMessageIds ? JSON.stringify(input.observedMessageIds) : null;
      const result = await this.#db.client.query(
        `UPDATE ${tableName} SET
          "activeObservations" = $1,
          "lastObservedAt" = $2,
          "lastObservedAtZ" = $3,
          "pendingMessageTokens" = 0,
          "observationTokenCount" = $4,
          "totalTokensObserved" = "totalTokensObserved" + $5,
          "observedMessageIds" = $6,
          "updatedAt" = $7,
          "updatedAtZ" = $8
        WHERE id = $9`,
        [
          input.observations,
          lastObservedAtStr,
          lastObservedAtStr,
          Math.round(input.tokenCount),
          Math.round(input.tokenCount),
          observedMessageIdsJson,
          nowStr,
          nowStr,
          input.id,
        ],
      );

      if (result.rowCount === 0) {
        throw new MastraError({
          id: createStorageErrorId('PG', 'UPDATE_ACTIVE_OBSERVATIONS', 'NOT_FOUND'),
          text: `Observational memory record not found: ${input.id}`,
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
          details: { id: input.id },
        });
      }
    } catch (error) {
      if (error instanceof MastraError) {
        throw error;
      }
      throw new MastraError(
        {
          id: createStorageErrorId('PG', 'UPDATE_ACTIVE_OBSERVATIONS', 'FAILED'),
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
          details: { id: input.id },
        },
        error,
      );
    }
  }

  async createReflectionGeneration(input: CreateReflectionGenerationInput): Promise<ObservationalMemoryRecord> {
    try {
      const id = crypto.randomUUID();
      const now = new Date();
      const lookupKey = this.getOMKey(input.currentRecord.threadId, input.currentRecord.resourceId);

      const record: ObservationalMemoryRecord = {
        id,
        scope: input.currentRecord.scope,
        threadId: input.currentRecord.threadId,
        resourceId: input.currentRecord.resourceId,
        createdAt: now,
        updatedAt: now,
        lastObservedAt: input.currentRecord.lastObservedAt,
        originType: 'reflection',
        generationCount: input.currentRecord.generationCount + 1,
        activeObservations: input.reflection,
        totalTokensObserved: input.currentRecord.totalTokensObserved,
        observationTokenCount: input.tokenCount,
        pendingMessageTokens: 0,
        isReflecting: false,
        isObserving: false,
        isBufferingObservation: false,
        isBufferingReflection: false,
        lastBufferedAtTokens: 0,
        lastBufferedAtTime: null,
        config: input.currentRecord.config,
        metadata: input.currentRecord.metadata,
        observedTimezone: input.currentRecord.observedTimezone,
      };

      const tableName = getTableName({
        indexName: OM_TABLE,
        schemaName: getSchemaName(this.#schema),
      });
      const nowStr = now.toISOString();
      const lastObservedAtStr = record.lastObservedAt?.toISOString() || null;
      await this.#db.client.tx(async t => {
        await this.lockObservationalMemoryResource(t, input.currentRecord.resourceId);
        await this.assertCurrentObservationalMemoryGeneration(t, {
          recordId: input.currentRecord.id,
          threadId: input.currentRecord.threadId,
          resourceId: input.currentRecord.resourceId,
        });
        await t.none(
          `INSERT INTO ${tableName} (
          id, "lookupKey", scope, "resourceId", "threadId",
          "activeObservations", "activeObservationsPendingUpdate",
          "originType", config, "generationCount", "lastObservedAt", "lastObservedAtZ", "lastReflectionAt", "lastReflectionAtZ",
          "pendingMessageTokens", "totalTokensObserved", "observationTokenCount",
          "isObserving", "isReflecting", "isBufferingObservation", "isBufferingReflection", "lastBufferedAtTokens", "lastBufferedAtTime",
          "observedTimezone", metadata, "createdAt", "createdAtZ", "updatedAt", "updatedAtZ"
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29)`,
          [
            id,
            lookupKey,
            record.scope,
            record.resourceId,
            record.threadId || null,
            input.reflection,
            null,
            'reflection',
            JSON.stringify(record.config),
            input.currentRecord.generationCount + 1,
            lastObservedAtStr, // lastObservedAt
            lastObservedAtStr, // lastObservedAtZ
            nowStr, // lastReflectionAt
            nowStr, // lastReflectionAtZ
            record.pendingMessageTokens,
            Math.round(record.totalTokensObserved),
            Math.round(record.observationTokenCount),
            false, // isObserving
            false, // isReflecting
            false, // isBufferingObservation
            false, // isBufferingReflection
            0, // lastBufferedAtTokens
            null, // lastBufferedAtTime
            record.observedTimezone || null,
            record.metadata ? JSON.stringify(record.metadata) : null,
            nowStr, // createdAt
            nowStr, // createdAtZ
            nowStr, // updatedAt
            nowStr, // updatedAtZ
          ],
        );
      });

      return record;
    } catch (error) {
      throw new MastraError(
        {
          id: createStorageErrorId('PG', 'CREATE_REFLECTION_GENERATION', 'FAILED'),
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
          details: { currentRecordId: input.currentRecord.id },
        },
        error,
      );
    }
  }

  async setReflectingFlag(id: string, isReflecting: boolean): Promise<void> {
    try {
      const tableName = getTableName({
        indexName: OM_TABLE,
        schemaName: getSchemaName(this.#schema),
      });
      const nowStr = new Date().toISOString();
      const result = await this.#db.client.query(
        `UPDATE ${tableName} SET "isReflecting" = $1, "updatedAt" = $2, "updatedAtZ" = $3 WHERE id = $4`,
        [isReflecting, nowStr, nowStr, id],
      );

      if (result.rowCount === 0) {
        throw new MastraError({
          id: createStorageErrorId('PG', 'SET_REFLECTING_FLAG', 'NOT_FOUND'),
          text: `Observational memory record not found: ${id}`,
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
          details: { id, isReflecting },
        });
      }
    } catch (error) {
      if (error instanceof MastraError) {
        throw error;
      }
      throw new MastraError(
        {
          id: createStorageErrorId('PG', 'SET_REFLECTING_FLAG', 'FAILED'),
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
          details: { id, isReflecting },
        },
        error,
      );
    }
  }

  async setObservingFlag(id: string, isObserving: boolean): Promise<void> {
    try {
      const tableName = getTableName({
        indexName: OM_TABLE,
        schemaName: getSchemaName(this.#schema),
      });
      const nowStr = new Date().toISOString();
      const result = await this.#db.client.query(
        `UPDATE ${tableName} SET "isObserving" = $1, "updatedAt" = $2, "updatedAtZ" = $3 WHERE id = $4`,
        [isObserving, nowStr, nowStr, id],
      );

      if (result.rowCount === 0) {
        throw new MastraError({
          id: createStorageErrorId('PG', 'SET_OBSERVING_FLAG', 'NOT_FOUND'),
          text: `Observational memory record not found: ${id}`,
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
          details: { id, isObserving },
        });
      }
    } catch (error) {
      if (error instanceof MastraError) {
        throw error;
      }
      throw new MastraError(
        {
          id: createStorageErrorId('PG', 'SET_OBSERVING_FLAG', 'FAILED'),
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
          details: { id, isObserving },
        },
        error,
      );
    }
  }

  async setBufferingObservationFlag(id: string, isBuffering: boolean, lastBufferedAtTokens?: number): Promise<void> {
    try {
      const tableName = getTableName({
        indexName: OM_TABLE,
        schemaName: getSchemaName(this.#schema),
      });
      const nowStr = new Date().toISOString();

      let query: string;
      let values: any[];

      if (lastBufferedAtTokens !== undefined) {
        query = `UPDATE ${tableName} SET "isBufferingObservation" = $1, "lastBufferedAtTokens" = $2, "updatedAt" = $3, "updatedAtZ" = $4 WHERE id = $5`;
        values = [isBuffering, Math.round(lastBufferedAtTokens), nowStr, nowStr, id];
      } else {
        query = `UPDATE ${tableName} SET "isBufferingObservation" = $1, "updatedAt" = $2, "updatedAtZ" = $3 WHERE id = $4`;
        values = [isBuffering, nowStr, nowStr, id];
      }

      const result = await this.#db.client.query(query, values);

      if (result.rowCount === 0) {
        throw new MastraError({
          id: createStorageErrorId('PG', 'SET_BUFFERING_OBSERVATION_FLAG', 'NOT_FOUND'),
          text: `Observational memory record not found: ${id}`,
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
          details: { id, isBuffering, lastBufferedAtTokens: lastBufferedAtTokens ?? null },
        });
      }
    } catch (error) {
      if (error instanceof MastraError) {
        throw error;
      }
      throw new MastraError(
        {
          id: createStorageErrorId('PG', 'SET_BUFFERING_OBSERVATION_FLAG', 'FAILED'),
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
          details: { id, isBuffering, lastBufferedAtTokens: lastBufferedAtTokens ?? null },
        },
        error,
      );
    }
  }

  async setBufferingReflectionFlag(id: string, isBuffering: boolean): Promise<void> {
    try {
      const tableName = getTableName({
        indexName: OM_TABLE,
        schemaName: getSchemaName(this.#schema),
      });
      const nowStr = new Date().toISOString();
      const result = await this.#db.client.query(
        `UPDATE ${tableName} SET "isBufferingReflection" = $1, "updatedAt" = $2, "updatedAtZ" = $3 WHERE id = $4`,
        [isBuffering, nowStr, nowStr, id],
      );

      if (result.rowCount === 0) {
        throw new MastraError({
          id: createStorageErrorId('PG', 'SET_BUFFERING_REFLECTION_FLAG', 'NOT_FOUND'),
          text: `Observational memory record not found: ${id}`,
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
          details: { id, isBuffering },
        });
      }
    } catch (error) {
      if (error instanceof MastraError) {
        throw error;
      }
      throw new MastraError(
        {
          id: createStorageErrorId('PG', 'SET_BUFFERING_REFLECTION_FLAG', 'FAILED'),
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
          details: { id, isBuffering },
        },
        error,
      );
    }
  }

  async clearObservationalMemory(threadId: string | null, resourceId: string): Promise<void> {
    try {
      const lookupKey = this.getOMKey(threadId, resourceId);
      const tableName = getTableName({
        indexName: OM_TABLE,
        schemaName: getSchemaName(this.#schema),
      });
      await this.#db.client.tx(async t => {
        await this.lockObservationalMemoryResource(t, resourceId);
        await t.none(`DELETE FROM ${tableName} WHERE "lookupKey" = $1`, [lookupKey]);
      });
    } catch (error) {
      throw new MastraError(
        {
          id: createStorageErrorId('PG', 'CLEAR_OBSERVATIONAL_MEMORY', 'FAILED'),
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
          details: { threadId, resourceId },
        },
        error,
      );
    }
  }

  private async retractObservationalMemoryInTransaction(
    t: TxClient,
    input: RetractObservationalMemoryInput,
  ): Promise<RetractObservationalMemoryResult> {
    const omTableName = getTableName({ indexName: OM_TABLE, schemaName: getSchemaName(this.#schema) });
    const resourceTableName = getTableName({
      indexName: TABLE_RESOURCES,
      schemaName: getSchemaName(this.#schema),
    });
    const threadTableName = getTableName({
      indexName: TABLE_THREADS,
      schemaName: getSchemaName(this.#schema),
    });
    const resourceLookupKey = this.getOMKey(null, input.resourceId);
    const threadLookupKey = this.getOMKey(input.threadId, input.resourceId);

    await this.lockObservationalMemoryResource(t, input.resourceId);
    const records = await t.manyOrNone<{ lookupKey: string; config: unknown }>(
      `SELECT "lookupKey", config FROM ${omTableName}
       WHERE "lookupKey" IN ($1, $2)
       FOR UPDATE`,
      [resourceLookupKey, threadLookupKey],
    );
    await t.none(`DELETE FROM ${omTableName} WHERE "lookupKey" IN ($1, $2)`, [resourceLookupKey, threadLookupKey]);

    const lookupKeys = new Set(records.map(record => record.lookupKey));
    const managedWorkingMemoryScopes = new Set(
      records
        .map(record => getManagedWorkingMemoryScope(record.config))
        .filter((scope): scope is 'thread' | 'resource' => scope !== undefined),
    );
    let clearedResourceWorkingMemory = false;
    let clearedThreadMetadata = false;
    if (lookupKeys.size > 0) {
      const now = new Date().toISOString();
      if (managedWorkingMemoryScopes.has('resource')) {
        const resource = await t.oneOrNone<{ workingMemory: string | null; metadata: unknown }>(
          `SELECT "workingMemory", metadata FROM ${resourceTableName} WHERE id = $1 FOR UPDATE`,
          [input.resourceId],
        );
        if (resource) {
          const metadata = parseMetadata(resource.metadata);
          if (hasWorkingMemorySnapshotControls(metadata)) {
            const current = readWorkingMemorySnapshot(resource.workingMemory, metadata);
            const next = retractObserverWorkingMemorySnapshot(current);
            clearedResourceWorkingMemory = next.value !== current.value;
            if (next !== current) {
              await t.none(
                `UPDATE ${resourceTableName}
                 SET "workingMemory" = $1, metadata = $2, "updatedAt" = $3, "updatedAtZ" = $4
                 WHERE id = $5`,
                [
                  next.value,
                  JSON.stringify(writeWorkingMemorySnapshotMetadata(metadata, next)),
                  now,
                  now,
                  input.resourceId,
                ],
              );
            }
          } else if (resource.workingMemory !== null) {
            await t.none(
              `UPDATE ${resourceTableName}
               SET "workingMemory" = NULL, "updatedAt" = $1, "updatedAtZ" = $2
               WHERE id = $3`,
              [now, now, input.resourceId],
            );
            clearedResourceWorkingMemory = true;
          }
        }
      }

      const resourceScopeCleared = lookupKeys.has(resourceLookupKey);
      const clearThreadWorkingMemory = managedWorkingMemoryScopes.has('thread');
      const threadSelector = resourceScopeCleared ? `"resourceId" = $1` : `id = $1 AND "resourceId" = $2`;
      const threadSelectorValues = resourceScopeCleared ? [input.resourceId] : [input.threadId, input.resourceId];
      const metadataSelector = clearThreadWorkingMemory
        ? `(jsonb_typeof(metadata->'mastra'->'om') = 'object'
           OR COALESCE(metadata, '{}'::jsonb) ? 'workingMemory'
           OR COALESCE(metadata->'mastra', '{}'::jsonb) ? 'workingMemory')`
        : `jsonb_typeof(metadata->'mastra'->'om') = 'object'`;
      const threads = await t.manyOrNone<{ id: string; title: string; metadata: unknown }>(
        `SELECT id, title, metadata FROM ${threadTableName}
         WHERE ${threadSelector} AND ${metadataSelector}
         FOR UPDATE`,
        threadSelectorValues,
      );
      for (const thread of threads) {
        const metadata = parseMetadata(thread.metadata);
        const mastra = parseMetadata(metadata.mastra);
        const om = parseMetadata(mastra.om);
        const derivedTitle = typeof om.threadTitle === 'string' ? om.threadTitle : undefined;
        delete mastra.om;
        let nextMetadata: Record<string, unknown> = { ...metadata, mastra };

        if (clearThreadWorkingMemory) {
          if (hasWorkingMemorySnapshotControls(metadata)) {
            const current = readWorkingMemorySnapshot(
              typeof metadata.workingMemory === 'string' ? metadata.workingMemory : null,
              metadata,
            );
            const next = retractObserverWorkingMemorySnapshot(current);
            nextMetadata = writeWorkingMemorySnapshotMetadata(nextMetadata, next);
            if (next.value === null) delete nextMetadata.workingMemory;
            else nextMetadata.workingMemory = next.value;
          } else {
            delete nextMetadata.workingMemory;
          }
        }

        await t.none(
          `UPDATE ${threadTableName}
           SET title = $1, metadata = $2, "updatedAt" = $3, "updatedAtZ" = $4
           WHERE id = $5`,
          [derivedTitle === thread.title ? '' : thread.title, JSON.stringify(nextMetadata), now, now, thread.id],
        );
      }
      clearedThreadMetadata = threads.length > 0;
    }

    return {
      clearedScopes: [
        ...(lookupKeys.has(resourceLookupKey) ? (['resource'] as const) : []),
        ...(lookupKeys.has(threadLookupKey) ? (['thread'] as const) : []),
      ],
      clearedResourceWorkingMemory,
      clearedThreadMetadata,
    };
  }

  private async retractObservationalMemoryForMessageRowsInTransaction(
    t: TxClient,
    messages: ReadonlyArray<{ threadId?: string | null; resourceId?: string | null }>,
  ): Promise<ObservationalMemoryRetractionReceipt[]> {
    const unresolvedThreadIds = [
      ...new Set(
        messages.filter(message => message.threadId && !message.resourceId).map(message => message.threadId as string),
      ),
    ];
    const threadResources = new Map<string, string>();
    if (unresolvedThreadIds.length > 0) {
      const threadTableName = getTableName({
        indexName: TABLE_THREADS,
        schemaName: getSchemaName(this.#schema),
      });
      const threads = await t.manyOrNone<{ threadId: string; resourceId: string }>(
        `SELECT id AS "threadId", "resourceId"
         FROM ${threadTableName}
         WHERE id IN (${inPlaceholders(unresolvedThreadIds.length)})`,
        unresolvedThreadIds,
      );
      for (const thread of threads) {
        threadResources.set(thread.threadId, thread.resourceId);
      }
    }

    const coordinates = new Map<string, RetractObservationalMemoryInput>();
    for (const message of messages) {
      if (!message.threadId) continue;
      const resourceId = message.resourceId ?? threadResources.get(message.threadId);
      if (!resourceId) continue;
      coordinates.set(`${resourceId}\u0000${message.threadId}`, {
        resourceId,
        threadId: message.threadId,
      });
    }

    const receipts: ObservationalMemoryRetractionReceipt[] = [];
    for (const input of [...coordinates.values()].sort((a, b) =>
      `${a.resourceId}\u0000${a.threadId}`.localeCompare(`${b.resourceId}\u0000${b.threadId}`),
    )) {
      const result = await this.retractObservationalMemoryInTransaction(t, input);
      receipts.push({ input, result });
    }
    return receipts;
  }

  async retractObservationalMemory(input: RetractObservationalMemoryInput): Promise<RetractObservationalMemoryResult> {
    try {
      return await this.#db.client.tx(t => this.retractObservationalMemoryInTransaction(t, input));
    } catch (error) {
      throw new MastraError(
        {
          id: createStorageErrorId('PG', 'RETRACT_OBSERVATIONAL_MEMORY', 'FAILED'),
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
          details: { threadId: input.threadId, resourceId: input.resourceId },
        },
        error,
      );
    }
  }

  async setPendingMessageTokens(id: string, tokenCount: number): Promise<void> {
    try {
      const tableName = getTableName({
        indexName: OM_TABLE,
        schemaName: getSchemaName(this.#schema),
      });
      const nowStr = new Date().toISOString();
      const result = await this.#db.client.query(
        `UPDATE ${tableName} SET 
          "pendingMessageTokens" = $1, 
          "updatedAt" = $2,
          "updatedAtZ" = $3
        WHERE id = $4`,
        [Math.round(tokenCount), nowStr, nowStr, id],
      );

      if (result.rowCount === 0) {
        throw new MastraError({
          id: createStorageErrorId('PG', 'SET_PENDING_MESSAGE_TOKENS', 'NOT_FOUND'),
          text: `Observational memory record not found: ${id}`,
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
          details: { id, tokenCount },
        });
      }
    } catch (error) {
      if (error instanceof MastraError) {
        throw error;
      }
      throw new MastraError(
        {
          id: createStorageErrorId('PG', 'SET_PENDING_MESSAGE_TOKENS', 'FAILED'),
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
          details: { id, tokenCount },
        },
        error,
      );
    }
  }

  async updateObservationalMemoryConfig(input: UpdateObservationalMemoryConfigInput): Promise<void> {
    try {
      const tableName = getTableName({
        indexName: OM_TABLE,
        schemaName: getSchemaName(this.#schema),
      });

      // Read current config
      const selectResult = await this.#db.client.query(`SELECT config FROM ${tableName} WHERE id = $1`, [input.id]);

      if (selectResult.rowCount === 0) {
        throw new MastraError({
          id: createStorageErrorId('PG', 'UPDATE_OM_CONFIG', 'NOT_FOUND'),
          text: `Observational memory record not found: ${input.id}`,
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
          details: { id: input.id },
        });
      }

      const row = selectResult.rows[0];
      const existing: Record<string, unknown> = row.config
        ? typeof row.config === 'string'
          ? JSON.parse(row.config)
          : row.config
        : {};
      const merged = this.deepMergeConfig(existing, input.config);
      const nowStr = new Date().toISOString();

      await this.#db.client.query(
        `UPDATE ${tableName} SET config = $1, "updatedAt" = $2, "updatedAtZ" = $3 WHERE id = $4`,
        [JSON.stringify(merged), nowStr, nowStr, input.id],
      );
    } catch (error) {
      if (error instanceof MastraError) {
        throw error;
      }
      throw new MastraError(
        {
          id: createStorageErrorId('PG', 'UPDATE_OM_CONFIG', 'FAILED'),
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
          details: { id: input.id },
        },
        error,
      );
    }
  }

  // ============================================
  // Async Buffering Methods
  // ============================================

  async updateBufferedObservations(input: UpdateBufferedObservationsInput): Promise<void> {
    try {
      const tableName = getTableName({
        indexName: OM_TABLE,
        schemaName: getSchemaName(this.#schema),
      });
      const nowStr = new Date().toISOString();

      // Create new chunk with ID and timestamp
      const newChunk: BufferedObservationChunk = {
        id: `ombuf-${randomUUID()}`,
        cycleId: input.chunk.cycleId,
        observations: input.chunk.observations,
        tokenCount: Math.round(input.chunk.tokenCount),
        messageIds: input.chunk.messageIds,
        messageTokens: Math.round(input.chunk.messageTokens ?? 0),
        lastObservedAt: input.chunk.lastObservedAt,
        createdAt: new Date(),
        suggestedContinuation: input.chunk.suggestedContinuation,
        currentTask: input.chunk.currentTask,
        threadTitle: input.chunk.threadTitle,
        extractedValues: input.chunk.extractedValues,
        extractionFailures: input.chunk.extractionFailures,
      };

      // Append chunk to existing array using JSONB concatenation
      const lastBufferedAtTime = input.lastBufferedAtTime ? input.lastBufferedAtTime.toISOString() : null;
      const result = await this.#db.client.query(
        `UPDATE ${tableName} SET
          "bufferedObservationChunks" = COALESCE("bufferedObservationChunks", '[]'::jsonb) || $1::jsonb,
          "lastBufferedAtTime" = COALESCE($2, "lastBufferedAtTime"),
          "updatedAt" = $3,
          "updatedAtZ" = $4
        WHERE id = $5`,
        [JSON.stringify([newChunk]), lastBufferedAtTime, nowStr, nowStr, input.id],
      );

      if (result.rowCount === 0) {
        throw new MastraError({
          id: createStorageErrorId('PG', 'UPDATE_BUFFERED_OBSERVATIONS', 'NOT_FOUND'),
          text: `Observational memory record not found: ${input.id}`,
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
          details: { id: input.id },
        });
      }
    } catch (error) {
      if (error instanceof MastraError) {
        throw error;
      }
      throw new MastraError(
        {
          id: createStorageErrorId('PG', 'UPDATE_BUFFERED_OBSERVATIONS', 'FAILED'),
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
          details: { id: input.id },
        },
        error,
      );
    }
  }

  async swapBufferedToActive(input: SwapBufferedToActiveInput): Promise<SwapBufferedToActiveResult> {
    try {
      const tableName = getTableName({
        indexName: OM_TABLE,
        schemaName: getSchemaName(this.#schema),
      });
      const nowStr = new Date().toISOString();

      // Get current record
      const record = await this.#db.client.oneOrNone(`SELECT * FROM ${tableName} WHERE id = $1`, [input.id]);
      if (!record) {
        throw new MastraError({
          id: createStorageErrorId('PG', 'SWAP_BUFFERED_TO_ACTIVE', 'NOT_FOUND'),
          text: `Observational memory record not found: ${input.id}`,
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
          details: { id: input.id },
        });
      }

      // Parse buffered chunks
      let chunks: BufferedObservationChunk[] = [];
      if (record.bufferedObservationChunks) {
        try {
          const parsed =
            typeof record.bufferedObservationChunks === 'string'
              ? JSON.parse(record.bufferedObservationChunks)
              : record.bufferedObservationChunks;
          chunks = Array.isArray(parsed) ? parsed : [];
        } catch {
          chunks = [];
        }
      }

      if (chunks.length === 0) {
        return {
          chunksActivated: 0,
          messageTokensActivated: 0,
          observationTokensActivated: 0,
          messagesActivated: 0,
          activatedCycleIds: [],
          activatedMessageIds: [],
        };
      }

      // Calculate target message tokens to activate based on new formula:
      // retentionFloor = threshold * (1 - ratio) represents tokens to keep as raw messages
      // targetMessageTokens = max(0, currentPending - retentionFloor) represents tokens to activate
      const retentionFloor = input.messageTokensThreshold * (1 - input.activationRatio);
      const targetMessageTokens = Math.max(0, input.currentPendingTokens - retentionFloor);

      // Find the closest chunk boundary to the target, biased over (prefer removing
      // slightly more than the target so remaining context lands at or below retentionFloor).
      // Track both best-over and best-under boundaries so we can fall back to under
      // if the over boundary would overshoot by too much.
      let cumulativeMessageTokens = 0;
      let chunksToActivate = 0;
      let bestOverBoundary = 0;
      let bestOverTokens = 0;
      let bestUnderBoundary = 0;
      let bestUnderTokens = 0;

      for (let i = 0; i < chunks.length; i++) {
        cumulativeMessageTokens += chunks[i]!.messageTokens ?? 0;
        const boundary = i + 1;

        if (cumulativeMessageTokens >= targetMessageTokens) {
          // Over or equal — track the closest (lowest) over boundary
          if (bestOverBoundary === 0 || cumulativeMessageTokens < bestOverTokens) {
            bestOverBoundary = boundary;
            bestOverTokens = cumulativeMessageTokens;
          }
        } else {
          // Under — track the closest (highest) under boundary
          if (cumulativeMessageTokens > bestUnderTokens) {
            bestUnderBoundary = boundary;
            bestUnderTokens = cumulativeMessageTokens;
          }
        }
      }

      // Safeguard: if the over boundary would eat into more than 95% of the
      // retention floor, fall back to the best under boundary instead.
      // This prevents edge cases where a large chunk overshoots dramatically.
      // When forceMaxActivation is set (above blockAfter), still prefer the over
      // boundary, but never if it would leave fewer than the smaller of 1000
      // tokens or the retention floor remaining.
      const maxOvershoot = retentionFloor * 0.95;
      const overshoot = bestOverTokens - targetMessageTokens;
      const remainingAfterOver = input.currentPendingTokens - bestOverTokens;
      const remainingAfterUnder = input.currentPendingTokens - bestUnderTokens;
      // When activationRatio ≈ 1.0, retentionFloor is 0 and minRemaining becomes 0 — intentional for "activate everything" configs.
      const minRemaining = Math.min(1000, retentionFloor);

      if (input.forceMaxActivation && bestOverBoundary > 0 && remainingAfterOver >= minRemaining) {
        chunksToActivate = bestOverBoundary;
      } else if (bestOverBoundary > 0 && overshoot <= maxOvershoot && remainingAfterOver >= minRemaining) {
        chunksToActivate = bestOverBoundary;
      } else if (bestUnderBoundary > 0 && remainingAfterUnder >= minRemaining) {
        chunksToActivate = bestUnderBoundary;
      } else if (bestOverBoundary > 0) {
        // All boundaries are over and exceed the safeguard — still activate
        // the closest over boundary (better than nothing)
        chunksToActivate = bestOverBoundary;
      } else {
        chunksToActivate = 1;
      }

      // Split chunks
      const activatedChunks = chunks.slice(0, chunksToActivate);
      const remainingChunks = chunks.slice(chunksToActivate);

      // Combine activated observations
      const activatedContent = activatedChunks.map(c => c.observations).join('\n\n');
      const activatedTokens = Math.round(activatedChunks.reduce((sum, c) => sum + c.tokenCount, 0));
      const activatedMessageTokens = Math.round(activatedChunks.reduce((sum, c) => sum + (c.messageTokens ?? 0), 0));
      const activatedMessageCount = activatedChunks.reduce((sum, c) => sum + c.messageIds.length, 0);
      const activatedCycleIds = activatedChunks.map(c => c.cycleId).filter((id): id is string => !!id);
      const activatedMessageIds = activatedChunks.flatMap(c => c.messageIds ?? []);

      // Derive lastObservedAt from the latest activated chunk, or use provided value
      const latestChunk = activatedChunks[activatedChunks.length - 1];
      const lastObservedAt =
        input.lastObservedAt ?? (latestChunk?.lastObservedAt ? new Date(latestChunk.lastObservedAt) : new Date());
      const lastObservedAtStr = lastObservedAt.toISOString();

      // NOTE: We intentionally do NOT add message IDs to observedMessageIds during buffered activation.
      // Buffered chunks represent observations of messages as they were at buffering time.
      // With streaming, messages grow after buffering, so we rely on lastObservedAt for filtering.
      // New content after lastObservedAt will be picked up in subsequent observations.

      // Atomic conditional update — the WHERE clause ensures chunks haven't already
      // been swapped by a concurrent run. If another run cleared the chunks first,
      // this UPDATE matches 0 rows and we return early with chunksActivated: 0.
      // Include message boundary delimiter for cache stability.
      const boundary = `\n\n--- message boundary (${lastObservedAt.toISOString()}) ---\n\n`;
      const updateResult = await this.#db.client.query(
        `UPDATE ${tableName} SET
          "activeObservations" = CASE
            WHEN "activeObservations" IS NOT NULL AND "activeObservations" != ''
            THEN "activeObservations" || $10 || $1
            ELSE $1
          END,
          "observationTokenCount" = COALESCE("observationTokenCount", 0) + $2,
          "pendingMessageTokens" = GREATEST(0, COALESCE("pendingMessageTokens", 0) - $3),
          "bufferedObservationChunks" = $4,
          "lastObservedAt" = $5,
          "lastObservedAtZ" = $6,
          "updatedAt" = $7,
          "updatedAtZ" = $8
        WHERE id = $9
          AND "bufferedObservationChunks" IS NOT NULL
          AND "bufferedObservationChunks"::text != '[]'`,
        [
          activatedContent,
          activatedTokens,
          activatedMessageTokens,
          remainingChunks.length > 0 ? JSON.stringify(remainingChunks) : null,
          lastObservedAtStr,
          lastObservedAtStr,
          nowStr,
          nowStr,
          input.id,
          boundary,
        ],
      );

      if (updateResult.rowCount === 0) {
        return {
          chunksActivated: 0,
          messageTokensActivated: 0,
          observationTokensActivated: 0,
          messagesActivated: 0,
          activatedCycleIds: [],
          activatedMessageIds: [],
        };
      }

      // Use hints from the most recent activated chunk only — stale hints from older chunks are discarded
      const latestChunkHints = activatedChunks[activatedChunks.length - 1];

      return {
        chunksActivated: activatedChunks.length,
        messageTokensActivated: activatedMessageTokens,
        observationTokensActivated: activatedTokens,
        messagesActivated: activatedMessageCount,
        activatedCycleIds,
        activatedMessageIds,
        observations: activatedContent,
        perChunk: activatedChunks.map(c => ({
          cycleId: c.cycleId ?? '',
          messageTokens: c.messageTokens ?? 0,
          observationTokens: c.tokenCount,
          messageCount: c.messageIds.length,
          observations: c.observations,
        })),
        suggestedContinuation: latestChunkHints?.suggestedContinuation ?? undefined,
        currentTask: latestChunkHints?.currentTask ?? undefined,
      };
    } catch (error) {
      if (error instanceof MastraError) {
        throw error;
      }
      throw new MastraError(
        {
          id: createStorageErrorId('PG', 'SWAP_BUFFERED_TO_ACTIVE', 'FAILED'),
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
          details: { id: input.id },
        },
        error,
      );
    }
  }

  async updateBufferedReflection(input: UpdateBufferedReflectionInput): Promise<void> {
    try {
      const tableName = getTableName({
        indexName: OM_TABLE,
        schemaName: getSchemaName(this.#schema),
      });
      const nowStr = new Date().toISOString();

      // Append reflection to existing buffered content
      const result = await this.#db.client.query(
        `UPDATE ${tableName} SET
          "bufferedReflection" = CASE 
            WHEN "bufferedReflection" IS NOT NULL AND "bufferedReflection" != '' 
            THEN "bufferedReflection" || E'\\n\\n' || $1
            ELSE $1
          END,
          "bufferedReflectionTokens" = COALESCE("bufferedReflectionTokens", 0) + $2,
          "bufferedReflectionInputTokens" = COALESCE("bufferedReflectionInputTokens", 0) + $3,
          "reflectedObservationLineCount" = $4,
          "updatedAt" = $5,
          "updatedAtZ" = $6
        WHERE id = $7`,
        [
          input.reflection,
          Math.round(input.tokenCount),
          Math.round(input.inputTokenCount),
          input.reflectedObservationLineCount,
          nowStr,
          nowStr,
          input.id,
        ],
      );

      if (result.rowCount === 0) {
        throw new MastraError({
          id: createStorageErrorId('PG', 'UPDATE_BUFFERED_REFLECTION', 'NOT_FOUND'),
          text: `Observational memory record not found: ${input.id}`,
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
          details: { id: input.id },
        });
      }
    } catch (error) {
      if (error instanceof MastraError) {
        throw error;
      }
      throw new MastraError(
        {
          id: createStorageErrorId('PG', 'UPDATE_BUFFERED_REFLECTION', 'FAILED'),
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
          details: { id: input.id },
        },
        error,
      );
    }
  }

  async swapBufferedReflectionToActive(input: SwapBufferedReflectionToActiveInput): Promise<ObservationalMemoryRecord> {
    try {
      const tableName = getTableName({
        indexName: OM_TABLE,
        schemaName: getSchemaName(this.#schema),
      });

      // Get current record to calculate split
      const record = await this.#db.client.oneOrNone(`SELECT * FROM ${tableName} WHERE id = $1`, [
        input.currentRecord.id,
      ]);
      if (!record) {
        throw new MastraError({
          id: createStorageErrorId('PG', 'SWAP_BUFFERED_REFLECTION_TO_ACTIVE', 'NOT_FOUND'),
          text: `Observational memory record not found: ${input.currentRecord.id}`,
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
          details: { id: input.currentRecord.id },
        });
      }

      const bufferedReflection = record.bufferedReflection || '';
      const reflectedLineCount = Number(record.reflectedObservationLineCount || 0);

      if (!bufferedReflection) {
        throw new MastraError({
          id: createStorageErrorId('PG', 'SWAP_BUFFERED_REFLECTION_TO_ACTIVE', 'NO_CONTENT'),
          text: 'No buffered reflection to swap',
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.USER,
          details: { id: input.currentRecord.id },
        });
      }

      // Split current activeObservations by the recorded boundary.
      // Lines 0..reflectedLineCount were reflected on → replaced by bufferedReflection.
      // Lines after reflectedLineCount were added after reflection started → kept as-is.
      const currentObservations = (record.activeObservations as string) || '';
      const allLines = currentObservations.split('\n');
      const unreflectedLines = allLines.slice(reflectedLineCount);
      const unreflectedContent = unreflectedLines.join('\n').trim();

      // New activeObservations = bufferedReflection + unreflected observations
      const newObservations = unreflectedContent
        ? `${bufferedReflection}\n\n${unreflectedContent}`
        : bufferedReflection;

      // Create new generation with the merged content.
      // tokenCount is computed by the processor using its token counter on the combined content.
      const newRecord = await this.createReflectionGeneration({
        currentRecord: input.currentRecord,
        reflection: newObservations,
        tokenCount: input.tokenCount,
      });

      // Clear buffered state on old record
      const nowStr = new Date().toISOString();
      await this.#db.client.query(
        `UPDATE ${tableName} SET
          "bufferedReflection" = NULL,
          "bufferedReflectionTokens" = NULL,
          "bufferedReflectionInputTokens" = NULL,
          "reflectedObservationLineCount" = NULL,
          "updatedAt" = $1,
          "updatedAtZ" = $2
        WHERE id = $3`,
        [nowStr, nowStr, input.currentRecord.id],
      );

      return newRecord;
    } catch (error) {
      if (error instanceof MastraError) {
        throw error;
      }
      throw new MastraError(
        {
          id: createStorageErrorId('PG', 'SWAP_BUFFERED_REFLECTION_TO_ACTIVE', 'FAILED'),
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
          details: { id: input.currentRecord.id },
        },
        error,
      );
    }
  }
}
