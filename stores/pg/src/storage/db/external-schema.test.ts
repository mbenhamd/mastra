import { TABLE_PROMPT_BLOCKS, TABLE_PROMPT_BLOCK_VERSIONS, TABLE_SCHEMAS } from '@mastra/core/storage';
import type { StorageColumn } from '@mastra/core/storage';
import { describe, expect, it, vi } from 'vitest';

import type { DbClient, QueryResult } from '../client';
import { ExperimentsPG } from '../domains/experiments';
import { MemoryPG } from '../domains/memory';
import { PromptBlocksPG } from '../domains/prompt-blocks';
import { WorkflowDefinitionsPG } from '../domains/workflow-definitions';
import { PgDB, resolvePgConfig } from '.';

const TABLE = 'mastra_threads' as const;

type ExternalCatalog = {
  columns?: string[];
  indexes?: string[];
};

function createClient(catalog: ExternalCatalog = {}) {
  const catalogQueries: string[] = [];
  const ddlQueries: string[] = [];
  const tableRows = (catalog.columns ?? []).map(column_name => ({ column_name }));
  const indexRows = (catalog.indexes ?? []).map(index_name => ({ index_name }));

  const manyOrNone = vi.fn(async (query: string) => {
    catalogQueries.push(query);
    if (query.includes('pg_catalog.pg_attribute')) return tableRows;
    if (query.includes("index_row.relkind IN ('i', 'I')")) return indexRows;
    throw new Error(`unexpected catalog query: ${query}`);
  });
  const none = vi.fn(async (query: string) => {
    ddlQueries.push(query);
    return null;
  });
  const oneOrNone = vi.fn(async () => null);
  const emptyResult = (): QueryResult =>
    ({ rows: [], rowCount: 0, command: 'SELECT', oid: 0, fields: [] }) as QueryResult;

  const client = {
    $pool: {} as never,
    connect: vi.fn(async () => {
      throw new Error('connect should not be called');
    }),
    none,
    one: vi.fn(async () => {
      throw new Error('one should not be called');
    }),
    oneOrNone,
    any: vi.fn(async () => []),
    manyOrNone,
    many: vi.fn(async () => []),
    query: vi.fn(async () => emptyResult()),
    tx: vi.fn(async <T>(_callback: (tx: never) => Promise<T>) => {
      throw new Error('tx should not be called');
    }),
  } as unknown as DbClient;

  return { client, catalogQueries, ddlQueries, manyOrNone, none, oneOrNone, tableRows, indexRows };
}

const TABLE_SCHEMA: Record<string, StorageColumn> = {
  id: { type: 'text', nullable: false },
  createdAt: { type: 'timestamp', nullable: false },
};

describe('PgDB external schema mode', () => {
  it('validates tables and indexes through read-only catalog checks without issuing DDL', async () => {
    const { client, catalogQueries, ddlQueries } = createClient({
      columns: ['id', 'createdAt', 'createdAtZ'],
      indexes: ['external_threads_idx'],
    });
    const db = new PgDB({ client, schemaName: 'external_schema', disableInit: true });

    await db.createTable({ tableName: TABLE, schema: TABLE_SCHEMA });
    await db.alterTable({ tableName: TABLE, schema: TABLE_SCHEMA, ifNotExists: ['createdAt'] });
    await db.createIndex({ name: 'external_threads_idx', table: TABLE, columns: ['id'] });
    await db.createIndexFromStatement(
      'external_threads_idx',
      'CREATE INDEX external_threads_idx ON mastra_threads (id)',
    );

    expect(ddlQueries).toEqual([]);
    expect(catalogQueries.filter(query => query.includes('pg_catalog.pg_attribute'))).toHaveLength(1);
    expect(catalogQueries.filter(query => query.includes("index_row.relkind IN ('i', 'I')"))).toHaveLength(1);
    expect(catalogQueries.find(query => query.includes("index_row.relkind IN ('i', 'I')"))).toMatch(
      /indisvalid[\s\S]*indisready/,
    );
  });

  it('rejects an incomplete required table before any DDL can run', async () => {
    const { client, ddlQueries } = createClient({ columns: ['id'] });
    const db = new PgDB({ client, schemaName: 'external_schema', disableInit: true });

    await expect(db.createTable({ tableName: TABLE, schema: TABLE_SCHEMA })).rejects.toMatchObject({
      id: 'MASTRA_STORAGE_PG_CREATE_TABLE_FAILED',
      cause: expect.objectContaining({ message: expect.stringContaining('createdAt') }),
    });
    await expect(
      db.createIndexFromStatement('missing_threads_idx', 'CREATE INDEX missing_threads_idx ON mastra_threads (id)'),
    ).rejects.toMatchObject({
      id: 'MASTRA_STORAGE_PG_INDEX_CREATE_FAILED',
      cause: expect.objectContaining({ message: expect.stringContaining('missing required index') }),
    });
    expect(ddlQueries).toEqual([]);
  });

  it('rechecks a table after an external migration repairs a previous miss', async () => {
    const { client, ddlQueries, tableRows } = createClient();
    const db = new PgDB({ client, schemaName: 'external_schema', disableInit: true });

    await expect(db.createTable({ tableName: TABLE, schema: TABLE_SCHEMA })).rejects.toMatchObject({
      id: 'MASTRA_STORAGE_PG_CREATE_TABLE_FAILED',
    });

    tableRows.push({ column_name: 'id' }, { column_name: 'createdAt' }, { column_name: 'createdAtZ' });
    await expect(db.createTable({ tableName: TABLE, schema: TABLE_SCHEMA })).resolves.toBeUndefined();
    expect(ddlQueries).toEqual([]);
  });

  it('refreshes a cached index miss after an external migration repairs it', async () => {
    const { client, ddlQueries, indexRows } = createClient();
    const db = new PgDB({ client, schemaName: 'external_schema', disableInit: true });

    await expect(db.createIndex({ name: 'repaired_threads_idx', table: TABLE, columns: ['id'] })).rejects.toMatchObject(
      {
        id: 'MASTRA_STORAGE_PG_INDEX_CREATE_FAILED',
      },
    );

    indexRows.push({ index_name: 'repaired_threads_idx' });
    await expect(
      db.createIndex({ name: 'repaired_threads_idx', table: TABLE, columns: ['id'] }),
    ).resolves.toBeUndefined();
    expect(ddlQueries).toEqual([]);
  });

  it('returns a typed error for missing indexes whose names contain CONCURRENTLY', async () => {
    const { client, catalogQueries, ddlQueries } = createClient();
    const db = new PgDB({ client, schemaName: 'external_schema', disableInit: true });

    await expect(
      db.createIndex({ name: 'missing_CONCURRENTLY_idx', table: TABLE, columns: ['id'] }),
    ).rejects.toMatchObject({
      id: 'MASTRA_STORAGE_PG_INDEX_CREATE_FAILED',
      cause: expect.objectContaining({ message: expect.stringContaining('missing required index') }),
    });
    expect(catalogQueries.filter(query => query.includes("index_row.relkind IN ('i', 'I')"))).toHaveLength(2);
    expect(ddlQueries).toEqual([]);
  });

  it('exports indexes required by read-only workflow and experiment initialization', () => {
    const workflowDDL = WorkflowDefinitionsPG.getExportDDL('external_schema').join('\n');
    const experimentDDL = ExperimentsPG.getExportDDL('external_schema').join('\n');

    expect(workflowDDL).toContain('"external_schema_idx_workflow_definitions_status"');
    expect(experimentDDL).toContain('"idx_experiments_datasetid"');
  });

  it('bounds schema-prefixed workflow-definition index names for PostgreSQL exports', () => {
    const workflowDDL = WorkflowDefinitionsPG.getExportDDL('s'.repeat(32)).join('\n');
    const indexNames = [...workflowDDL.matchAll(/CREATE INDEX IF NOT EXISTS "([^"]+)"/g)].map(match => match[1]);

    expect(indexNames).toHaveLength(1);
    expect(Buffer.byteLength(indexNames[0]!)).toBeLessThanOrEqual(63);
  });

  it('bounds and distinguishes schema-prefixed native index names', () => {
    const schemaPrefix = `${'s'.repeat(59)}_`;
    const names = [
      ...MemoryPG.getDefaultIndexDefs(schemaPrefix).map(index => index.name),
      ...PromptBlocksPG.getDefaultIndexDefs(schemaPrefix).map(index => index.name),
      ...WorkflowDefinitionsPG.getDefaultIndexDefs(schemaPrefix).map(index => index.name),
    ];

    expect(names.every(name => Buffer.byteLength(name) <= 63)).toBe(true);
    expect(new Set(names).size).toBe(names.length);
  });

  it('fails direct external domain init when a required default index is missing', async () => {
    const columns = [TABLE_PROMPT_BLOCKS, TABLE_PROMPT_BLOCK_VERSIONS].flatMap(tableName =>
      Object.entries(TABLE_SCHEMAS[tableName]).flatMap(([columnName, column]) => [
        columnName,
        ...(column.type === 'timestamp' ? [`${columnName}Z`] : []),
      ]),
    );
    const { client, ddlQueries } = createClient({ columns: [...new Set(columns)] });
    const domain = new PromptBlocksPG({ client, schemaName: 'external_schema', disableInit: true });

    await expect(domain.init()).rejects.toMatchObject({
      id: 'MASTRA_STORAGE_PG_INDEX_CREATE_FAILED',
      cause: expect.objectContaining({ message: expect.stringContaining('missing required index') }),
    });
    expect(ddlQueries).toEqual([]);
  });

  it('propagates disableInit through standalone domain configuration', () => {
    const { client } = createClient();

    expect(resolvePgConfig({ client, disableInit: true })).toMatchObject({
      client,
      readClient: client,
      disableInit: true,
    });
  });

  it('uses the initialization environment guard even when the PgDB was constructed without disableInit', async () => {
    const { client, ddlQueries } = createClient({
      columns: ['id', 'createdAt', 'createdAtZ'],
      indexes: ['external_threads_idx'],
    });
    const db = new PgDB({ client, schemaName: 'external_schema' });

    vi.stubEnv('MASTRA_DISABLE_STORAGE_INIT', 'true');
    try {
      await db.createTable({ tableName: TABLE, schema: TABLE_SCHEMA });
      await db.createIndex({ name: 'external_threads_idx', table: TABLE, columns: ['id'] });
    } finally {
      vi.unstubAllEnvs();
    }

    expect(ddlQueries).toEqual([]);
  });

  it('keeps the explicit spans migration available under the CLI initialization guard', async () => {
    const { client, ddlQueries, oneOrNone } = createClient();
    oneOrNone.mockResolvedValue({ exists: true });
    const db = new PgDB({ client });

    vi.stubEnv('MASTRA_DISABLE_STORAGE_INIT', 'true');
    try {
      await expect(db.migrateSpans()).resolves.toMatchObject({ alreadyMigrated: true });
    } finally {
      vi.unstubAllEnvs();
    }

    expect(ddlQueries).toEqual([]);
  });
});
