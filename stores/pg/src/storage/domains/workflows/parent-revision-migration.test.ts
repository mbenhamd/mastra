import { randomUUID } from 'node:crypto';
import { createEmptyWorkflowSnapshot } from '@mastra/core/storage';
import type { WorkflowRunState } from '@mastra/core/workflows';
import { Pool } from 'pg';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { exportSchemas, PostgresStore } from '../..';
import { PoolAdapter, RoutingDbClient } from '../../client';
import { loadSchemaSnapshot } from '../../db/schema-snapshot';
import { TEST_CONFIG } from '../../test-utils';
import { WorkflowsPG } from '.';

const MARKER_TABLE = 'mastra_workflow_schema_migrations';
const REVISION_TABLE = 'mastra_workflow_parent_revisions';
const EPOCH_TABLE = 'mastra_workflow_parent_revision_migration_epoch';
const SNAPSHOT_TABLE = 'mastra_workflow_snapshot';
const MIGRATION_KEY = 'workflow-parent-revision-v1';
const MIGRATION_REQUIRED =
  'WORKFLOW_PARENT_REVISION_MIGRATION_REQUIRED: run PostgresStore.init() with disableInit=false before applying exported schema to populated workflow storage';

describe('WorkflowsPG parent revision migration', () => {
  const pool = new Pool({
    host: process.env.POSTGRES_HOST || '127.0.0.1',
    port: Number(process.env.POSTGRES_PORT) || 5434,
    database: process.env.POSTGRES_DB || 'postgres',
    user: process.env.POSTGRES_USER || 'postgres',
    password: process.env.POSTGRES_PASSWORD || 'postgres',
  });
  const schemas = new Set<string>();

  beforeAll(async () => {
    await pool.query('SELECT 1');
  });

  afterAll(async () => {
    for (const schema of schemas) {
      await pool.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`);
    }
    await pool.end();
  });

  function uniqueSchema(prefix: string): string {
    const schema = `${prefix}_${randomUUID().replaceAll('-', '').slice(0, 12)}`;
    schemas.add(schema);
    return schema;
  }

  async function createSchema(prefix: string): Promise<string> {
    const schema = uniqueSchema(prefix);
    await pool.query(`CREATE SCHEMA "${schema}"`);
    return schema;
  }

  function tableDDL(schema: string, table: string): string {
    const ddl = WorkflowsPG.getExportDDL(schema).find(
      statement => statement.includes(`"${schema}"."${table}"`) && /CREATE TABLE IF NOT EXISTS/.test(statement),
    );
    if (!ddl) throw new Error(`Missing exported DDL for ${table}`);
    return ddl;
  }

  async function createEvidenceTables(schema: string): Promise<void> {
    await pool.query(tableDDL(schema, 'mastra_workflow_snapshot'));
    await pool.query(tableDDL(schema, 'mastra_workflow_terminalizations'));
  }

  async function createCurrentRevisionTable(schema: string): Promise<void> {
    await pool.query(tableDDL(schema, MARKER_TABLE));
    await pool.query(tableDDL(schema, REVISION_TABLE));
  }

  async function createMigrationEpoch(schema: string, insert = true): Promise<void> {
    await pool.query(
      `CREATE TABLE "${schema}"."${EPOCH_TABLE}" (
         epoch SMALLINT NOT NULL PRIMARY KEY,
         created_at BIGINT NOT NULL,
         CHECK (epoch = 1),
         CHECK (created_at >= 0)
       )`,
    );
    if (insert) {
      await pool.query(`INSERT INTO "${schema}"."${EPOCH_TABLE}" (epoch, created_at) VALUES (1, 1)`);
    }
  }

  async function relationExists(schema: string, table: string): Promise<boolean> {
    const result = await pool.query<{ exists: boolean }>(`SELECT to_regclass($1) IS NOT NULL AS exists`, [
      `"${schema}"."${table}"`,
    ]);
    return result.rows[0]!.exists;
  }

  async function relationCheckName(schema: string, table: string, expression: string): Promise<string> {
    const result = await pool.query<{ conname: string }>(
      `SELECT constraint_row.conname
       FROM pg_catalog.pg_constraint AS constraint_row
       WHERE constraint_row.conrelid = $1::regclass
         AND constraint_row.contype = 'c'
         AND pg_get_expr(constraint_row.conbin, constraint_row.conrelid, true) = $2`,
      [`"${schema}"."${table}"`, expression],
    );
    if (result.rowCount !== 1) throw new Error(`Missing ${table} CHECK ${expression}`);
    return result.rows[0]!.conname;
  }

  async function replacePrivatePrimaryKeyWithDeferrable(
    schema: string,
    table: typeof MARKER_TABLE | typeof EPOCH_TABLE | typeof REVISION_TABLE,
  ): Promise<void> {
    const result = await pool.query<{ conname: string }>(
      `SELECT constraint_row.conname
       FROM pg_catalog.pg_constraint AS constraint_row
       WHERE constraint_row.conrelid = $1::regclass
         AND constraint_row.contype = 'p'`,
      [`"${schema}"."${table}"`],
    );
    if (result.rowCount !== 1) throw new Error(`Missing ${table} PRIMARY KEY`);
    const columns = {
      [MARKER_TABLE]: 'migration_key',
      [EPOCH_TABLE]: 'epoch',
      [REVISION_TABLE]: 'workflow_name, run_id',
    } as const;
    await pool.query(
      `ALTER TABLE "${schema}"."${table}"
       DROP CONSTRAINT "${result.rows[0]!.conname}",
       ADD PRIMARY KEY (${columns[table]}) DEFERRABLE INITIALLY IMMEDIATE`,
    );
  }

  async function waitForBlockedLock({
    pid,
    lockType,
    relation,
    mode,
  }: {
    pid: number;
    lockType: 'advisory' | 'relation';
    relation?: string;
    mode?: 'ExclusiveLock' | 'AccessExclusiveLock';
  }): Promise<void> {
    for (let attempt = 0; attempt < 400; attempt++) {
      const result = await pool.query(
        `SELECT 1
         FROM pg_catalog.pg_locks
         WHERE pid = $1
           AND locktype = $2
           AND NOT granted
           AND ($3::text IS NULL OR relation = to_regclass($3))
           AND ($4::text IS NULL OR mode = $4)`,
        [pid, lockType, relation ?? null, mode ?? null],
      );
      if (result.rowCount) return;
      await new Promise(resolve => setTimeout(resolve, 25));
    }
    throw new Error(`Timed out waiting for blocked ${mode ?? lockType} lock`);
  }

  async function createAllWorkflowTablesExceptRevisionHistory(schema: string): Promise<void> {
    for (const statement of WorkflowsPG.getExportDDL(schema)) {
      if (!/CREATE TABLE IF NOT EXISTS/.test(statement)) continue;
      if (statement.includes(`"${schema}"."${REVISION_TABLE}"`)) continue;
      if (statement.includes(`"${schema}"."${MARKER_TABLE}"`)) continue;
      await pool.query(statement);
    }
  }

  async function insertSnapshot(
    schema: string,
    workflowName: string,
    runId: string,
    status: WorkflowRunState['status'],
    overrides: Partial<WorkflowRunState> = {},
  ): Promise<void> {
    const now = new Date();
    const snapshot = { ...createEmptyWorkflowSnapshot(runId), ...overrides, status };
    await pool.query(
      `INSERT INTO "${schema}"."mastra_workflow_snapshot"
       (workflow_name, run_id, snapshot, "createdAt", "updatedAt", "createdAtZ", "updatedAtZ")
       VALUES ($1, $2, $3, $4, $5, $6, $7)`,
      [workflowName, runId, JSON.stringify(snapshot), now, now, now, now],
    );
  }

  async function insertJournal(
    schema: string,
    workflowName: string,
    runId: string,
    terminalStatus: 'success' | 'failed' | 'canceled',
  ): Promise<void> {
    await pool.query(
      `INSERT INTO "${schema}"."mastra_workflow_terminalizations"
       (workflow_name, run_id, version, event_key, terminal_status, phase, owner_id, claim_token,
        claim_generation, lease_expires_at, created_at, updated_at, completed_at)
       VALUES ($1, $2, 1, $3, $4, 'terminalization_pending', 'owner', 'claim', 1, 1000, 1, 1, NULL)`,
      [workflowName, runId, `${workflowName}-event`, terminalStatus],
    );
  }

  async function createLegacyRevisionTable(schema: string): Promise<void> {
    await pool.query(
      `CREATE TABLE "${schema}"."${REVISION_TABLE}" (
         workflow_name TEXT NOT NULL,
         run_id TEXT NOT NULL,
         generation BIGINT NOT NULL,
         updated_at BIGINT NOT NULL,
         PRIMARY KEY (workflow_name, run_id),
         CHECK (generation >= 0),
         CHECK (updated_at >= 0)
       )`,
    );
  }

  async function markerCount(schema: string): Promise<string> {
    const result = await pool.query<{ count: string }>(
      `SELECT count(*)::text AS count FROM "${schema}"."${MARKER_TABLE}" WHERE migration_key = $1`,
      [MIGRATION_KEY],
    );
    return result.rows[0]!.count;
  }

  async function epochCount(schema: string): Promise<string> {
    const result = await pool.query<{ count: string }>(
      `SELECT count(*)::text AS count FROM "${schema}"."${EPOCH_TABLE}" WHERE epoch = 1`,
    );
    return result.rows[0]!.count;
  }

  async function createPartitionedPrivateRelation(
    schema: string,
    table: typeof MARKER_TABLE | typeof EPOCH_TABLE | typeof REVISION_TABLE,
    placement: 'root' | 'child',
  ): Promise<void> {
    const definitions = {
      [MARKER_TABLE]: {
        columns: 'migration_key TEXT NOT NULL PRIMARY KEY, applied_at BIGINT NOT NULL',
        partitionKey: 'migration_key',
      },
      [EPOCH_TABLE]: {
        columns: 'epoch SMALLINT NOT NULL PRIMARY KEY, created_at BIGINT NOT NULL',
        partitionKey: 'epoch',
      },
      [REVISION_TABLE]: {
        columns:
          'workflow_name TEXT NOT NULL, run_id TEXT NOT NULL, generation BIGINT NOT NULL, terminal_status TEXT, updated_at BIGINT NOT NULL, PRIMARY KEY (workflow_name, run_id)',
        partitionKey: 'workflow_name',
      },
    } as const;
    const definition = definitions[table];
    const root = placement === 'root' ? table : `${table}_partition_root`;
    await pool.query(
      `CREATE TABLE "${schema}"."${root}" (${definition.columns}) PARTITION BY HASH (${definition.partitionKey})`,
    );
    if (placement === 'child') {
      await pool.query(
        `CREATE TABLE "${schema}"."${table}" PARTITION OF "${schema}"."${root}"
         FOR VALUES WITH (MODULUS 1, REMAINDER 0)`,
      );
    }
  }

  async function createInheritedPrivateRelation(
    schema: string,
    table: typeof MARKER_TABLE | typeof EPOCH_TABLE | typeof REVISION_TABLE,
    placement: 'parent' | 'child',
  ): Promise<void> {
    if (placement === 'parent') {
      await pool.query(`CREATE TABLE "${schema}"."${table}_inheritance_child" () INHERITS ("${schema}"."${table}")`);
      return;
    }

    const definitions = {
      [MARKER_TABLE]: {
        columns: `migration_key TEXT NOT NULL,
                  applied_at BIGINT NOT NULL,
                  CHECK (length(migration_key) BETWEEN 1 AND 256),
                  CHECK (applied_at >= 0)`,
        primaryKey: 'migration_key',
      },
      [EPOCH_TABLE]: {
        columns: `epoch SMALLINT NOT NULL,
                  created_at BIGINT NOT NULL,
                  CHECK (epoch = 1),
                  CHECK (created_at >= 0)`,
        primaryKey: 'epoch',
      },
      [REVISION_TABLE]: {
        columns: `workflow_name TEXT NOT NULL,
                  run_id TEXT NOT NULL,
                  generation BIGINT NOT NULL,
                  terminal_status TEXT,
                  updated_at BIGINT NOT NULL,
                  CHECK (generation >= 0),
                  CHECK (terminal_status IS NULL OR terminal_status IN
                    ('success', 'failed', 'canceled', 'tripwire', 'bailed', 'skipped')),
                  CHECK (updated_at >= 0)`,
        primaryKey: 'workflow_name, run_id',
      },
    } as const;
    const definition = definitions[table];
    const parent = `${table}_inheritance_parent`;
    await pool.query(`CREATE TABLE "${schema}"."${parent}" (${definition.columns})`);
    await pool.query(
      `CREATE TABLE "${schema}"."${table}" (PRIMARY KEY (${definition.primaryKey}))
       INHERITS ("${schema}"."${parent}")`,
    );
  }

  async function expectPrivateRelationRejected(schema: string, exportError: string, runtimeError = exportError) {
    await expect(pool.query(exportSchemas(schema))).rejects.toThrow(exportError);

    const client = new RoutingDbClient(new PoolAdapter(pool));
    const snapshot = await loadSchemaSnapshot(client, schema);
    client.setSchemaSnapshot(snapshot);
    try {
      await expect(new WorkflowsPG({ client, schemaName: schema, skipDefaultIndexes: true }).init()).rejects.toThrow(
        runtimeError,
      );
    } finally {
      client.setSchemaSnapshot(null);
    }

    await expect(new WorkflowsPG({ pool, schemaName: schema, skipDefaultIndexes: true }).init()).rejects.toThrow(
      runtimeError,
    );
    return snapshot;
  }

  async function createPartitionedSnapshotTable(schema: string): Promise<void> {
    const ordinary = tableDDL(schema, SNAPSHOT_TABLE);
    const partitioned = ordinary.replace(
      /(\n\s*)\);(\s*\n\s*DO \$\$ BEGIN)/,
      '$1) PARTITION BY HASH (workflow_name);$2',
    );
    if (partitioned === ordinary) throw new Error('Could not convert exported snapshot DDL to a partitioned table');
    await pool.query(partitioned);
  }

  async function snapshotStatusIndexCount(schema: string): Promise<number> {
    const result = await pool.query(
      `SELECT 1
       FROM pg_catalog.pg_index AS index_metadata
       JOIN pg_catalog.pg_class AS index_row ON index_row.oid = index_metadata.indexrelid
       JOIN pg_catalog.pg_class AS table_row ON table_row.oid = index_metadata.indrelid
       JOIN pg_catalog.pg_namespace AS namespace_row ON namespace_row.oid = table_row.relnamespace
       WHERE namespace_row.nspname = $1
         AND table_row.relname = $2
         AND pg_get_indexdef(index_row.oid) LIKE '%snapshot ->>%'`,
      [schema, SNAPSHOT_TABLE],
    );
    return result.rowCount ?? 0;
  }

  it('bootstraps a quoted schema for disableInit without marker data and remains idempotent', async () => {
    const schema = await createSchema('Rx');
    const exported = WorkflowsPG.getExportDDL(schema);
    const markerDDL = exported.find(statement => statement.includes(`"${schema}"."${MARKER_TABLE}"`));
    expect(markerDDL).toContain('PRIMARY KEY');
    const exportedSql = exported.join('\n');
    expect(exportedSql).not.toMatch(new RegExp(`INSERT\\s+INTO\\s+"${schema}"\\."${MARKER_TABLE}"`, 'i'));
    expect(exportedSql).not.toMatch(new RegExp(`CREATE\\s+TABLE[^;]*"${EPOCH_TABLE}"`, 'i'));
    expect(exportedSql).not.toMatch(new RegExp(`INSERT\\s+INTO\\s+"${schema}"\\."${EPOCH_TABLE}"`, 'i'));

    await pool.query(exportSchemas(schema));
    expect(await relationExists(schema, REVISION_TABLE)).toBe(true);
    expect(await markerCount(schema)).toBe('0');

    const external = new PostgresStore({
      ...TEST_CONFIG,
      id: `revision-export-${schema}`,
      schemaName: schema,
      disableInit: true,
    });
    const run = { workflowName: 'export-workflow', runId: 'export-run' };
    try {
      await external.init();
      const workflows = (await external.getStore('workflows'))!;
      await expect(workflows.getWorkflowRunTerminalStatus(run)).resolves.toEqual({ status: 'missing_run' });
      await workflows.persistWorkflowSnapshot({
        ...run,
        snapshot: { ...createEmptyWorkflowSnapshot(run.runId), status: 'running' },
      });
    } finally {
      await external.close();
    }

    const beforeReapply = await pool.query(
      `SELECT generation::text, terminal_status, updated_at::text, xmin::text AS row_version
       FROM "${schema}"."${REVISION_TABLE}"`,
    );
    await pool.query(exportSchemas(schema));
    const afterReapply = await pool.query(
      `SELECT generation::text, terminal_status, updated_at::text, xmin::text AS row_version
       FROM "${schema}"."${REVISION_TABLE}"`,
    );
    expect(afterReapply.rows).toEqual(beforeReapply.rows);
    expect(await markerCount(schema)).toBe('0');

    const privileged = new PostgresStore({
      ...TEST_CONFIG,
      id: `revision-privileged-${schema}`,
      schemaName: schema,
    });
    try {
      await privileged.init();
      const afterAdoption = await pool.query(
        `SELECT generation::text, terminal_status, updated_at::text, xmin::text AS row_version
         FROM "${schema}"."${REVISION_TABLE}"`,
      );
      expect(afterAdoption.rows).toEqual(beforeReapply.rows);
      expect(await markerCount(schema)).toBe('1');
      expect(await epochCount(schema)).toBe('1');
      await (await privileged.getStore('workflows'))!.dangerouslyClearAll();
      expect(await markerCount(schema)).toBe('1');
    } finally {
      await privileged.close();
    }
  });

  it('records only primary-key attributes when indexes carry INCLUDE columns', async () => {
    const schema = await createSchema('revision_snapshot_pk');
    await createEvidenceTables(schema);
    await new WorkflowsPG({ pool, schemaName: schema, skipDefaultIndexes: true }).init();
    await pool.query(
      `ALTER TABLE "${schema}"."${MARKER_TABLE}"
       DROP CONSTRAINT "${MARKER_TABLE}_pkey",
       ADD PRIMARY KEY (migration_key) INCLUDE (applied_at)`,
    );
    await pool.query(
      `ALTER TABLE "${schema}"."${EPOCH_TABLE}"
       DROP CONSTRAINT "${EPOCH_TABLE}_pkey",
       ADD PRIMARY KEY (epoch) INCLUDE (created_at)`,
    );
    await pool.query(
      `ALTER TABLE "${schema}"."${REVISION_TABLE}"
       DROP CONSTRAINT "${REVISION_TABLE}_pkey",
       ADD PRIMARY KEY (workflow_name, run_id) INCLUDE (generation)`,
    );

    // Both live migration inspection and exported-schema validation must
    // treat INCLUDE attributes as payload, not as part of PRIMARY KEY shape.
    await new WorkflowsPG({ pool, schemaName: schema, skipDefaultIndexes: true }).init();
    for (const statement of WorkflowsPG.getExportDDL(schema)) await pool.query(statement);

    const client = new RoutingDbClient(new PoolAdapter(pool));
    client.setSchemaSnapshot(await loadSchemaSnapshot(client, schema));
    const workflows = new WorkflowsPG({ client, schemaName: schema, skipDefaultIndexes: true });

    await workflows.init();

    expect(client.schemaSnapshot?.primaryKeyColumns.get(MARKER_TABLE)).toEqual(['migration_key']);
    expect(client.schemaSnapshot?.primaryKeyColumns.get(EPOCH_TABLE)).toEqual(['epoch']);
    expect(client.schemaSnapshot?.primaryKeyColumns.get(REVISION_TABLE)).toEqual(['workflow_name', 'run_id']);
    expect(client.schemaSnapshot?.immediatePrimaryKeyTables.has(MARKER_TABLE)).toBe(true);
    expect(client.schemaSnapshot?.immediatePrimaryKeyTables.has(EPOCH_TABLE)).toBe(true);
    expect(client.schemaSnapshot?.immediatePrimaryKeyTables.has(REVISION_TABLE)).toBe(true);
    client.setSchemaSnapshot(null);
  });

  it('updates an installed cold snapshot with immediate private keys for a second init', async () => {
    const schema = await createSchema('rsi');
    await createEvidenceTables(schema);
    const client = new RoutingDbClient(new PoolAdapter(pool));
    const snapshot = await loadSchemaSnapshot(client, schema);
    expect(snapshot.tables.has(MARKER_TABLE)).toBe(false);
    expect(snapshot.tables.has(EPOCH_TABLE)).toBe(false);
    expect(snapshot.tables.has(REVISION_TABLE)).toBe(false);
    client.setSchemaSnapshot(snapshot);

    try {
      await expect(
        new WorkflowsPG({ client, schemaName: schema, skipDefaultIndexes: true }).init(),
      ).resolves.toBeUndefined();
      for (const table of [MARKER_TABLE, EPOCH_TABLE, REVISION_TABLE]) {
        expect(snapshot.tables.has(table)).toBe(true);
        expect(snapshot.immediatePrimaryKeyTables.has(table)).toBe(true);
      }
      await expect(
        new WorkflowsPG({ client, schemaName: schema, skipDefaultIndexes: true }).init(),
      ).resolves.toBeUndefined();
    } finally {
      client.setSchemaSnapshot(null);
    }
  });

  it('seeds genuine legacy running, terminal, and journal-only identities exactly once', async () => {
    const schema = await createSchema('revision_legacy');
    await createEvidenceTables(schema);
    await insertSnapshot(schema, 'running-workflow', 'running-run', 'running');
    await insertSnapshot(schema, 'terminal-workflow', 'terminal-run', 'success');
    await insertJournal(schema, 'journal-workflow', 'journal-run', 'failed');

    const workflows = new WorkflowsPG({ pool, schemaName: schema, skipDefaultIndexes: true });
    await workflows.init();
    const first = await pool.query<{
      workflow_name: string;
      run_id: string;
      generation: string;
      terminal_status: string | null;
      row_version: string;
    }>(
      `SELECT workflow_name, run_id, generation::text, terminal_status, xmin::text AS row_version
       FROM "${schema}"."${REVISION_TABLE}" ORDER BY workflow_name`,
    );
    expect(first.rows).toEqual([
      {
        workflow_name: 'journal-workflow',
        run_id: 'journal-run',
        generation: '1',
        terminal_status: 'failed',
        row_version: expect.any(String),
      },
      {
        workflow_name: 'running-workflow',
        run_id: 'running-run',
        generation: '1',
        terminal_status: null,
        row_version: expect.any(String),
      },
      {
        workflow_name: 'terminal-workflow',
        run_id: 'terminal-run',
        generation: '1',
        terminal_status: 'success',
        row_version: expect.any(String),
      },
    ]);
    await workflows.init();
    const repeated = await pool.query(
      `SELECT workflow_name, run_id, generation::text, terminal_status, xmin::text AS row_version
       FROM "${schema}"."${REVISION_TABLE}" ORDER BY workflow_name`,
    );
    expect(repeated.rows).toEqual(first.rows);
  });

  it.each(['jsonb', 'json', 'text'] as const)(
    'migrates genuine pre-revision %s snapshot storage',
    async snapshotType => {
      const schema = await createSchema(`revision_${snapshotType}`);
      await createEvidenceTables(schema);
      if (snapshotType !== 'jsonb') {
        await pool.query(
          `ALTER TABLE "${schema}"."mastra_workflow_snapshot"
           ALTER COLUMN snapshot TYPE ${snapshotType} USING snapshot::${snapshotType}`,
        );
      }
      await insertSnapshot(schema, 'workflow', 'run', 'failed', {
        context: { escapedPattern: '\\u0000' },
      });

      const workflows = new WorkflowsPG({ pool, schemaName: schema, skipDefaultIndexes: true });
      await workflows.init();

      await expect(workflows.getWorkflowRunTerminalStatus({ workflowName: 'workflow', runId: 'run' })).resolves.toEqual(
        { status: 'terminal', terminalStatus: 'failed' },
      );
      const listed = await workflows.listWorkflowRuns({ workflowName: 'workflow', status: 'failed' });
      expect(listed.runs).toHaveLength(1);
      expect(listed.runs[0]!.snapshot).toMatchObject({ context: { escapedPattern: '\\u0000' } });
      await workflows.persistWorkflowSnapshot({
        workflowName: 'workflow',
        runId: 'run',
        snapshot: { ...createEmptyWorkflowSnapshot('run'), status: 'failed' },
      });

      const revision = await pool.query(
        `SELECT generation::text, terminal_status
         FROM "${schema}"."${REVISION_TABLE}" WHERE workflow_name = 'workflow' AND run_id = 'run'`,
      );
      expect(revision.rows).toEqual([{ generation: '2', terminal_status: 'failed' }]);
      expect(await markerCount(schema)).toBe('1');
      expect(await epochCount(schema)).toBe('1');
    },
  );

  it.each(['jsonb', 'json', 'text'] as const)(
    'exports and initializes a partitioned %s snapshot table with only the supported status index',
    async snapshotType => {
      const schemaPrefix = { jsonb: 'pxb', json: 'pxj', text: 'pxt' }[snapshotType];
      const schema = await createSchema(schemaPrefix);
      await createPartitionedSnapshotTable(schema);
      if (snapshotType !== 'jsonb') {
        await pool.query(
          `ALTER TABLE "${schema}"."${SNAPSHOT_TABLE}"
           ALTER COLUMN snapshot TYPE ${snapshotType} USING snapshot::${snapshotType}`,
        );
      }

      await expect(pool.query(exportSchemas(schema))).resolves.toBeDefined();
      expect(await snapshotStatusIndexCount(schema)).toBe(snapshotType === 'jsonb' ? 1 : 0);

      const client = new RoutingDbClient(new PoolAdapter(pool));
      client.setSchemaSnapshot(await loadSchemaSnapshot(client, schema));
      try {
        await expect(
          new WorkflowsPG({ client, schemaName: schema, skipDefaultIndexes: true }).init(),
        ).resolves.toBeUndefined();
      } finally {
        client.setSchemaSnapshot(null);
      }
      await expect(
        new WorkflowsPG({ pool, schemaName: schema, skipDefaultIndexes: true }).init(),
      ).resolves.toBeUndefined();
      expect(await snapshotStatusIndexCount(schema)).toBe(snapshotType === 'jsonb' ? 1 : 0);
    },
  );

  it('rejects an unsupported partitioned snapshot type before creating a status index', async () => {
    const schema = await createSchema('pxu');
    await createPartitionedSnapshotTable(schema);
    await pool.query(
      `ALTER TABLE "${schema}"."${SNAPSHOT_TABLE}"
       ALTER COLUMN snapshot TYPE bytea USING convert_to(snapshot::text, 'UTF8')`,
    );
    const error = 'Workflow parent revision migration does not support snapshot column type bytea';

    await expect(pool.query(exportSchemas(schema))).rejects.toThrow(error);
    expect(await snapshotStatusIndexCount(schema)).toBe(0);

    const client = new RoutingDbClient(new PoolAdapter(pool));
    client.setSchemaSnapshot(await loadSchemaSnapshot(client, schema));
    try {
      await expect(new WorkflowsPG({ client, schemaName: schema, skipDefaultIndexes: true }).init()).rejects.toThrow(
        error,
      );
    } finally {
      client.setSchemaSnapshot(null);
    }
    await expect(new WorkflowsPG({ pool, schemaName: schema, skipDefaultIndexes: true }).init()).rejects.toThrow(error);
    expect(await snapshotStatusIndexCount(schema)).toBe(0);
  });

  it('keeps JavaScript and SQL sanitization parity through text migration, reads, writes, and filtering', async () => {
    const schema = await createSchema('rtext_parity');
    await createEvidenceTables(schema);
    await pool.query(
      `ALTER TABLE "${schema}"."${SNAPSHOT_TABLE}"
       ALTER COLUMN snapshot TYPE text USING snapshot::text`,
    );

    const expectedRawParity: Record<string, string> = {};
    const expectedSanitizedParity: Record<string, string> = {};
    const encodedParity: string[] = [];
    for (const token of ['u0000', 'uD800', 'uDC00'] as const) {
      for (const slashCount of [1, 2, 3, 5]) {
        const key = `${token}_${slashCount}`;
        const slashes = '\\'.repeat(slashCount);
        encodedParity.push(`${JSON.stringify(key)}:${JSON.stringify(`${slashes}${token}`).replaceAll('\\\\', '\\')}`);
        const decodedUnsafe = { u0000: '\u0000', uD800: '\uD800', uDC00: '\uDC00' }[token]!;
        expectedRawParity[key] =
          slashCount % 2 === 0
            ? `${'\\'.repeat(slashCount / 2)}${token}`
            : `${'\\'.repeat((slashCount - 1) / 2)}${decodedUnsafe}`;
        expectedSanitizedParity[key] =
          slashCount % 2 === 0 ? `${'\\'.repeat(slashCount / 2)}${token}` : '\\'.repeat((slashCount - 1) / 2);
      }
    }
    encodedParity.push('"validPair":"\\uD83D\\uDE00"');
    encodedParity.push(`"escapedPair":"${'\\'.repeat(3)}uD83D\\uDE00"`);
    encodedParity.push(`"regex":${JSON.stringify('[^\\ud800-\\udfff]')}`);

    const baseSnapshot = JSON.stringify({
      ...createEmptyWorkflowSnapshot('run'),
      status: 'failed',
      context: { parity: '__PARITY__' },
    }).replace('"__PARITY__"', `{${encodedParity.join(',')}}`);
    const escapedNullPoisonKey = JSON.stringify('\\u0000status');
    const snapshot = baseSnapshot.replace(/}$/, `,${escapedNullPoisonKey}:"running","\\uD83D\\uDE00status":"running"}`);
    const now = new Date();
    await pool.query(
      `INSERT INTO "${schema}"."${SNAPSHOT_TABLE}"
       (workflow_name, run_id, snapshot, "createdAt", "updatedAt", "createdAtZ", "updatedAtZ")
       VALUES ('workflow', 'run', $1, $2, $3, $4, $5)`,
      [snapshot, now, now, now, now],
    );

    const workflows = new WorkflowsPG({ pool, schemaName: schema, skipDefaultIndexes: true });
    await workflows.init();
    const migratedRevision = await pool.query(
      `SELECT terminal_status FROM "${schema}"."${REVISION_TABLE}"
       WHERE workflow_name = 'workflow' AND run_id = 'run'`,
    );
    expect(migratedRevision.rows).toEqual([{ terminal_status: 'failed' }]);
    await expect(workflows.getWorkflowRunTerminalStatus({ workflowName: 'workflow', runId: 'run' })).resolves.toEqual({
      status: 'terminal',
      terminalStatus: 'failed',
    });
    const first = await workflows.listWorkflowRuns({ workflowName: 'workflow', status: 'failed' });
    expect(first.runs).toHaveLength(1);
    const parity = (first.runs[0]!.snapshot.context as { parity: Record<string, string> }).parity;
    expect(parity).toMatchObject(expectedRawParity);
    expect(parity.validPair).toBe('😀');
    expect(parity.escapedPair).toBe('\\😀');
    expect(parity.regex).toBe('[^\\ud800-\\udfff]');

    await workflows.persistWorkflowSnapshot({
      workflowName: 'workflow',
      runId: 'run',
      snapshot: first.runs[0]!.snapshot,
    });
    const second = await workflows.listWorkflowRuns({ workflowName: 'workflow', status: 'failed' });
    const persistedParity = (second.runs[0]!.snapshot.context as { parity: Record<string, string> }).parity;
    expect(persistedParity).toMatchObject(expectedSanitizedParity);
    expect(persistedParity.validPair).toBe('😀');
    expect(persistedParity.escapedPair).toBe('\\😀');
    expect(persistedParity.regex).toBe('[^\\ud800-\\udfff]');
    const revision = await pool.query(
      `SELECT generation::text FROM "${schema}"."${REVISION_TABLE}"
       WHERE workflow_name = 'workflow' AND run_id = 'run'`,
    );
    expect(revision.rows).toEqual([{ generation: '2' }]);
  });

  it('uses an externally migrated text snapshot schema without running domain init', async () => {
    const schema = await createSchema('revision_text_external');
    await createEvidenceTables(schema);
    await pool.query(
      `ALTER TABLE "${schema}"."mastra_workflow_snapshot"
       ALTER COLUMN snapshot TYPE text USING snapshot::text`,
    );
    await insertSnapshot(schema, 'workflow', 'run', 'failed');
    await new WorkflowsPG({ pool, schemaName: schema, skipDefaultIndexes: true }).init();

    const runtimePool = new Pool({ ...TEST_CONFIG, max: 1 });
    try {
      const writeFirstRuntime = new WorkflowsPG({
        pool: runtimePool,
        schemaName: schema,
        skipDefaultIndexes: true,
      });
      await writeFirstRuntime.persistWorkflowSnapshot({
        workflowName: 'workflow',
        runId: 'run',
        snapshot: { ...createEmptyWorkflowSnapshot('run'), status: 'failed' },
      });
      await expect(
        writeFirstRuntime.getWorkflowRunTerminalStatus({ workflowName: 'workflow', runId: 'run' }),
      ).resolves.toEqual({
        status: 'terminal',
        terminalStatus: 'failed',
      });

      const readFirstRuntime = new WorkflowsPG({
        pool: runtimePool,
        schemaName: schema,
        skipDefaultIndexes: true,
      });
      await expect(
        readFirstRuntime.getWorkflowRunTerminalStatus({ workflowName: 'workflow', runId: 'run' }),
      ).resolves.toEqual({
        status: 'terminal',
        terminalStatus: 'failed',
      });
    } finally {
      await runtimePool.end();
    }

    const revision = await pool.query(
      `SELECT generation::text, terminal_status
       FROM "${schema}"."${REVISION_TABLE}" WHERE workflow_name = 'workflow' AND run_id = 'run'`,
    );
    expect(revision.rows).toEqual([{ generation: '2', terminal_status: 'failed' }]);
  });

  it('rejects an invalid pre-marker terminalization status without recording provenance', async () => {
    const schema = await createSchema('revision_invalid_journal_migration');
    await createEvidenceTables(schema);
    await createCurrentRevisionTable(schema);
    await insertSnapshot(schema, 'workflow', 'run', 'tripwire');
    await insertJournal(schema, 'workflow', 'run', 'failed');
    await pool.query(
      `UPDATE "${schema}"."mastra_workflow_terminalizations"
       SET terminal_status = 'tripwire' WHERE workflow_name = 'workflow' AND run_id = 'run'`,
    );
    await pool.query(
      `INSERT INTO "${schema}"."${REVISION_TABLE}"
       (workflow_name, run_id, generation, terminal_status, updated_at)
       VALUES ('workflow', 'run', 1, 'tripwire', 1)`,
    );

    await expect(new WorkflowsPG({ pool, schemaName: schema, skipDefaultIndexes: true }).init()).rejects.toThrow(
      'Workflow parent revision migration found an invalid terminalization status',
    );
    expect(await markerCount(schema)).toBe('0');
    expect(await relationExists(schema, EPOCH_TABLE)).toBe(false);
  });

  it('rejects an invalid terminalization status after migration', async () => {
    const schema = await createSchema('revision_invalid_journal_runtime');
    const workflows = new WorkflowsPG({ pool, schemaName: schema, skipDefaultIndexes: true });
    await workflows.init();
    await workflows.persistWorkflowSnapshot({
      workflowName: 'workflow',
      runId: 'run',
      snapshot: { ...createEmptyWorkflowSnapshot('run'), status: 'tripwire' },
    });
    await insertJournal(schema, 'workflow', 'run', 'failed');
    await pool.query(
      `UPDATE "${schema}"."mastra_workflow_terminalizations"
       SET terminal_status = 'tripwire' WHERE workflow_name = 'workflow' AND run_id = 'run'`,
    );

    await expect(workflows.getWorkflowRunTerminalStatus({ workflowName: 'workflow', runId: 'run' })).rejects.toThrow(
      'Invalid workflow terminalization status',
    );
  });

  it.each([
    ['an unterminated object', '{"status":'],
    ['valid JSON followed by trailing text', '{"status":"failed"} trailing'],
  ] as const)('rejects %s in legacy text snapshots and rolls the migration back', async (_, malformedSnapshot) => {
    const schema = await createSchema('rti');
    await createEvidenceTables(schema);
    await pool.query(
      `ALTER TABLE "${schema}"."mastra_workflow_snapshot"
       ALTER COLUMN snapshot TYPE text USING snapshot::text`,
    );
    const now = new Date();
    await pool.query(
      `INSERT INTO "${schema}"."mastra_workflow_snapshot"
       (workflow_name, run_id, snapshot, "createdAt", "updatedAt", "createdAtZ", "updatedAtZ")
       VALUES ('workflow', 'run', $1, $2, $3, $4, $5)`,
      [malformedSnapshot, now, now, now, now],
    );

    await expect(new WorkflowsPG({ pool, schemaName: schema, skipDefaultIndexes: true }).init()).rejects.toThrow(
      'Workflow parent revision migration found invalid legacy snapshot JSON',
    );
    expect(await relationExists(schema, REVISION_TABLE)).toBe(false);
    expect(await relationExists(schema, EPOCH_TABLE)).toBe(false);
    expect(await relationExists(schema, MARKER_TABLE)).toBe(false);
  });

  it('rejects populated pre-revision export and preserves provenance for privileged init', async () => {
    const schema = await createSchema('rep');
    await createEvidenceTables(schema);
    await insertSnapshot(schema, 'workflow', 'run', 'running');

    await expect(pool.query(exportSchemas(schema))).rejects.toThrow(MIGRATION_REQUIRED);
    expect(await relationExists(schema, REVISION_TABLE)).toBe(false);
    expect(await relationExists(schema, MARKER_TABLE)).toBe(false);

    await new WorkflowsPG({ pool, schemaName: schema, skipDefaultIndexes: true }).init();
    const revision = await pool.query(
      `SELECT generation::text, terminal_status
       FROM "${schema}"."${REVISION_TABLE}" WHERE workflow_name = 'workflow' AND run_id = 'run'`,
    );
    expect(revision.rows).toEqual([{ generation: '1', terminal_status: null }]);
    expect(await markerCount(schema)).toBe('1');
  });

  it('rejects supported legacy export unchanged until privileged init performs the column migration', async () => {
    const schema = await createSchema('rel');
    await createEvidenceTables(schema);
    await createLegacyRevisionTable(schema);
    await insertSnapshot(schema, 'workflow', 'run', 'failed');
    await pool.query(
      `INSERT INTO "${schema}"."${REVISION_TABLE}" (workflow_name, run_id, generation, updated_at)
       VALUES ('workflow', 'run', 17, 170)`,
    );

    await expect(pool.query(exportSchemas(schema))).rejects.toThrow(MIGRATION_REQUIRED);
    const unchanged = await pool.query(
      `SELECT generation::text, updated_at::text FROM "${schema}"."${REVISION_TABLE}"`,
    );
    expect(unchanged.rows).toEqual([{ generation: '17', updated_at: '170' }]);
    const terminalColumn = await pool.query(
      `SELECT 1 FROM information_schema.columns
       WHERE table_schema = $1 AND table_name = $2 AND column_name = 'terminal_status'`,
      [schema, REVISION_TABLE],
    );
    expect(terminalColumn.rowCount).toBe(0);

    await new WorkflowsPG({ pool, schemaName: schema, skipDefaultIndexes: true }).init();
    const migrated = await pool.query(
      `SELECT generation::text, terminal_status, updated_at::text FROM "${schema}"."${REVISION_TABLE}"`,
    );
    expect(migrated.rows).toEqual([{ generation: '17', terminal_status: 'failed', updated_at: '170' }]);

    const emptySchema = await createSchema('re');
    await createEvidenceTables(emptySchema);
    await createLegacyRevisionTable(emptySchema);
    await pool.query(tableDDL(emptySchema, MARKER_TABLE));
    await expect(pool.query(exportSchemas(emptySchema))).rejects.toThrow(MIGRATION_REQUIRED);
    const emptyTerminalColumn = await pool.query(
      `SELECT 1 FROM information_schema.columns
       WHERE table_schema = $1 AND table_name = $2 AND column_name = 'terminal_status'`,
      [emptySchema, REVISION_TABLE],
    );
    expect(emptyTerminalColumn.rowCount).toBe(0);

    const markedSchema = await createSchema('rml');
    await createEvidenceTables(markedSchema);
    await createLegacyRevisionTable(markedSchema);
    await pool.query(tableDDL(markedSchema, MARKER_TABLE));
    await pool.query(`INSERT INTO "${markedSchema}"."${MARKER_TABLE}" (migration_key, applied_at) VALUES ($1, 1)`, [
      MIGRATION_KEY,
    ]);
    await expect(pool.query(exportSchemas(markedSchema))).rejects.toThrow(
      'Workflow parent revision migration provenance is damaged or incomplete',
    );
  }, 30000);

  it('rejects incompatible export-owned relations without mutating revision data', async () => {
    const malformedMarkerSchema = await createSchema('rbm');
    await createEvidenceTables(malformedMarkerSchema);
    await createCurrentRevisionTable(malformedMarkerSchema);
    await pool.query(`DROP TABLE "${malformedMarkerSchema}"."${MARKER_TABLE}"`);
    await pool.query(
      `CREATE TABLE "${malformedMarkerSchema}"."${MARKER_TABLE}" (
         migration_key TEXT PRIMARY KEY,
         applied_at INTEGER NOT NULL
       )`,
    );
    const before = await pool.query(
      `SELECT count(*)::text AS count FROM "${malformedMarkerSchema}"."${REVISION_TABLE}"`,
    );
    await expect(pool.query(exportSchemas(malformedMarkerSchema))).rejects.toThrow(
      'Workflow parent revision migration marker table has an incompatible shape',
    );
    const after = await pool.query(
      `SELECT count(*)::text AS count FROM "${malformedMarkerSchema}"."${REVISION_TABLE}"`,
    );
    expect(after.rows).toEqual(before.rows);

    const malformedRevisionSchema = await createSchema('rbr');
    await createEvidenceTables(malformedRevisionSchema);
    await pool.query(tableDDL(malformedRevisionSchema, MARKER_TABLE));
    await pool.query(
      `CREATE TABLE "${malformedRevisionSchema}"."${REVISION_TABLE}" (
         workflow_name TEXT NOT NULL,
         run_id TEXT NOT NULL,
         generation INTEGER NOT NULL,
         PRIMARY KEY (workflow_name, run_id)
       )`,
    );
    await expect(pool.query(exportSchemas(malformedRevisionSchema))).rejects.toThrow(
      'Workflow parent revision table has an incompatible shape',
    );
  });

  it('rejects revision defaults that change provisional parent semantics', async () => {
    const schema = await createSchema('rd');
    await createEvidenceTables(schema);
    await createCurrentRevisionTable(schema);
    await pool.query(
      `ALTER TABLE "${schema}"."${REVISION_TABLE}"
       ALTER COLUMN terminal_status SET DEFAULT 'failed'`,
    );

    await expect(pool.query(exportSchemas(schema))).rejects.toThrow(
      'Workflow parent revision table has an incompatible shape',
    );
    const client = new RoutingDbClient(new PoolAdapter(pool));
    const snapshot = await loadSchemaSnapshot(client, schema);
    expect(snapshot.columnsWithDefaults.get(REVISION_TABLE)).toEqual(new Set(['terminal_status']));
    client.setSchemaSnapshot(snapshot);
    try {
      await expect(new WorkflowsPG({ client, schemaName: schema, skipDefaultIndexes: true }).init()).rejects.toThrow(
        'Workflow parent revision table has an incompatible shape',
      );
    } finally {
      client.setSchemaSnapshot(null);
    }
    expect(await markerCount(schema)).toBe('0');
    expect(await relationExists(schema, EPOCH_TABLE)).toBe(false);
  });

  it('rejects unlogged migration provenance and revision relations', async () => {
    const cases = [
      { table: MARKER_TABLE, error: 'Workflow parent revision migration marker table has an incompatible shape' },
      { table: REVISION_TABLE, error: 'Workflow parent revision table has an incompatible shape' },
      { table: EPOCH_TABLE, error: 'Workflow parent revision migration epoch table has an incompatible shape' },
    ];

    for (const [index, testCase] of cases.entries()) {
      const schema = await createSchema(`ru${index}`);
      if (testCase.table === EPOCH_TABLE) {
        await new WorkflowsPG({ pool, schemaName: schema, skipDefaultIndexes: true }).init();
      } else {
        await createEvidenceTables(schema);
        await createCurrentRevisionTable(schema);
      }
      await pool.query(`ALTER TABLE "${schema}"."${testCase.table}" SET UNLOGGED`);

      await expect(pool.query(exportSchemas(schema))).rejects.toThrow(testCase.error);
      const client = new RoutingDbClient(new PoolAdapter(pool));
      const snapshot = await loadSchemaSnapshot(client, schema);
      expect(snapshot.tablePersistence.get(testCase.table)).toBe('u');
      client.setSchemaSnapshot(snapshot);
      try {
        await expect(new WorkflowsPG({ client, schemaName: schema, skipDefaultIndexes: true }).init()).rejects.toThrow(
          testCase.error,
        );
      } finally {
        client.setSchemaSnapshot(null);
      }
    }
  }, 30000);

  it.each([
    ['marker root', MARKER_TABLE, 'root', 'Workflow parent revision migration marker table has an incompatible shape'],
    [
      'marker child',
      MARKER_TABLE,
      'child',
      'Workflow parent revision migration marker table has an incompatible shape',
    ],
    ['epoch root', EPOCH_TABLE, 'root', 'Workflow parent revision migration epoch table has an incompatible shape'],
    ['epoch child', EPOCH_TABLE, 'child', 'Workflow parent revision migration epoch table has an incompatible shape'],
    ['revision root', REVISION_TABLE, 'root', 'Workflow parent revision table has an incompatible shape'],
    ['revision child', REVISION_TABLE, 'child', 'Workflow parent revision table has an incompatible shape'],
  ] as const)(
    'rejects a private %s in export, cached-catalog, and live-catalog paths',
    async (_, table, placement, error) => {
      const schema = await createSchema(
        `rp${table === MARKER_TABLE ? 'm' : table === EPOCH_TABLE ? 'e' : 'r'}${placement[0]}`,
      );
      await new WorkflowsPG({ pool, schemaName: schema, skipDefaultIndexes: true }).init();
      await pool.query(`DROP TABLE "${schema}"."${table}"`);
      await createPartitionedPrivateRelation(schema, table, placement);

      const snapshot = await expectPrivateRelationRejected(
        schema,
        error,
        table === REVISION_TABLE
          ? 'Workflow parent revision migration marker conflicts with the durable schema'
          : error,
      );
      expect(snapshot.tableKinds.get(table)).toBe(placement === 'root' ? 'p' : 'r');
      expect(snapshot.partitionedTables.has(table)).toBe(placement === 'child');
    },
  );

  it.each([
    [
      'marker parent',
      MARKER_TABLE,
      'parent',
      'Workflow parent revision migration marker table has an incompatible shape',
    ],
    [
      'marker child',
      MARKER_TABLE,
      'child',
      'Workflow parent revision migration marker table has an incompatible shape',
    ],
    ['epoch parent', EPOCH_TABLE, 'parent', 'Workflow parent revision migration epoch table has an incompatible shape'],
    ['epoch child', EPOCH_TABLE, 'child', 'Workflow parent revision migration epoch table has an incompatible shape'],
    ['revision parent', REVISION_TABLE, 'parent', 'Workflow parent revision table has an incompatible shape'],
    ['revision child', REVISION_TABLE, 'child', 'Workflow parent revision table has an incompatible shape'],
  ] as const)(
    'rejects a private inheritance %s in export, cached-catalog, and live-catalog paths',
    async (_, table, placement, error) => {
      const schema = await createSchema(
        `rh${table === MARKER_TABLE ? 'm' : table === EPOCH_TABLE ? 'e' : 'r'}${placement[0]}`,
      );
      await new WorkflowsPG({ pool, schemaName: schema, skipDefaultIndexes: true }).init();
      if (placement === 'child') {
        await pool.query(`DROP TABLE "${schema}"."${table}"`);
      }
      await createInheritedPrivateRelation(schema, table, placement);

      const snapshot = await expectPrivateRelationRejected(
        schema,
        error,
        table === REVISION_TABLE
          ? 'Workflow parent revision migration marker conflicts with the durable schema'
          : error,
      );
      expect(snapshot.tableKinds.get(table)).toBe('r');
      expect(snapshot.partitionedTables.has(table)).toBe(false);
      expect(snapshot.inheritedTables.has(table)).toBe(true);
    },
  );

  it.each([
    [MARKER_TABLE, 'Workflow parent revision migration marker table has an incompatible shape'],
    [EPOCH_TABLE, 'Workflow parent revision migration epoch table has an incompatible shape'],
    [REVISION_TABLE, 'Workflow parent revision table has an incompatible shape'],
  ] as const)(
    'rejects a deferrable PRIMARY KEY on %s in every catalog path without changing durable rows',
    async (table, exportError) => {
      const schema = await createSchema(`rdp${table === MARKER_TABLE ? 'm' : table === EPOCH_TABLE ? 'e' : 'r'}`);
      const workflows = new WorkflowsPG({ pool, schemaName: schema, skipDefaultIndexes: true });
      await workflows.init();
      await workflows.persistWorkflowSnapshot({
        workflowName: 'workflow',
        runId: 'run',
        snapshot: { ...createEmptyWorkflowSnapshot('run'), status: 'failed' },
      });
      await replacePrivatePrimaryKeyWithDeferrable(schema, table);
      const before = await pool.query(
        `SELECT
           (SELECT jsonb_agg(to_jsonb(marker_row) ORDER BY marker_row.migration_key)
            FROM "${schema}"."${MARKER_TABLE}" AS marker_row) AS marker_rows,
           (SELECT jsonb_agg(to_jsonb(epoch_row) ORDER BY epoch_row.epoch)
            FROM "${schema}"."${EPOCH_TABLE}" AS epoch_row) AS epoch_rows,
           (SELECT jsonb_agg(to_jsonb(revision_row) ORDER BY revision_row.workflow_name, revision_row.run_id)
            FROM "${schema}"."${REVISION_TABLE}" AS revision_row) AS revision_rows`,
      );

      const snapshot = await expectPrivateRelationRejected(
        schema,
        exportError,
        table === REVISION_TABLE
          ? 'Workflow parent revision migration marker conflicts with the durable schema'
          : exportError,
      );
      expect(snapshot.primaryKeyColumns.get(table)).toEqual(
        table === MARKER_TABLE ? ['migration_key'] : table === EPOCH_TABLE ? ['epoch'] : ['workflow_name', 'run_id'],
      );
      expect(snapshot.immediatePrimaryKeyTables.has(table)).toBe(false);
      const after = await pool.query(
        `SELECT
           (SELECT jsonb_agg(to_jsonb(marker_row) ORDER BY marker_row.migration_key)
            FROM "${schema}"."${MARKER_TABLE}" AS marker_row) AS marker_rows,
           (SELECT jsonb_agg(to_jsonb(epoch_row) ORDER BY epoch_row.epoch)
            FROM "${schema}"."${EPOCH_TABLE}" AS epoch_row) AS epoch_rows,
           (SELECT jsonb_agg(to_jsonb(revision_row) ORDER BY revision_row.workflow_name, revision_row.run_id)
            FROM "${schema}"."${REVISION_TABLE}" AS revision_row) AS revision_rows`,
      );
      expect(after.rows).toEqual(before.rows);
    },
  );

  it('rejects a deferrable legacy revision PRIMARY KEY without starting its migration', async () => {
    const schema = await createSchema('r');
    await createEvidenceTables(schema);
    await createLegacyRevisionTable(schema);
    await insertSnapshot(schema, 'workflow', 'run', 'failed');
    await pool.query(
      `INSERT INTO "${schema}"."${REVISION_TABLE}" (workflow_name, run_id, generation, updated_at)
       VALUES ('workflow', 'run', 7, 70)`,
    );
    await replacePrivatePrimaryKeyWithDeferrable(schema, REVISION_TABLE);

    const snapshot = await expectPrivateRelationRejected(
      schema,
      'Workflow parent revision table has an incompatible shape',
    );
    expect(snapshot.primaryKeyColumns.get(REVISION_TABLE)).toEqual(['workflow_name', 'run_id']);
    expect(snapshot.immediatePrimaryKeyTables.has(REVISION_TABLE)).toBe(false);
    const revision = await pool.query(`SELECT generation::text, updated_at::text FROM "${schema}"."${REVISION_TABLE}"`);
    expect(revision.rows).toEqual([{ generation: '7', updated_at: '70' }]);
    const terminalColumn = await pool.query(
      `SELECT 1 FROM information_schema.columns
       WHERE table_schema = $1 AND table_name = $2 AND column_name = 'terminal_status'`,
      [schema, REVISION_TABLE],
    );
    expect(terminalColumn.rowCount).toBe(0);
    expect(await relationExists(schema, MARKER_TABLE)).toBe(false);
    expect(await relationExists(schema, EPOCH_TABLE)).toBe(false);
  });

  it.each([
    [MARKER_TABLE, 'applied_at', 'Workflow parent revision migration marker table has an incompatible shape'],
    [EPOCH_TABLE, 'created_at', 'Workflow parent revision migration epoch table has an incompatible shape'],
    [REVISION_TABLE, 'generation', 'Workflow parent revision table has an incompatible shape'],
  ] as const)(
    'treats an identity on %s.%s as a forbidden default in every catalog path',
    async (table, column, error) => {
      const schema = await createSchema(`ri${table === MARKER_TABLE ? 'm' : table === EPOCH_TABLE ? 'e' : 'r'}`);
      await new WorkflowsPG({ pool, schemaName: schema, skipDefaultIndexes: true }).init();
      await pool.query(
        `ALTER TABLE "${schema}"."${table}"
         ALTER COLUMN "${column}" ADD GENERATED BY DEFAULT AS IDENTITY`,
      );
      const catalog = await pool.query<{ atthasdef: boolean; attidentity: string }>(
        `SELECT column_row.atthasdef, column_row.attidentity
         FROM pg_catalog.pg_attribute AS column_row
         WHERE column_row.attrelid = $1::regclass
           AND column_row.attname = $2`,
        [`"${schema}"."${table}"`, column],
      );
      expect(catalog.rows).toEqual([{ atthasdef: false, attidentity: 'd' }]);

      const snapshot = await expectPrivateRelationRejected(
        schema,
        error,
        table === REVISION_TABLE
          ? 'Workflow parent revision migration marker conflicts with the durable schema'
          : error,
      );
      expect(snapshot.columnsWithDefaults.get(table)).toContain(column);
    },
  );

  it.each([
    [MARKER_TABLE, 'applied_at >= 0', 'Workflow parent revision migration marker table has an incompatible shape'],
    [REVISION_TABLE, 'generation >= 0', 'Workflow parent revision table has an incompatible shape'],
  ] as const)(
    'rejects an unvalidated CHECK on %s in every catalog path without changing durable rows',
    async (table, expression, exportError) => {
      const schema = await createSchema(`rc${table === MARKER_TABLE ? 'm' : 'r'}`);
      const workflows = new WorkflowsPG({ pool, schemaName: schema, skipDefaultIndexes: true });
      await workflows.init();
      await workflows.persistWorkflowSnapshot({
        workflowName: 'workflow',
        runId: 'run',
        snapshot: { ...createEmptyWorkflowSnapshot('run'), status: 'failed' },
      });
      const checkName = await relationCheckName(schema, table, expression);
      await pool.query(
        `ALTER TABLE "${schema}"."${table}"
         DROP CONSTRAINT "${checkName}",
         ADD CONSTRAINT "${table}_test_unvalidated" CHECK (${expression}) NOT VALID`,
      );
      const before = await pool.query(
        `SELECT
           (SELECT jsonb_agg(to_jsonb(marker_row) ORDER BY marker_row.migration_key)
            FROM "${schema}"."${MARKER_TABLE}" AS marker_row) AS marker_rows,
           (SELECT jsonb_agg(to_jsonb(epoch_row) ORDER BY epoch_row.epoch)
            FROM "${schema}"."${EPOCH_TABLE}" AS epoch_row) AS epoch_rows,
           (SELECT jsonb_agg(to_jsonb(revision_row) ORDER BY revision_row.workflow_name, revision_row.run_id)
            FROM "${schema}"."${REVISION_TABLE}" AS revision_row) AS revision_rows`,
      );

      const snapshot = await expectPrivateRelationRejected(
        schema,
        exportError,
        table === REVISION_TABLE
          ? 'Workflow parent revision migration marker conflicts with the durable schema'
          : exportError,
      );
      expect(snapshot.checkConstraints.get(table)).toContainEqual({ expression, validated: false });
      const after = await pool.query(
        `SELECT
           (SELECT jsonb_agg(to_jsonb(marker_row) ORDER BY marker_row.migration_key)
            FROM "${schema}"."${MARKER_TABLE}" AS marker_row) AS marker_rows,
           (SELECT jsonb_agg(to_jsonb(epoch_row) ORDER BY epoch_row.epoch)
            FROM "${schema}"."${EPOCH_TABLE}" AS epoch_row) AS epoch_rows,
           (SELECT jsonb_agg(to_jsonb(revision_row) ORDER BY revision_row.workflow_name, revision_row.run_id)
            FROM "${schema}"."${REVISION_TABLE}" AS revision_row) AS revision_rows`,
      );
      expect(after.rows).toEqual(before.rows);
    },
  );

  it('rejects malformed epoch columns, checks, and primary keys in every catalog path', async () => {
    const cases = [
      {
        label: 'extra-column',
        mutate: (schema: string) => pool.query(`ALTER TABLE "${schema}"."${EPOCH_TABLE}" ADD COLUMN extra BIGINT`),
      },
      {
        label: 'missing-check',
        mutate: async (schema: string) => {
          const name = await relationCheckName(schema, EPOCH_TABLE, 'created_at >= 0');
          await pool.query(`ALTER TABLE "${schema}"."${EPOCH_TABLE}" DROP CONSTRAINT "${name}"`);
        },
      },
      {
        label: 'wrong-primary-key',
        mutate: async (schema: string) => {
          const primaryKey = await pool.query<{ conname: string }>(
            `SELECT constraint_row.conname
             FROM pg_catalog.pg_constraint AS constraint_row
             WHERE constraint_row.conrelid = $1::regclass
               AND constraint_row.contype = 'p'`,
            [`"${schema}"."${EPOCH_TABLE}"`],
          );
          await pool.query(
            `ALTER TABLE "${schema}"."${EPOCH_TABLE}"
             DROP CONSTRAINT "${primaryKey.rows[0]!.conname}",
             ADD PRIMARY KEY (created_at)`,
          );
        },
      },
    ];

    for (const [index, testCase] of cases.entries()) {
      const schema = await createSchema(`res${index}`);
      await new WorkflowsPG({ pool, schemaName: schema, skipDefaultIndexes: true }).init();
      await testCase.mutate(schema);
      await expectPrivateRelationRejected(
        schema,
        'Workflow parent revision migration epoch table has an incompatible shape',
      );
    }
  }, 30000);

  it('enforces the marker and epoch export provenance matrix without manufacturing evidence', async () => {
    const cases = [
      { label: 'marker-only', marker: true, epoch: 'absent', succeeds: false },
      { label: 'epoch-only-empty', marker: false, epoch: 'empty', succeeds: false },
      { label: 'epoch-only-populated', marker: false, epoch: 'populated', succeeds: false },
      { label: 'marker-empty-epoch', marker: true, epoch: 'empty', succeeds: false },
      { label: 'complete', marker: true, epoch: 'populated', succeeds: true },
    ] as const;

    for (const [index, testCase] of cases.entries()) {
      const schema = await createSchema(`rpm${index}`);
      await createEvidenceTables(schema);
      await createCurrentRevisionTable(schema);
      if (testCase.marker) {
        await pool.query(`INSERT INTO "${schema}"."${MARKER_TABLE}" (migration_key, applied_at) VALUES ($1, 1)`, [
          MIGRATION_KEY,
        ]);
      }
      if (testCase.epoch !== 'absent') {
        await createMigrationEpoch(schema, testCase.epoch === 'populated');
      }

      const exported = pool.query(exportSchemas(schema));
      if (testCase.succeeds) {
        await expect(exported).resolves.toBeDefined();
      } else {
        await expect(exported).rejects.toThrow(
          'Workflow parent revision migration provenance is damaged or incomplete',
        );
      }
      expect(await markerCount(schema)).toBe(testCase.marker ? '1' : '0');
      expect(await relationExists(schema, EPOCH_TABLE)).toBe(testCase.epoch !== 'absent');
    }
  }, 30000);

  it('adds only terminal status to the intermediate schema while preserving generations', async () => {
    const schema = await createSchema('revision_intermediate');
    await createEvidenceTables(schema);
    await createLegacyRevisionTable(schema);
    await insertSnapshot(schema, 'running-workflow', 'running-run', 'running');
    await insertSnapshot(schema, 'terminal-workflow', 'terminal-run', 'failed');
    await pool.query(
      `INSERT INTO "${schema}"."${REVISION_TABLE}" (workflow_name, run_id, generation, updated_at)
       VALUES ('running-workflow', 'running-run', 7, 70), ('terminal-workflow', 'terminal-run', 11, 110)`,
    );

    await new WorkflowsPG({ pool, schemaName: schema, skipDefaultIndexes: true }).init();
    const revisions = await pool.query(
      `SELECT workflow_name, generation::text, terminal_status, updated_at::text
       FROM "${schema}"."${REVISION_TABLE}" ORDER BY workflow_name`,
    );
    expect(revisions.rows).toEqual([
      { workflow_name: 'running-workflow', generation: '7', terminal_status: null, updated_at: '70' },
      { workflow_name: 'terminal-workflow', generation: '11', terminal_status: 'failed', updated_at: '110' },
    ]);
  });

  it('adopts a current unmarked schema without mutating revision rows', async () => {
    const schema = await createSchema('revision_adopt');
    await createEvidenceTables(schema);
    await createCurrentRevisionTable(schema);
    await insertSnapshot(schema, 'workflow', 'run', 'running');
    await pool.query(
      `INSERT INTO "${schema}"."${REVISION_TABLE}"
       (workflow_name, run_id, generation, terminal_status, updated_at)
       VALUES ('workflow', 'run', 9, NULL, 123)`,
    );
    const before = await pool.query(
      `SELECT generation::text, terminal_status, updated_at::text, xmin::text AS row_version
       FROM "${schema}"."${REVISION_TABLE}"`,
    );

    await new WorkflowsPG({ pool, schemaName: schema, skipDefaultIndexes: true }).init();
    const after = await pool.query(
      `SELECT generation::text, terminal_status, updated_at::text, xmin::text AS row_version
       FROM "${schema}"."${REVISION_TABLE}"`,
    );
    expect(after.rows).toEqual(before.rows);
    expect(await markerCount(schema)).toBe('1');
  });

  it('rejects an invalid snapshot status without manufacturing migration provenance', async () => {
    const schema = await createSchema('revision_invalid_snapshot_status');
    await createEvidenceTables(schema);
    await createCurrentRevisionTable(schema);
    await insertSnapshot(schema, 'workflow', 'run', 'running');
    await pool.query(
      `UPDATE "${schema}"."${SNAPSHOT_TABLE}"
       SET snapshot = jsonb_set(snapshot, '{status}', '"unknown"'::jsonb)
       WHERE workflow_name = 'workflow' AND run_id = 'run'`,
    );
    await pool.query(
      `INSERT INTO "${schema}"."${REVISION_TABLE}"
       (workflow_name, run_id, generation, terminal_status, updated_at)
       VALUES ('workflow', 'run', 1, NULL, 1)`,
    );
    const before = await pool.query(
      `SELECT workflow_name, run_id, generation::text, terminal_status, updated_at::text, xmin::text AS row_version
       FROM "${schema}"."${REVISION_TABLE}"`,
    );

    await expect(new WorkflowsPG({ pool, schemaName: schema, skipDefaultIndexes: true }).init()).rejects.toThrow(
      'Workflow parent revision migration found an invalid snapshot status',
    );

    const after = await pool.query(
      `SELECT workflow_name, run_id, generation::text, terminal_status, updated_at::text, xmin::text AS row_version
       FROM "${schema}"."${REVISION_TABLE}"`,
    );
    expect(after.rows).toEqual(before.rows);
    expect(await markerCount(schema)).toBe('0');
    expect(await relationExists(schema, EPOCH_TABLE)).toBe(false);
  });

  it('keeps current-unmarked missing identity corruption unmarked across export and runtime calls', async () => {
    const schema = await createSchema('rcm');
    await createEvidenceTables(schema);
    await createCurrentRevisionTable(schema);
    await insertSnapshot(schema, 'workflow', 'run', 'running');

    await expect(pool.query(exportSchemas(schema))).resolves.toBeDefined();
    expect(await markerCount(schema)).toBe('0');
    const workflows = new WorkflowsPG({ pool, schemaName: schema, skipDefaultIndexes: true });
    await expect(workflows.getWorkflowRunTerminalStatus({ workflowName: 'workflow', runId: 'run' })).rejects.toThrow(
      'Workflow run is missing parent revision evidence',
    );
    await expect(
      workflows.persistWorkflowSnapshot({
        workflowName: 'workflow',
        runId: 'run',
        snapshot: { ...createEmptyWorkflowSnapshot('run'), status: 'running' },
      }),
    ).rejects.toThrow();
    await expect(workflows.init()).rejects.toThrow('durable identities without revision evidence');
    expect(await markerCount(schema)).toBe('0');
    const revision = await pool.query(`SELECT 1 FROM "${schema}"."${REVISION_TABLE}"`);
    expect(revision.rowCount).toBe(0);
  });

  it('verifies the complete terminal evidence matrix before adopting current schema', async () => {
    const schema = await createSchema('revision_terminal_matrix');
    await createEvidenceTables(schema);
    await createCurrentRevisionTable(schema);
    await insertSnapshot(schema, 'journal-workflow', 'journal-run', 'running');
    await insertJournal(schema, 'journal-workflow', 'journal-run', 'failed');
    await insertSnapshot(schema, 'snapshot-workflow', 'snapshot-run', 'success');
    await pool.query(
      `INSERT INTO "${schema}"."${REVISION_TABLE}"
       (workflow_name, run_id, generation, terminal_status, updated_at)
       VALUES
         ('journal-workflow', 'journal-run', 2, 'failed', 2),
         ('snapshot-workflow', 'snapshot-run', 3, 'success', 3),
         ('tombstone-workflow', 'tombstone-run', 4, 'failed', 4)`,
    );

    const workflows = new WorkflowsPG({ pool, schemaName: schema, skipDefaultIndexes: true });
    await workflows.init();
    await expect(
      workflows.getWorkflowRunTerminalStatus({ workflowName: 'journal-workflow', runId: 'journal-run' }),
    ).resolves.toEqual({ status: 'terminal', terminalStatus: 'failed' });
    await expect(
      workflows.getWorkflowRunTerminalStatus({ workflowName: 'snapshot-workflow', runId: 'snapshot-run' }),
    ).resolves.toEqual({ status: 'terminal', terminalStatus: 'success' });
    await expect(
      workflows.getWorkflowRunTerminalStatus({ workflowName: 'tombstone-workflow', runId: 'tombstone-run' }),
    ).resolves.toEqual({ status: 'terminal', terminalStatus: 'failed' });
  });

  it('rejects a terminal revision paired only with a nonterminal snapshot', async () => {
    const schema = await createSchema('revision_terminal_running');
    await createEvidenceTables(schema);
    await createCurrentRevisionTable(schema);
    await insertSnapshot(schema, 'workflow', 'run', 'running');
    await pool.query(
      `INSERT INTO "${schema}"."${REVISION_TABLE}"
       (workflow_name, run_id, generation, terminal_status, updated_at)
       VALUES ('workflow', 'run', 5, 'failed', 5)`,
    );

    const workflows = new WorkflowsPG({ pool, schemaName: schema, skipDefaultIndexes: true });
    await expect(workflows.init()).rejects.toThrow('mismatched terminal status evidence');
    expect(await markerCount(schema)).toBe('0');
    await expect(workflows.getWorkflowRunTerminalStatus({ workflowName: 'workflow', runId: 'run' })).rejects.toThrow(
      'Workflow parent revision conflicts with nonterminal snapshot status',
    );
    await expect(
      workflows.persistWorkflowSnapshot({
        workflowName: 'workflow',
        runId: 'run',
        snapshot: { ...createEmptyWorkflowSnapshot('run'), status: 'running' },
      }),
    ).rejects.toThrow('Workflow parent revision conflict');
    const retained = await pool.query(
      `SELECT snapshot->>'status' AS status, generation::text
       FROM "${schema}"."mastra_workflow_snapshot" AS snapshot
       JOIN "${schema}"."${REVISION_TABLE}" AS revision USING (workflow_name, run_id)`,
    );
    expect(retained.rows).toEqual([{ status: 'running', generation: '5' }]);
  });

  it('fails unmarked corruption without installing a marker', async () => {
    const cases = [
      {
        label: 'missing-identity',
        setup: async (schema: string) => {
          await createEvidenceTables(schema);
          await createCurrentRevisionTable(schema);
          await insertSnapshot(schema, 'workflow', 'run', 'running');
        },
      },
      {
        label: 'legacy-missing-identity',
        setup: async (schema: string) => {
          await createEvidenceTables(schema);
          await createLegacyRevisionTable(schema);
          await pool.query(tableDDL(schema, MARKER_TABLE));
          await insertSnapshot(schema, 'workflow', 'run', 'running');
        },
      },
      {
        label: 'generation-zero',
        setup: async (schema: string) => {
          await createEvidenceTables(schema);
          await createCurrentRevisionTable(schema);
          await pool.query(
            `INSERT INTO "${schema}"."${REVISION_TABLE}"
             (workflow_name, run_id, generation, terminal_status, updated_at)
             VALUES ('workflow', 'run', 0, NULL, 1)`,
          );
        },
      },
      {
        label: 'terminal-mismatch',
        setup: async (schema: string) => {
          await createEvidenceTables(schema);
          await createCurrentRevisionTable(schema);
          await insertSnapshot(schema, 'workflow', 'run', 'success');
          await pool.query(
            `INSERT INTO "${schema}"."${REVISION_TABLE}"
             (workflow_name, run_id, generation, terminal_status, updated_at)
             VALUES ('workflow', 'run', 3, 'failed', 1)`,
          );
        },
      },
      {
        label: 'incompatible-shape',
        setup: async (schema: string) => {
          await createEvidenceTables(schema);
          await pool.query(tableDDL(schema, MARKER_TABLE));
          await pool.query(
            `CREATE TABLE "${schema}"."${REVISION_TABLE}" (
               workflow_name TEXT NOT NULL,
               run_id TEXT NOT NULL,
               generation INTEGER NOT NULL,
               PRIMARY KEY (workflow_name, run_id)
             )`,
          );
        },
      },
    ];

    for (const testCase of cases) {
      const schema = await createSchema(`revision_corrupt_${testCase.label.replaceAll('-', '_')}`);
      await testCase.setup(schema);
      const workflows = new WorkflowsPG({ pool, schemaName: schema, skipDefaultIndexes: true });
      await expect(workflows.init()).rejects.toThrow();
      expect(await markerCount(schema)).toBe('0');
    }
  });

  it('rejects nullable columns, incomplete CHECK semantics, and invalid durable row values', async () => {
    const cases = [
      {
        label: 'nullable-null-values',
        setup: async (schema: string) => {
          await pool.query(
            `ALTER TABLE "${schema}"."${REVISION_TABLE}"
             ALTER COLUMN generation DROP NOT NULL,
             ALTER COLUMN updated_at DROP NOT NULL`,
          );
          await pool.query(
            `INSERT INTO "${schema}"."${REVISION_TABLE}"
             (workflow_name, run_id, generation, terminal_status, updated_at)
             VALUES ('workflow', 'run', NULL, NULL, NULL)`,
          );
        },
      },
      {
        label: 'missing-check',
        setup: async (schema: string) => {
          const name = await relationCheckName(schema, REVISION_TABLE, 'generation >= 0');
          await pool.query(`ALTER TABLE "${schema}"."${REVISION_TABLE}" DROP CONSTRAINT "${name}"`);
        },
      },
      {
        label: 'unvalidated-check',
        setup: async (schema: string) => {
          const name = await relationCheckName(schema, REVISION_TABLE, 'generation >= 0');
          await pool.query(
            `ALTER TABLE "${schema}"."${REVISION_TABLE}"
             DROP CONSTRAINT "${name}",
             ADD CONSTRAINT revision_generation_unvalidated CHECK (generation >= 0) NOT VALID`,
          );
        },
      },
      {
        label: 'wrong-check',
        setup: async (schema: string) => {
          const name = await relationCheckName(schema, REVISION_TABLE, 'generation >= 0');
          await pool.query(
            `ALTER TABLE "${schema}"."${REVISION_TABLE}"
             DROP CONSTRAINT "${name}",
             ADD CONSTRAINT revision_generation_wrong CHECK (generation >= 1)`,
          );
        },
      },
      {
        label: 'unsafe-generation',
        setup: async (schema: string) => {
          await pool.query(
            `INSERT INTO "${schema}"."${REVISION_TABLE}"
             (workflow_name, run_id, generation, terminal_status, updated_at)
             VALUES ('workflow', 'run', 9007199254740992, NULL, 1)`,
          );
        },
      },
    ];

    for (const testCase of cases) {
      const schema = await createSchema(`revision_shape_${testCase.label.replaceAll('-', '_')}`);
      await createEvidenceTables(schema);
      await createCurrentRevisionTable(schema);
      await testCase.setup(schema);
      await expect(new WorkflowsPG({ pool, schemaName: schema, skipDefaultIndexes: true }).init()).rejects.toThrow();
      expect(await markerCount(schema)).toBe('0');
    }
  });

  it('rejects a durable marker when the revision relation is missing or incompatible', async () => {
    for (const relation of ['missing', 'incompatible'] as const) {
      const schema = await createSchema(relation === 'missing' ? 'm' : 'i');
      await createEvidenceTables(schema);
      await pool.query(tableDDL(schema, MARKER_TABLE));
      await pool.query(`INSERT INTO "${schema}"."${MARKER_TABLE}" (migration_key, applied_at) VALUES ($1, 1)`, [
        MIGRATION_KEY,
      ]);
      await createMigrationEpoch(schema);
      if (relation === 'incompatible') {
        await pool.query(
          `CREATE TABLE "${schema}"."${REVISION_TABLE}" (
             workflow_name TEXT NOT NULL,
             run_id TEXT NOT NULL,
             generation INTEGER NOT NULL,
             PRIMARY KEY (workflow_name, run_id)
           )`,
        );
      }

      if (relation === 'missing') {
        await expect(pool.query(exportSchemas(schema))).rejects.toThrow(
          'Workflow parent revision migration marker conflicts with the durable schema',
        );
        expect(await relationExists(schema, REVISION_TABLE)).toBe(false);
      }

      const workflows = new WorkflowsPG({ pool, schemaName: schema, skipDefaultIndexes: true });
      await expect(workflows.init()).rejects.toThrow(
        'Workflow parent revision migration marker conflicts with the durable schema',
      );
      expect(await markerCount(schema)).toBe('1');
    }
  });

  it('rolls back an injected marker failure and converges on retry', async () => {
    const schema = await createSchema('revision_rollback');
    await createEvidenceTables(schema);
    await createLegacyRevisionTable(schema);
    await pool.query(tableDDL(schema, MARKER_TABLE));
    await insertSnapshot(schema, 'workflow', 'run', 'failed');
    await pool.query(
      `INSERT INTO "${schema}"."${REVISION_TABLE}" (workflow_name, run_id, generation, updated_at)
       VALUES ('workflow', 'run', 8, 80)`,
    );
    await pool.query(
      `CREATE FUNCTION "${schema}".fail_revision_marker() RETURNS trigger AS $$
       BEGIN
         IF NEW.migration_key = '${MIGRATION_KEY}' THEN
           RAISE EXCEPTION 'injected marker failure';
         END IF;
         RETURN NEW;
       END;
       $$ LANGUAGE plpgsql`,
    );
    await pool.query(
      `CREATE TRIGGER fail_revision_marker
       BEFORE INSERT ON "${schema}"."${MARKER_TABLE}"
       FOR EACH ROW EXECUTE FUNCTION "${schema}".fail_revision_marker()`,
    );

    const workflows = new WorkflowsPG({ pool, schemaName: schema, skipDefaultIndexes: true });
    await expect(workflows.init()).rejects.toThrow('injected marker failure');
    const rolledBackColumn = await pool.query(
      `SELECT 1 FROM information_schema.columns
       WHERE table_schema = $1 AND table_name = $2 AND column_name = 'terminal_status'`,
      [schema, REVISION_TABLE],
    );
    expect(rolledBackColumn.rowCount).toBe(0);
    expect(await markerCount(schema)).toBe('0');

    await pool.query(`DROP TRIGGER fail_revision_marker ON "${schema}"."${MARKER_TABLE}"`);
    await workflows.init();
    const revision = await pool.query(
      `SELECT generation::text, terminal_status, updated_at::text
       FROM "${schema}"."${REVISION_TABLE}"`,
    );
    expect(revision.rows).toEqual([{ generation: '8', terminal_status: 'failed', updated_at: '80' }]);
    expect(await markerCount(schema)).toBe('1');
  });

  it('serializes concurrent legacy migrators and seeds once', async () => {
    const schema = await createSchema('revision_concurrent');
    await createAllWorkflowTablesExceptRevisionHistory(schema);
    await insertSnapshot(schema, 'workflow', 'run', 'running');
    const firstPool = new Pool({ ...TEST_CONFIG, max: 1, application_name: `pf1946-first-${schema}` });
    const secondPool = new Pool({ ...TEST_CONFIG, max: 1, application_name: `pf1946-second-${schema}` });
    const coordinator = await pool.connect();
    const first = new WorkflowsPG({ pool: firstPool, schemaName: schema, skipDefaultIndexes: true });
    const second = new WorkflowsPG({ pool: secondPool, schemaName: schema, skipDefaultIndexes: true });
    const firstPid = (await firstPool.query<{ pid: number }>('SELECT pg_backend_pid() AS pid')).rows[0]!.pid;
    const secondPid = (await secondPool.query<{ pid: number }>('SELECT pg_backend_pid() AS pid')).rows[0]!.pid;
    expect(firstPid).not.toBe(secondPid);
    let firstInit: Promise<void> | undefined;
    let secondInit: Promise<void> | undefined;
    try {
      await coordinator.query(
        `SELECT pg_advisory_lock(
           hashtextextended(current_database() || E'\\n' || $1 || E'\\n' || $2, 0)
         )`,
        [schema, MIGRATION_KEY],
      );
      firstInit = first.init();
      secondInit = second.init();
      await Promise.all([
        waitForBlockedLock({ pid: firstPid, lockType: 'advisory' }),
        waitForBlockedLock({ pid: secondPid, lockType: 'advisory' }),
      ]);
      await coordinator.query(
        `SELECT pg_advisory_unlock(
           hashtextextended(current_database() || E'\\n' || $1 || E'\\n' || $2, 0)
         )`,
        [schema, MIGRATION_KEY],
      );

      await Promise.all([firstInit, secondInit]);
      const revisions = await pool.query(
        `SELECT generation::text, terminal_status FROM "${schema}"."${REVISION_TABLE}"`,
      );
      expect(revisions.rows).toEqual([{ generation: '1', terminal_status: null }]);
      expect(await markerCount(schema)).toBe('1');
      expect(await epochCount(schema)).toBe('1');
    } finally {
      await coordinator
        .query(
          `SELECT pg_advisory_unlock(
             hashtextextended(current_database() || E'\\n' || $1 || E'\\n' || $2, 0)
           )`,
          [schema, MIGRATION_KEY],
        )
        .catch(() => undefined);
      coordinator.release();
      await Promise.all([firstInit?.catch(() => undefined), secondInit?.catch(() => undefined)]);
      await Promise.all([firstPool.end(), secondPool.end()]);
    }
  }, 30000);

  it('drains a terminal writer before taking the journal lock without a lock-cycle deadlock', async () => {
    const schema = await createSchema('revision_lock_terminal');
    await createEvidenceTables(schema);
    await createCurrentRevisionTable(schema);
    await insertSnapshot(schema, 'workflow', 'run', 'running');
    await insertJournal(schema, 'workflow', 'run', 'failed');
    await pool.query(
      `INSERT INTO "${schema}"."${REVISION_TABLE}"
       (workflow_name, run_id, generation, terminal_status, updated_at)
       VALUES ('workflow', 'run', 2, 'failed', 2)`,
    );

    const writer = await pool.connect();
    const migratorPool = new Pool({
      ...TEST_CONFIG,
      max: 1,
      application_name: `pf1946-terminal-${schema}`,
    });
    const migrator = new WorkflowsPG({ pool: migratorPool, schemaName: schema, skipDefaultIndexes: true });
    const migratorPid = (await migratorPool.query<{ pid: number }>('SELECT pg_backend_pid() AS pid')).rows[0]!.pid;
    let initPromise: Promise<void> | undefined;
    try {
      await writer.query(`SET statement_timeout = '15s'`);
      await migratorPool.query(`SET statement_timeout = '15s'`);
      await writer.query(
        `SELECT pg_advisory_lock(
           hashtextextended(current_database() || E'\\n' || $1 || E'\\n' || $2, 0)
        )`,
        [schema, MIGRATION_KEY],
      );
      initPromise = migrator.init();
      await waitForBlockedLock({ pid: migratorPid, lockType: 'advisory' });
      await writer.query('BEGIN');
      await writer.query(
        `SELECT 1 FROM "${schema}"."mastra_workflow_terminalizations"
         WHERE workflow_name = 'workflow' AND run_id = 'run' FOR UPDATE`,
      );
      await writer.query(
        `UPDATE "${schema}"."${REVISION_TABLE}"
         SET updated_at = updated_at WHERE workflow_name = 'workflow' AND run_id = 'run'`,
      );
      await writer.query(
        `SELECT pg_advisory_unlock(
           hashtextextended(current_database() || E'\\n' || $1 || E'\\n' || $2, 0)
         )`,
        [schema, MIGRATION_KEY],
      );
      await waitForBlockedLock({
        pid: migratorPid,
        lockType: 'relation',
        relation: `"${schema}"."mastra_workflow_terminalizations"`,
        mode: 'ExclusiveLock',
      });

      await writer.query(
        `UPDATE "${schema}"."mastra_workflow_terminalizations"
         SET updated_at = updated_at + 1 WHERE workflow_name = 'workflow' AND run_id = 'run'`,
      );
      await writer.query('COMMIT');
      await expect(initPromise).resolves.toBeUndefined();
      expect(await markerCount(schema)).toBe('1');
    } finally {
      await writer.query('ROLLBACK').catch(() => undefined);
      await writer
        .query(
          `SELECT pg_advisory_unlock(
             hashtextextended(current_database() || E'\\n' || $1 || E'\\n' || $2, 0)
           )`,
          [schema, MIGRATION_KEY],
        )
        .catch(() => undefined);
      writer.release();
      await initPromise?.catch(() => undefined);
      await migratorPool.end();
    }
  }, 30000);

  it('drains a revision-first snapshot writer before taking later locks without a lock-cycle deadlock', async () => {
    const schema = await createSchema('revision_lock_strict');
    await createEvidenceTables(schema);
    await createCurrentRevisionTable(schema);
    await insertSnapshot(schema, 'workflow', 'run', 'running');
    await pool.query(
      `INSERT INTO "${schema}"."${REVISION_TABLE}"
       (workflow_name, run_id, generation, terminal_status, updated_at)
       VALUES ('workflow', 'run', 2, NULL, 2)`,
    );

    const writer = await pool.connect();
    const migratorPool = new Pool({
      ...TEST_CONFIG,
      max: 1,
      application_name: `pf1946-strict-${schema}`,
    });
    const migrator = new WorkflowsPG({ pool: migratorPool, schemaName: schema, skipDefaultIndexes: true });
    const migratorPid = (await migratorPool.query<{ pid: number }>('SELECT pg_backend_pid() AS pid')).rows[0]!.pid;
    let initPromise: Promise<void> | undefined;
    try {
      await writer.query(`SET statement_timeout = '15s'`);
      await migratorPool.query(`SET statement_timeout = '15s'`);
      await writer.query(
        `SELECT pg_advisory_lock(
           hashtextextended(current_database() || E'\\n' || $1 || E'\\n' || $2, 0)
        )`,
        [schema, MIGRATION_KEY],
      );
      initPromise = migrator.init();
      await waitForBlockedLock({ pid: migratorPid, lockType: 'advisory' });
      await writer.query('BEGIN');
      await writer.query(
        `SELECT 1 FROM "${schema}"."${REVISION_TABLE}"
         WHERE workflow_name = 'workflow' AND run_id = 'run' FOR UPDATE`,
      );
      await writer.query(
        `UPDATE "${schema}"."mastra_workflow_snapshot"
         SET "updatedAt" = "updatedAt" WHERE workflow_name = 'workflow' AND run_id = 'run'`,
      );
      await writer.query(
        `SELECT pg_advisory_unlock(
           hashtextextended(current_database() || E'\\n' || $1 || E'\\n' || $2, 0)
         )`,
        [schema, MIGRATION_KEY],
      );
      await waitForBlockedLock({
        pid: migratorPid,
        lockType: 'relation',
        relation: `"${schema}"."${REVISION_TABLE}"`,
        mode: 'ExclusiveLock',
      });
      const laterSnapshotLock = await pool.query(
        `SELECT 1 FROM pg_catalog.pg_locks
         WHERE pid = $1
           AND relation = to_regclass($2)
           AND mode IN ('ExclusiveLock', 'AccessExclusiveLock')
           AND granted`,
        [migratorPid, `"${schema}"."mastra_workflow_snapshot"`],
      );
      expect(laterSnapshotLock.rowCount).toBe(0);

      await writer.query(
        `UPDATE "${schema}"."${REVISION_TABLE}"
         SET updated_at = updated_at + 1 WHERE workflow_name = 'workflow' AND run_id = 'run'`,
      );
      await writer.query('COMMIT');
      await expect(initPromise).resolves.toBeUndefined();
      expect(await markerCount(schema)).toBe('1');
    } finally {
      await writer.query('ROLLBACK').catch(() => undefined);
      await writer
        .query(
          `SELECT pg_advisory_unlock(
             hashtextextended(current_database() || E'\\n' || $1 || E'\\n' || $2, 0)
           )`,
          [schema, MIGRATION_KEY],
        )
        .catch(() => undefined);
      writer.release();
      await initPromise?.catch(() => undefined);
      await migratorPool.end();
    }
  }, 30000);

  it('fails closed when either migration provenance record is missing', async () => {
    const schema = await createSchema('revision_provenance_loss');
    const workflows = new WorkflowsPG({ pool, schemaName: schema, skipDefaultIndexes: true });
    await workflows.init();
    const run = { workflowName: 'workflow', runId: 'run' };
    const snapshot = { ...createEmptyWorkflowSnapshot(run.runId), status: 'running' as const };
    await workflows.persistWorkflowSnapshot({ ...run, snapshot });
    const before = await pool.query(
      `SELECT generation::text, terminal_status, updated_at::text, xmin::text AS row_version
       FROM "${schema}"."${REVISION_TABLE}"`,
    );

    await pool.query(`DELETE FROM "${schema}"."${MARKER_TABLE}" WHERE migration_key = $1`, [MIGRATION_KEY]);
    await expect(workflows.init()).rejects.toThrow('migration provenance is damaged or incomplete');
    const afterMarkerLoss = await pool.query(
      `SELECT generation::text, terminal_status, updated_at::text, xmin::text AS row_version
       FROM "${schema}"."${REVISION_TABLE}"`,
    );
    expect(afterMarkerLoss.rows).toEqual(before.rows);
    expect(await markerCount(schema)).toBe('0');
    expect(await epochCount(schema)).toBe('1');

    await pool.query(`INSERT INTO "${schema}"."${MARKER_TABLE}" (migration_key, applied_at) VALUES ($1, 1)`, [
      MIGRATION_KEY,
    ]);
    await pool.query(`DELETE FROM "${schema}"."${EPOCH_TABLE}" WHERE epoch = 1`);
    await expect(workflows.init()).rejects.toThrow('migration provenance is damaged or incomplete');
    expect(await markerCount(schema)).toBe('1');
    expect(await epochCount(schema)).toBe('0');
  });

  it('never recreates deleted revision tombstones after marker loss', async () => {
    const schema = await createSchema('revision_no_repair');
    const workflows = new WorkflowsPG({ pool, schemaName: schema, skipDefaultIndexes: true });
    await workflows.init();
    const run = { workflowName: 'workflow', runId: 'run' };
    const snapshot = { ...createEmptyWorkflowSnapshot(run.runId), status: 'failed' as const };
    await workflows.persistWorkflowSnapshot({ ...run, snapshot });
    await pool.query(`DELETE FROM "${schema}"."mastra_workflow_snapshot" WHERE workflow_name = $1 AND run_id = $2`, [
      run.workflowName,
      run.runId,
    ]);

    await pool.query(`DELETE FROM "${schema}"."${REVISION_TABLE}" WHERE workflow_name = $1 AND run_id = $2`, [
      run.workflowName,
      run.runId,
    ]);
    await pool.query(`DELETE FROM "${schema}"."${MARKER_TABLE}" WHERE migration_key = $1`, [MIGRATION_KEY]);

    const restarted = new WorkflowsPG({ pool, schemaName: schema, skipDefaultIndexes: true });
    await expect(restarted.init()).rejects.toThrow('migration provenance is damaged or incomplete');
    expect(await markerCount(schema)).toBe('0');
    expect(await epochCount(schema)).toBe('1');
    const revisions = await pool.query(
      `SELECT generation FROM "${schema}"."${REVISION_TABLE}"
       WHERE workflow_name = $1 AND run_id = $2`,
      [run.workflowName, run.runId],
    );
    expect(revisions.rowCount).toBe(0);
  });

  it('leaves row-level revision loss to fail closed on every runtime mutation', async () => {
    const schema = await createSchema('revision_runtime_loss');
    const workflows = new WorkflowsPG({ pool, schemaName: schema, skipDefaultIndexes: true });
    await workflows.init();
    const run = { workflowName: 'workflow', runId: 'run' };
    const snapshot = { ...createEmptyWorkflowSnapshot(run.runId), status: 'running' as const };
    await workflows.persistWorkflowSnapshot({ ...run, snapshot });
    await pool.query(`DELETE FROM "${schema}"."${REVISION_TABLE}" WHERE workflow_name = $1 AND run_id = $2`, [
      run.workflowName,
      run.runId,
    ]);

    await expect(workflows.init()).resolves.toBeUndefined();
    await expect(workflows.getWorkflowRunTerminalStatus(run)).rejects.toThrow('missing parent revision evidence');
    await expect(workflows.persistWorkflowSnapshot({ ...run, snapshot })).rejects.toThrow();
    await expect(workflows.updateWorkflowState({ ...run, opts: { status: 'suspended' } })).rejects.toThrow();
    await expect(workflows.deleteWorkflowRunById(run)).rejects.toThrow();
    await expect(
      workflows.claimWorkflowTerminalization({
        ...run,
        eventKey: 'terminal-event',
        terminalStatus: 'failed',
        ownerId: 'owner',
        leaseMs: 1000,
      }),
    ).rejects.toThrow('missing parent revision evidence');
    const retained = await pool.query(
      `SELECT snapshot FROM "${schema}"."mastra_workflow_snapshot"
       WHERE workflow_name = $1 AND run_id = $2`,
      [run.workflowName, run.runId],
    );
    const revisions = await pool.query(
      `SELECT generation FROM "${schema}"."${REVISION_TABLE}"
       WHERE workflow_name = $1 AND run_id = $2`,
      [run.workflowName, run.runId],
    );
    expect(retained.rowCount).toBe(1);
    expect(revisions.rowCount).toBe(0);
  });

  it('keeps generation zero provisional under two first writers and preserves terminal tombstones', async () => {
    const schema = await createSchema('revision_writers');
    const first = new WorkflowsPG({ pool, schemaName: schema, skipDefaultIndexes: true });
    const second = new WorkflowsPG({ pool, schemaName: schema, skipDefaultIndexes: true });
    await first.init();
    const run = { workflowName: 'workflow', runId: 'run' };
    await Promise.all([
      first.persistWorkflowSnapshot({
        ...run,
        snapshot: { ...createEmptyWorkflowSnapshot(run.runId), status: 'running' },
      }),
      second.persistWorkflowSnapshot({
        ...run,
        snapshot: { ...createEmptyWorkflowSnapshot(run.runId), status: 'suspended' },
      }),
    ]);
    const revision = await pool.query<{ generation: string }>(
      `SELECT generation::text FROM "${schema}"."${REVISION_TABLE}"
       WHERE workflow_name = $1 AND run_id = $2`,
      [run.workflowName, run.runId],
    );
    expect(revision.rows).toEqual([{ generation: '2' }]);
    const zero = await pool.query<{ count: string }>(
      `SELECT count(*)::text AS count FROM "${schema}"."${REVISION_TABLE}" WHERE generation = 0`,
    );
    expect(zero.rows[0]!.count).toBe('0');

    const terminal = { workflowName: 'terminal-workflow', runId: 'terminal-run' };
    await first.persistWorkflowSnapshot({
      ...terminal,
      snapshot: { ...createEmptyWorkflowSnapshot(terminal.runId), status: 'failed' },
    });
    await pool.query(`DELETE FROM "${schema}"."mastra_workflow_snapshot" WHERE workflow_name = $1 AND run_id = $2`, [
      terminal.workflowName,
      terminal.runId,
    ]);
    await expect(first.getWorkflowRunTerminalStatus(terminal)).resolves.toEqual({
      status: 'terminal',
      terminalStatus: 'failed',
    });
  });

  it('fails terminal status reads and snapshot upserts closed on zero or mismatched revisions', async () => {
    const schema = await createSchema('revision_runtime_corrupt');
    const workflows = new WorkflowsPG({ pool, schemaName: schema, skipDefaultIndexes: true });
    await workflows.init();

    const zero = { workflowName: 'zero-workflow', runId: 'zero-run' };
    const zeroSnapshot = { ...createEmptyWorkflowSnapshot(zero.runId), status: 'running' as const };
    await workflows.persistWorkflowSnapshot({ ...zero, snapshot: zeroSnapshot });
    await pool.query(
      `UPDATE "${schema}"."${REVISION_TABLE}" SET generation = 0
       WHERE workflow_name = $1 AND run_id = $2`,
      [zero.workflowName, zero.runId],
    );
    await expect(workflows.getWorkflowRunTerminalStatus(zero)).rejects.toThrow(
      'Invalid workflow parent revision generation',
    );
    await expect(workflows.persistWorkflowSnapshot({ ...zero, snapshot: zeroSnapshot })).rejects.toThrow();
    const retainedZero = await pool.query<{ generation: string }>(
      `SELECT generation::text FROM "${schema}"."${REVISION_TABLE}"
       WHERE workflow_name = $1 AND run_id = $2`,
      [zero.workflowName, zero.runId],
    );
    expect(retainedZero.rows).toEqual([{ generation: '0' }]);

    const mismatch = { workflowName: 'mismatch-workflow', runId: 'mismatch-run' };
    await workflows.persistWorkflowSnapshot({
      ...mismatch,
      snapshot: { ...createEmptyWorkflowSnapshot(mismatch.runId), status: 'failed' },
    });
    await pool.query(
      `UPDATE "${schema}"."mastra_workflow_snapshot"
       SET snapshot = jsonb_set(snapshot, '{status}', '"success"'::jsonb)
       WHERE workflow_name = $1 AND run_id = $2`,
      [mismatch.workflowName, mismatch.runId],
    );
    await expect(workflows.getWorkflowRunTerminalStatus(mismatch)).rejects.toThrow(
      'Workflow parent revision conflicts with terminal snapshot status',
    );
  });
});
