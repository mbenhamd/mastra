import type { DbClient } from '../client';

export interface SchemaCheckConstraint {
  expression: string;
  validated: boolean;
}

/**
 * A read-only picture of one Postgres schema's catalog, taken once at the start
 * of `PostgresStore.init()`.
 *
 * Before this existed, every domain's init converged its own tables by asking
 * the server: an `information_schema.columns` probe per column, a `pg_indexes`
 * probe per index, plus an unconditional `CREATE TABLE IF NOT EXISTS` and a
 * no-op `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` per column. On an
 * already-converged schema that is ~350 statements that change nothing, and
 * because init pins every domain to a single backend connection (issue #17679)
 * they are strictly serialized — warm init cost is ~350 x RTT.
 *
 * The snapshot answers all of those questions locally instead, from three
 * catalog reads.
 *
 * **Init-scoped by design.** The snapshot is installed on the store's
 * `RoutingDbClient` for exactly the pinned-init window and cleared in the same
 * `finally` that unpins, so staleness is bounded to a single init() call and
 * runtime code paths keep querying the live catalog. It is deliberately *not* a
 * process-global cache (that shape was proposed in PR #13960 and closed
 * unmerged): every init re-reads the catalog, so a schema that drifted out of
 * band between inits is still detected and healed.
 *
 * **Why `pg_catalog` and not `information_schema`.** `information_schema` views
 * are privilege-filtered: a role with USAGE on the schema but no grants on a
 * table sees the table but none of its columns, which would make the snapshot
 * report "table present, zero columns" and skip DDL that is genuinely needed.
 * The `pg_catalog` views used here are not privilege-filtered, so all three
 * queries share one visibility rule.
 */
export interface SchemaSnapshot {
  /** Schema the snapshot was taken from (`public` when the store has none). */
  readonly schemaName: string;
  /** Unqualified names of tables present in the schema. */
  tables: Set<string>;
  /** table name -> PostgreSQL relation kind (`r` ordinary, `p` partitioned). */
  tableKinds: Map<string, string>;
  /** Tables that are partitions of another relation. */
  partitionedTables: Set<string>;
  /** Tables participating as either parent or child in PostgreSQL inheritance. */
  inheritedTables: Set<string>;
  /** table name -> PostgreSQL persistence (`p` permanent, `u` unlogged, `t` temporary). */
  tablePersistence: Map<string, string>;
  /** table name -> column names present on that table. */
  columns: Map<string, Set<string>>;
  /** table name -> column name -> Postgres type name (`jsonb`, `text`, ...). */
  columnTypes: Map<string, Map<string, string>>;
  /** table name -> columns declared NOT NULL. */
  notNullColumns: Map<string, Set<string>>;
  /** table name -> columns with a catalog default. */
  columnsWithDefaults: Map<string, Set<string>>;
  /** table name -> validated state and normalized expression for each CHECK constraint. */
  checkConstraints: Map<string, SchemaCheckConstraint[]>;
  /** table name -> PRIMARY KEY columns in key order. */
  primaryKeyColumns: Map<string, string[]>;
  /** Tables whose PRIMARY KEY can be used as an immediate `ON CONFLICT` arbiter. */
  immediatePrimaryKeyTables: Set<string>;
  /** Index names present in the schema, exactly as the catalog stores them. */
  indexes: Set<string>;
  /** Names of indexes that are the replica identity of their table. */
  replicaIdentityIndexes: Set<string>;
  /**
   * Lowercased names of PRIMARY KEY indexes. A primary key is always backed by
   * an index of the same name, so this answers "does constraint X exist?" for
   * primary keys without a `pg_constraint` probe. Lowercased because the
   * queries it replaces compare `conname = lower($1)`.
   */
  primaryKeyIndexes: Set<string>;
}

/**
 * Implemented by `RoutingDbClient` so the `PgDB` instances that share it (one
 * per storage domain) can all read the single snapshot loaded for the current
 * init window.
 */
export interface SchemaSnapshotHost {
  readonly schemaSnapshot: SchemaSnapshot | null;
}

/**
 * Returns the snapshot currently installed on `client`, but only when it was
 * taken from the schema the caller operates on. A store configured for another
 * schema (or a client with no snapshot at all) gets `null` and falls back to
 * probing the live catalog.
 */
export function getSchemaSnapshot(client: unknown, schemaName: string | undefined): SchemaSnapshot | null {
  const snapshot = (client as Partial<SchemaSnapshotHost> | null | undefined)?.schemaSnapshot;
  if (!snapshot) return null;
  return snapshot.schemaName === (schemaName || 'public') ? snapshot : null;
}

/**
 * Reads the catalog for `schemaName` in three queries. Must be called on the
 * pinned init client so the snapshot reflects what that connection will see.
 */
export async function loadSchemaSnapshot(client: DbClient, schemaName: string | undefined): Promise<SchemaSnapshot> {
  const schema = schemaName || 'public';

  const [tableRows, columnRows, indexRows] = await Promise.all([
    client.manyOrNone<{
      tablename: string;
      kind: string;
      is_partition: boolean;
      has_inheritance: boolean;
      persistence: string;
      check_constraints: SchemaCheckConstraint[];
    }>(
      `SELECT table_row.tablename,
              relation_row.relkind AS kind,
              relation_row.relispartition AS is_partition,
              EXISTS (
                SELECT 1
                FROM pg_catalog.pg_inherits AS inheritance_row
                WHERE inheritance_row.inhrelid = relation_row.oid
                   OR inheritance_row.inhparent = relation_row.oid
              ) AS has_inheritance,
              relation_row.relpersistence AS persistence,
              COALESCE(constraint_rows.check_constraints, '[]'::jsonb) AS check_constraints
         FROM pg_catalog.pg_tables AS table_row
         JOIN pg_catalog.pg_namespace AS namespace_row
           ON namespace_row.nspname = table_row.schemaname
         JOIN pg_catalog.pg_class AS relation_row
           ON relation_row.relnamespace = namespace_row.oid
          AND relation_row.relname = table_row.tablename
          AND relation_row.relkind IN ('r', 'p')
         LEFT JOIN LATERAL (
           SELECT jsonb_agg(
                    jsonb_build_object(
                      'expression', pg_get_expr(constraint_row.conbin, constraint_row.conrelid, true),
                      'validated', constraint_row.convalidated
                    )
                    ORDER BY constraint_row.oid
                  ) AS check_constraints
             FROM pg_catalog.pg_constraint AS constraint_row
            WHERE constraint_row.conrelid = relation_row.oid
              AND constraint_row.contype = 'c'
         ) AS constraint_rows ON TRUE
        WHERE table_row.schemaname = $1`,
      [schema],
    ),
    client.manyOrNone<{
      table_name: string;
      column_name: string;
      data_type: string;
      is_not_null: boolean;
      has_default: boolean;
    }>(
      `SELECT c.relname AS table_name,
              a.attname AS column_name,
              format_type(a.atttypid, a.atttypmod) AS data_type,
              a.attnotnull AS is_not_null,
              (a.atthasdef OR a.attidentity <> '') AS has_default
         FROM pg_catalog.pg_class c
         JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
         JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid
        WHERE n.nspname = $1
          AND c.relkind IN ('r', 'p')
          AND a.attnum > 0
          AND NOT a.attisdropped`,
      [schema],
    ),
    // pg_index rather than the pg_indexes view: same names, but it also carries
    // indisreplident, which createTable needs to know whether the
    // workflow_snapshot unique index is already the table's replica identity,
    // and indisprimary, which answers primary-key constraint existence.
    client.manyOrNone<{
      indexname: string;
      table_name: string;
      key_columns: string[];
      is_replica_identity: boolean;
      is_primary: boolean;
      is_immediate: boolean;
    }>(
      `SELECT index_row.relname AS indexname,
              table_row.relname AS table_name,
              ARRAY(
                SELECT column_row.attname::text
                FROM unnest(index_metadata.indkey)
                  WITH ORDINALITY AS key_column(attnum, ordinal_position)
                JOIN pg_catalog.pg_attribute AS column_row
                  ON column_row.attrelid = index_metadata.indrelid
                 AND column_row.attnum = key_column.attnum
                WHERE key_column.ordinal_position <= index_metadata.indnkeyatts
                ORDER BY key_column.ordinal_position
              ) AS key_columns,
              index_metadata.indisreplident AS is_replica_identity,
              index_metadata.indisprimary AS is_primary,
              index_metadata.indimmediate AS is_immediate
         FROM pg_catalog.pg_index AS index_metadata
         JOIN pg_catalog.pg_class AS index_row ON index_row.oid = index_metadata.indexrelid
         JOIN pg_catalog.pg_class AS table_row ON table_row.oid = index_metadata.indrelid
         JOIN pg_catalog.pg_namespace AS namespace_row ON namespace_row.oid = table_row.relnamespace
        WHERE namespace_row.nspname = $1`,
      [schema],
    ),
  ]);

  const columns = new Map<string, Set<string>>();
  const columnTypes = new Map<string, Map<string, string>>();
  const notNullColumns = new Map<string, Set<string>>();
  const columnsWithDefaults = new Map<string, Set<string>>();
  for (const row of columnRows) {
    let set = columns.get(row.table_name);
    if (!set) {
      set = new Set<string>();
      columns.set(row.table_name, set);
    }
    set.add(row.column_name);

    let types = columnTypes.get(row.table_name);
    if (!types) {
      types = new Map<string, string>();
      columnTypes.set(row.table_name, types);
    }
    types.set(row.column_name, row.data_type);

    if (row.is_not_null) {
      let notNull = notNullColumns.get(row.table_name);
      if (!notNull) {
        notNull = new Set<string>();
        notNullColumns.set(row.table_name, notNull);
      }
      notNull.add(row.column_name);
    }

    if (row.has_default) {
      let withDefaults = columnsWithDefaults.get(row.table_name);
      if (!withDefaults) {
        withDefaults = new Set<string>();
        columnsWithDefaults.set(row.table_name, withDefaults);
      }
      withDefaults.add(row.column_name);
    }
  }

  const checkConstraints = new Map(
    tableRows.map(row => [row.tablename, row.check_constraints] satisfies [string, SchemaCheckConstraint[]]),
  );

  const indexes = new Set<string>();
  const replicaIdentityIndexes = new Set<string>();
  const primaryKeyIndexes = new Set<string>();
  const primaryKeyColumns = new Map<string, string[]>();
  const immediatePrimaryKeyTables = new Set<string>();
  for (const row of indexRows) {
    indexes.add(row.indexname);
    if (row.is_replica_identity) {
      replicaIdentityIndexes.add(row.indexname);
    }
    if (row.is_primary) {
      primaryKeyIndexes.add(row.indexname.toLowerCase());
      primaryKeyColumns.set(row.table_name, row.key_columns);
      if (row.is_immediate) immediatePrimaryKeyTables.add(row.table_name);
    }
  }

  return {
    schemaName: schema,
    tables: new Set(tableRows.map(r => r.tablename)),
    tableKinds: new Map(tableRows.map(row => [row.tablename, row.kind])),
    partitionedTables: new Set(tableRows.filter(row => row.is_partition).map(row => row.tablename)),
    inheritedTables: new Set(tableRows.filter(row => row.has_inheritance).map(row => row.tablename)),
    tablePersistence: new Map(tableRows.map(row => [row.tablename, row.persistence])),
    columns,
    columnTypes,
    notNullColumns,
    columnsWithDefaults,
    checkConstraints,
    primaryKeyColumns,
    immediatePrimaryKeyTables,
    indexes,
    replicaIdentityIndexes,
    primaryKeyIndexes,
  };
}
