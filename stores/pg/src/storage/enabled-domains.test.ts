import { MastraError } from '@mastra/core/error';
import { DOMAIN_KEYS } from '@mastra/core/storage';
import { Client, Pool } from 'pg';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { MemoryPG } from './domains/memory';
import { ObservabilityStoragePostgresVNext } from './domains/observability/v-next';
import { connectionString } from './test-utils';
import { PostgresStore, PostgresStoreVNext } from '.';

const DEAD_CONNECTION_STRING = 'postgresql://user:pass@127.0.0.1:1/db';
const CROSS_DOMAIN_SQL = /mastra_(workflow|harness|agents?|agent_versions)/i;
const CREATE_TABLE = /CREATE TABLE IF NOT EXISTS/i;
const NO_OP_ALTER = /ALTER TABLE[\s\S]*ADD COLUMN IF NOT EXISTS/i;
const CREATE_INDEX = /CREATE (UNIQUE )?INDEX/i;

async function captureStatements(fn: () => Promise<void>): Promise<string[]> {
  const statements: string[] = [];
  const original = Client.prototype.query;

  (Client.prototype as any).query = function (this: any, ...args: any[]) {
    const first = args[0];
    const text = typeof first === 'string' ? first : first?.text;
    if (typeof text === 'string') statements.push(text);
    return (original as any).apply(this, args);
  };

  try {
    await fn();
  } finally {
    (Client.prototype as any).query = original;
  }

  return statements;
}

function count(statements: string[], pattern: RegExp): number {
  return statements.filter(statement => pattern.test(statement)).length;
}

afterEach(() => {
  vi.restoreAllMocks();
});

describe('PostgresStore enabledDomains configuration', () => {
  it.each([
    {
      description: 'an empty list',
      enabledDomains: [],
      expectedError: /enabledDomains must contain at least one domain/i,
    },
    {
      description: 'an unknown domain',
      enabledDomains: ['not-a-domain'],
      expectedError: /enabledDomains contains unknown domain 'not-a-domain'/i,
    },
    {
      description: 'a duplicate domain',
      enabledDomains: ['memory', 'memory'],
      expectedError: /enabledDomains contains duplicate domain 'memory'/i,
    },
    {
      description: 'a non-array value',
      enabledDomains: 'memory',
      expectedError: /enabledDomains must be an array/i,
    },
  ])('rejects $description before creating an internal pool', ({ enabledDomains, expectedError }) => {
    const createPool = vi.spyOn(PostgresStore.prototype as any, 'createPool');

    expect(
      () =>
        new PostgresStore({
          id: 'invalid-enabled-domains',
          connectionString: DEAD_CONNECTION_STRING,
          enabledDomains,
        } as any),
    ).toThrow(expectedError);
    expect(createPool).not.toHaveBeenCalled();
  });

  it('registers every canonical domain when enabledDomains is omitted', async () => {
    const pool = new Pool({ connectionString: DEAD_CONNECTION_STRING });
    const store = new PostgresStore({ id: 'all-domains-by-default', pool });

    try {
      expect(Object.keys(store.stores).sort()).toEqual([...DOMAIN_KEYS].sort());
      for (const key of DOMAIN_KEYS) {
        expect(await store.getStore(key), `${key} should be registered by default`).toBeDefined();
      }
    } finally {
      await store.close();
      await pool.end();
    }
  });

  it('constructs and exposes only the selected domain', async () => {
    const pool = new Pool({ connectionString: DEAD_CONNECTION_STRING });
    const store = new PostgresStore({
      id: 'memory-domain-only',
      pool,
      enabledDomains: ['memory'],
    });

    try {
      expect(Object.keys(store.stores)).toEqual(['memory']);
      expect(await store.getStore('memory')).toBeInstanceOf(MemoryPG);
      expect(await store.getStore('workflows')).toBeUndefined();
      expect(await store.getStore('harness')).toBeUndefined();
      expect(await store.getStore('agents')).toBeUndefined();
    } finally {
      await store.close();
      await pool.end();
    }
  });

  it('rejects a vNext selection without observability before creating the primary pool', () => {
    const createPool = vi.spyOn(PostgresStore.prototype as any, 'createPool');
    let thrown: unknown;

    try {
      new PostgresStoreVNext({
        id: 'memory-domain-only-vnext',
        connectionString: DEAD_CONNECTION_STRING,
        enabledDomains: ['memory'],
        observability: { connectionString: DEAD_CONNECTION_STRING },
      });
    } catch (error) {
      thrown = error;
    }

    expect(thrown).toBeInstanceOf(MastraError);
    expect(thrown).toMatchObject({ id: 'MASTRA_STORAGE_PG_INITIALIZATION_FAILED' });
    expect((thrown as MastraError).cause?.message).toMatch(/enabledDomains must include 'observability'/i);
    expect(createPool).not.toHaveBeenCalled();
  });

  it('exposes only vNext observability when it is the sole selected domain', async () => {
    const primaryPool = new Pool({ connectionString: DEAD_CONNECTION_STRING });
    const observabilityPool = new Pool({ connectionString: 'postgresql://user:pass@127.0.0.1:2/db' });
    const store = new PostgresStoreVNext({
      id: 'observability-domain-only-vnext',
      pool: primaryPool,
      enabledDomains: ['observability'],
      observability: { pool: observabilityPool },
    });

    try {
      expect(Object.keys(store.stores)).toEqual(['observability']);
      expect(await store.getStore('observability')).toBeInstanceOf(ObservabilityStoragePostgresVNext);
    } finally {
      await store.close();
      await primaryPool.end();
      await observabilityPool.end();
    }
  });

  it('allows vNext observability alongside selected primary domains', async () => {
    const primaryPool = new Pool({ connectionString: DEAD_CONNECTION_STRING });
    const observabilityPool = new Pool({ connectionString: 'postgresql://user:pass@127.0.0.1:2/db' });
    const store = new PostgresStoreVNext({
      id: 'memory-and-observability-vnext',
      pool: primaryPool,
      enabledDomains: ['memory', 'observability'],
      observability: { pool: observabilityPool },
    });

    try {
      expect(Object.keys(store.stores).sort()).toEqual(['memory', 'observability']);
      expect(await store.getStore('memory')).toBeInstanceOf(MemoryPG);
      expect(await store.getStore('observability')).toBeInstanceOf(ObservabilityStoragePostgresVNext);
    } finally {
      await store.close();
      await primaryPool.end();
      await observabilityPool.end();
    }
  });
});

describe('PostgresStore memory-only initialization', () => {
  it('converges cold and spends only three read-only catalog calls when warm', async () => {
    const schemaName = `pf3552_budget_${Date.now()}_${Math.floor(Math.random() * 1e6)}`;
    const cold = new PostgresStore({
      id: `memory-budget-cold-${Date.now()}`,
      connectionString,
      schemaName,
      enabledDomains: ['memory'],
    });
    const warm = new PostgresStore({
      id: `memory-budget-warm-${Date.now()}`,
      connectionString,
      schemaName,
      enabledDomains: ['memory'],
    });
    const adminPool = new Pool({ connectionString });

    try {
      const coldStatements = await captureStatements(() => cold.init());
      await cold.close();

      const tables = await adminPool.query(
        'SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = $1 ORDER BY tablename',
        [schemaName],
      );
      expect(tables.rows.map(row => row.tablename)).toEqual([...MemoryPG.MANAGED_TABLES].sort());
      expect(coldStatements.filter(sql => CROSS_DOMAIN_SQL.test(sql))).toEqual([]);

      const warmStatements = await captureStatements(() => warm.init());
      const catalogStatements = warmStatements.filter(sql => /(?:pg_catalog|information_schema)\./i.test(sql));

      expect(warmStatements.length).toBeGreaterThanOrEqual(1);
      expect(warmStatements.length).toBeLessThanOrEqual(3);
      expect(warmStatements.every(sql => /^\s*SELECT\b/i.test(sql))).toBe(true);
      expect(catalogStatements).toHaveLength(3);
      expect(count(catalogStatements, /pg_catalog\.pg_tables/i)).toBe(1);
      expect(count(catalogStatements, /pg_catalog\.pg_attribute/i)).toBe(1);
      expect(count(catalogStatements, /pg_catalog\.pg_index\b/i)).toBe(1);
      expect(warmStatements.filter(sql => CROSS_DOMAIN_SQL.test(sql))).toEqual([]);
      expect(count(warmStatements, CREATE_TABLE)).toBe(0);
      expect(count(warmStatements, NO_OP_ALTER)).toBe(0);
      expect(count(warmStatements, CREATE_INDEX)).toBe(0);

      const memory = await warm.getStore('memory');
      expect(memory).toBeInstanceOf(MemoryPG);
      await expect(memory!.getThreadById({ threadId: 'missing-memory-only-thread' })).resolves.toBeNull();
    } finally {
      await cold.close();
      await warm.close();
      try {
        await adminPool.query(`DROP SCHEMA IF EXISTS "${schemaName}" CASCADE`);
      } finally {
        await adminPool.end();
      }
    }
  }, 90000);

  it('single-flights concurrent init and caches later same-instance calls', async () => {
    const schemaName = `pf3552_single_${Date.now()}_${Math.floor(Math.random() * 1e6)}`;
    const pool = new Pool({ connectionString });
    const originalConnect = pool.connect.bind(pool);
    let connects = 0;
    (pool as any).connect = (...args: any[]) => {
      connects++;
      return (originalConnect as any)(...args);
    };
    const store = new PostgresStore({
      id: `memory-single-flight-${Date.now()}`,
      pool,
      schemaName,
      enabledDomains: ['memory'],
    });

    try {
      await expect(Promise.all([store.init(), store.init(), store.init()])).resolves.toBeDefined();
      expect(connects).toBe(1);

      await expect(store.init()).resolves.toBeUndefined();
      expect(connects).toBe(1);
    } finally {
      await store.close();
      await pool.end();

      const cleanup = new Pool({ connectionString });
      try {
        await cleanup.query(`DROP SCHEMA IF EXISTS "${schemaName}" CASCADE`);
      } finally {
        await cleanup.end();
      }
    }
  });
});
