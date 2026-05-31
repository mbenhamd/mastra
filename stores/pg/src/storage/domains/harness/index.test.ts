import { randomUUID } from 'node:crypto';

import { createSampleSessionRecord } from '@internal/storage-test-utils';
import {
  HarnessStorageAttachmentInUseError,
  HarnessStorageAttachmentUnavailableError,
  HarnessStorageChannelBindingConflictError,
  HarnessStorageLeaseConflictError,
  HarnessStorageProviderCallbackBindingTransitionError,
  HarnessStorageSessionNotFoundError,
  HarnessStorageThreadDeleteFenceConflictError,
  TABLE_HARNESS_PROVIDER_CALLBACK_BINDINGS,
  TABLE_HARNESS_SESSION_EVENTS,
  TABLE_HARNESS_WORKSPACE_ACTIONS,
} from '@mastra/core/storage';
import type {
  ChannelBinding,
  HarnessProviderCallbackBinding,
  WorkspaceActionJournalEntry,
} from '@mastra/core/storage';
import { describe, expect, it, beforeAll, beforeEach, afterAll, vi } from 'vitest';

import { exportSchemas, HarnessPG, PostgresStore } from '../..';
import { TEST_CONFIG } from '../../test-utils';

vi.setConfig({ testTimeout: 60_000, hookTimeout: 60_000 });

describe('HarnessPG', () => {
  const store = new PostgresStore({ ...TEST_CONFIG, id: 'pg-harness-test-store' });

  beforeAll(async () => {
    await store.init();
  });

  beforeEach(async () => {
    await store.stores.harness?.dangerouslyClearAll();
  });

  afterAll(async () => {
    await store.stores.harness?.dangerouslyClearAll().catch(() => {});
    await store.close();
  });

  it('exports harness tables and creates live default indexes', async () => {
    const ddl = exportSchemas();

    expect(ddl).toContain('mastra_harness_sessions');
    expect(ddl).toContain('mastra_harness_attachments');
    expect(ddl).toContain('mastra_harness_channel_inbox');
    expect(ddl).toContain('mastra_harness_wakeups');
    expect(ddl).toContain(TABLE_HARNESS_SESSION_EVENTS);
    expect(ddl).toContain(TABLE_HARNESS_WORKSPACE_ACTIONS);
    expect(ddl).toContain('idx_harness_sessions_active_key');
    expect(ddl).toContain('idx_harness_sessions_parent');
    expect(ddl).toContain('idx_harness_message_results_lookup');
    expect(ddl).toContain('idx_harness_message_results_admission');
    expect(ddl).toContain('idx_harness_tombstones_lookup');
    expect(ddl).toContain('idx_harness_session_events_replay');
    expect(ddl).toContain('idx_harness_workspace_actions_page');
    expect(ddl).toContain('idx_harness_workspace_actions_session');
    expect(ddl).toContain('idx_harness_channel_outbox_idempotency');

    const indexes = await store.db.manyOrNone<{ indexname: string }>(
      `SELECT indexname FROM pg_indexes
       WHERE schemaname = 'public'
         AND indexname = ANY($1)
       ORDER BY indexname`,
      [
        [
          'idx_harness_sessions_active_key',
          'idx_harness_channel_inbox_idempotency',
          'idx_harness_channel_outbox_idempotency',
          'idx_harness_session_events_replay',
          'idx_harness_wakeups_idempotency',
          'idx_harness_workspace_actions_page',
          'idx_harness_workspace_actions_session',
        ],
      ],
    );
    expect(indexes.map(row => row.indexname)).toEqual([
      'idx_harness_channel_inbox_idempotency',
      'idx_harness_channel_outbox_idempotency',
      'idx_harness_session_events_replay',
      'idx_harness_sessions_active_key',
      'idx_harness_wakeups_idempotency',
      'idx_harness_workspace_actions_page',
      'idx_harness_workspace_actions_session',
    ]);
  });

  it('keeps long schema-prefixed default index names valid and unique', async () => {
    const schemaName = 'migration_test_schema_1779266119440_dfd7f958d5b5e8';
    const indexNames = HarnessPG.getDefaultIndexDefs(`${schemaName}_`).map(index => index.name);

    expect(indexNames).toHaveLength(new Set(indexNames).size);
    for (const indexName of indexNames) {
      expect(Buffer.byteLength(indexName, 'utf-8')).toBeLessThanOrEqual(63);
    }

    const ddl = HarnessPG.getExportDDL(schemaName).join('\n');
    expect(ddl).toContain('CREATE INDEX');
    expect(ddl).toMatch(/_idx_[0-9a-f]{8}/);
    expect(ddl).not.toContain(`${schemaName}_idx_harness_session_events_replay`);
  });

  it('preserves existing mixed-case short schema index names', () => {
    const indexNames = HarnessPG.getDefaultIndexDefs('TenantA_').map(index => index.name);

    expect(indexNames).toContain('TenantA_idx_harness_sessions_active_key');
    for (const indexName of indexNames) {
      expect(indexName.startsWith('TenantA_')).toBe(true);
      expect(Buffer.byteLength(indexName, 'utf-8')).toBeLessThanOrEqual(63);
    }
  });

  it('initializes Harness indexes for long schema names', async () => {
    const schemaName = `harness_long_schema_${randomUUID().replaceAll('-', '_')}`;
    const longSchemaStore = new PostgresStore({
      ...TEST_CONFIG,
      id: 'pg-harness-long-schema-indexes',
      schemaName,
    });
    try {
      await longSchemaStore.init();
      const expectedNames = HarnessPG.getDefaultIndexDefs(`${schemaName}_`).map(index => index.name);
      const indexes = await longSchemaStore.db.manyOrNone<{ indexname: string }>(
        `SELECT indexname FROM pg_indexes
         WHERE schemaname = $1
           AND indexname = ANY($2)
         ORDER BY indexname`,
        [schemaName, expectedNames],
      );

      expect(indexes.map(row => row.indexname)).toEqual([...expectedNames].sort());
    } finally {
      await longSchemaStore.close().catch(() => {});
      const cleanupStore = new PostgresStore({ ...TEST_CONFIG, id: 'pg-harness-long-schema-index-cleanup' });
      try {
        await cleanupStore.db.none(`DROP SCHEMA IF EXISTS "${schemaName}" CASCADE`);
      } finally {
        await cleanupStore.close();
      }
    }
  });

  it('allows legacy duplicate active sessions when default indexes are skipped', async () => {
    const schemaName = `harness_skip_${randomUUID().replaceAll('-', '_')}`;
    let seedStore: PostgresStore | undefined;
    let reopenedStore: PostgresStore | undefined;
    try {
      seedStore = new PostgresStore({
        ...TEST_CONFIG,
        id: 'pg-harness-skip-index-seed',
        schemaName,
        skipDefaultIndexes: true,
      });
      await seedStore.init();
      await seedStore.stores.harness!.saveSession(
        createSampleSessionRecord({
          id: 'legacy-active-a',
          resourceId: 'legacy-resource',
          threadId: 'legacy-thread',
        }),
        { ownerId: 'owner-a', ifVersion: 0 },
      );
      await seedStore.stores.harness!.saveSession(
        createSampleSessionRecord({
          id: 'legacy-active-b',
          resourceId: 'legacy-resource',
          threadId: 'legacy-thread',
        }),
        { ownerId: 'owner-b', ifVersion: 0 },
      );
      await seedStore.close();
      seedStore = undefined;

      reopenedStore = new PostgresStore({
        ...TEST_CONFIG,
        id: 'pg-harness-skip-index-reopen',
        schemaName,
        skipDefaultIndexes: true,
      });
      await expect(reopenedStore.init()).resolves.toBeUndefined();
    } finally {
      await reopenedStore?.close().catch(() => {});
      await seedStore?.close().catch(() => {});
      const cleanupStore = new PostgresStore({ ...TEST_CONFIG, id: 'pg-harness-skip-index-cleanup' });
      try {
        await cleanupStore.db.none(`DROP SCHEMA IF EXISTS "${schemaName}" CASCADE`);
      } finally {
        await cleanupStore.close();
      }
    }
  });

  it('persists primitive and element attachment metadata including object pointers', async () => {
    const harness = store.stores.harness;
    expect(harness).toBeDefined();

    await harness!.saveAttachment({
      sessionId: 'session-1',
      attachmentId: 'primitive-json',
      name: 'data.json',
      mimeType: 'application/json',
      source: 'provider',
      data: new TextEncoder().encode('{"ok":true}'),
      semantic: {
        kind: 'primitive',
        primitiveType: 'json',
        renderer: { id: 'json-viewer', version: '1' },
        schemaId: 'schema:paper-metadata',
        metadata: { label: 'metadata', rows: 1 },
        object: {
          providerId: 'cloudflare-r2',
          objectKey: 'harness/default/sessions/session-1/attachments/primitive-json/hash',
          etag: 'etag-1',
          storageClass: 'standard',
        },
      },
    });
    await harness!.saveAttachment({
      sessionId: 'session-1',
      attachmentId: 'element-citation',
      name: 'citation.element',
      mimeType: 'application/vnd.mastra.element+json',
      source: 'inline',
      data: new TextEncoder().encode('citation'),
      semantic: {
        kind: 'element',
        elementType: 'citation-card',
        renderer: { id: 'citation-card', version: '2' },
        metadata: { doi: '10.1234/example' },
      },
    });

    await expect(
      harness!.loadAttachment({ sessionId: 'session-1', attachmentId: 'primitive-json' }),
    ).resolves.toMatchObject({
      semantic: {
        kind: 'primitive',
        primitiveType: 'json',
        renderer: { id: 'json-viewer', version: '1' },
        schemaId: 'schema:paper-metadata',
        metadata: { label: 'metadata', rows: 1 },
        object: {
          providerId: 'cloudflare-r2',
          objectKey: 'harness/default/sessions/session-1/attachments/primitive-json/hash',
          etag: 'etag-1',
          storageClass: 'standard',
        },
      },
    });
    await expect(
      harness!.getAttachmentRecord({ sessionId: 'session-1', attachmentId: 'element-citation' }),
    ).resolves.toMatchObject({
      kind: 'element',
      elementType: 'citation-card',
      renderer: { id: 'citation-card', version: '2' },
      metadata: { doi: '10.1234/example' },
    });
  });

  it('appends and pages workspace action journal rows by session/resource', async () => {
    const harness = store.stores.harness;
    expect(harness).toBeDefined();

    await harness!.saveSession(createSampleSessionRecord(), { ownerId: 'h-1', ifVersion: 0 });
    await harness!.saveSession(createSampleSessionRecord({ id: 'other-session', resourceId: 'other-resource' }), {
      ownerId: 'h-1',
      ifVersion: 0,
    });

    await harness!.appendWorkspaceActionJournalEntry(sampleWorkspaceActionJournalEntry({ id: 'b', createdAt: 1000 }));
    await harness!.appendWorkspaceActionJournalEntry(sampleWorkspaceActionJournalEntry({ id: 'a', createdAt: 1000 }));
    await harness!.appendWorkspaceActionJournalEntry(sampleWorkspaceActionJournalEntry({ id: 'c', createdAt: 1100 }));
    await harness!.appendWorkspaceActionJournalEntry(
      sampleWorkspaceActionJournalEntry({
        id: 'other-resource-entry',
        sessionId: 'other-session',
        resourceId: 'other-resource',
      }),
    );

    await expect(
      harness!.listWorkspaceActionJournalEntries({
        sessionId: 'session-1',
        resourceId: 'resource-1',
        limit: 2,
      }),
    ).resolves.toEqual([
      expect.objectContaining({ id: 'a', actionKind: 'file', path: expect.objectContaining({ rootId: 'project' }) }),
      expect.objectContaining({ id: 'b', actionKind: 'file' }),
    ]);
    await expect(
      harness!.listWorkspaceActionJournalEntries({
        sessionId: 'session-1',
        resourceId: 'resource-1',
        after: { createdAt: 1000, id: 'b' },
        limit: 10,
      }),
    ).resolves.toEqual([expect.objectContaining({ id: 'c' })]);
    await expect(
      harness!.listWorkspaceActionJournalEntries({
        sessionId: 'session-1',
        resourceId: 'resource-1',
        limit: -1,
      }),
    ).resolves.toEqual([]);
  });

  it('filters workspace action journal rows by thread, kind, operation, and policy decision', async () => {
    const harness = store.stores.harness;
    expect(harness).toBeDefined();

    await harness!.saveSession(createSampleSessionRecord(), { ownerId: 'h-1', ifVersion: 0 });
    await harness!.saveSession(createSampleSessionRecord({ harnessName: 'other-harness' }), {
      ownerId: 'h-1',
      ifVersion: 0,
    });

    await expect(
      harness!.appendWorkspaceActionJournalEntry(
        sampleWorkspaceActionJournalEntry({ id: 'wrong-thread', threadId: 'thread-2' }),
      ),
    ).resolves.toEqual({ created: false });
    await harness!.appendWorkspaceActionJournalEntry(sampleWorkspaceActionJournalEntry({ id: 'a', createdAt: 1000 }));
    await harness!.appendWorkspaceActionJournalEntry(
      sampleWorkspaceActionJournalEntry({
        id: 'b',
        operation: 'read',
        action: { kind: 'file', operation: 'read', path: 'notes.md' },
        policyDecision: 'allow',
        createdAt: 1000,
      }),
    );
    await harness!.appendWorkspaceActionJournalEntry(
      sampleWorkspaceActionJournalEntry({
        id: 'c',
        actionKind: 'command',
        operation: 'run',
        action: { kind: 'command', operation: 'run', command: 'pnpm test' },
        policyDecision: 'deny',
        path: undefined,
        createdAt: 1100,
      }),
    );
    await harness!.appendWorkspaceActionJournalEntry(
      sampleWorkspaceActionJournalEntry({
        id: 'd',
        actionKind: 'mcp',
        operation: 'call',
        action: { kind: 'mcp', operation: 'call', serverKey: 'filesystem' },
        policyDecision: 'allow',
        path: undefined,
        createdAt: 1200,
      }),
    );
    await harness!.appendWorkspaceActionJournalEntry(
      sampleWorkspaceActionJournalEntry({
        id: 'e',
        actionKind: 'network',
        operation: 'fetch',
        action: { kind: 'network', operation: 'fetch', url: 'https://example.test' },
        path: undefined,
        createdAt: 1300,
      }),
    );
    await harness!.appendWorkspaceActionJournalEntry(
      sampleWorkspaceActionJournalEntry({
        harnessName: 'other-harness',
        id: 'other-namespace',
        createdAt: 900,
      }),
    );

    const listIds = async (
      overrides: Partial<Parameters<typeof harness.listWorkspaceActionJournalEntries>[0]>,
    ): Promise<string[]> =>
      (
        await harness!.listWorkspaceActionJournalEntries({
          sessionId: 'session-1',
          resourceId: 'resource-1',
          limit: 10,
          ...overrides,
        })
      ).map(entry => entry.id);

    await expect(listIds({ threadId: 'thread-1' })).resolves.toEqual(['a', 'b', 'c', 'd', 'e']);
    await expect(listIds({ threadId: 'thread-2' })).resolves.toEqual([]);
    await expect(listIds({ sessionId: 'other-session' })).resolves.toEqual([]);
    await expect(listIds({ resourceId: 'other-resource' })).resolves.toEqual([]);
    await expect(listIds({ actionKind: 'file' })).resolves.toEqual(['a', 'b']);
    await expect(listIds({ operation: 'write' })).resolves.toEqual(['a']);
    await expect(listIds({ policyDecision: 'ask' })).resolves.toEqual(['a', 'e']);
    await expect(listIds({ actionKind: 'mcp', policyDecision: 'allow' })).resolves.toEqual(['d']);
    await expect(listIds({ actionKind: 'command', operation: 'run', policyDecision: 'deny' })).resolves.toEqual(['c']);
    await expect(listIds({ actionKind: 'file', after: { createdAt: 1000, id: 'a' } })).resolves.toEqual(['b']);
    await expect(listIds({ harnessName: 'other-harness' })).resolves.toEqual(['other-namespace']);
  });

  it('round-trips workspace action journal observability correlation fields', async () => {
    const harness = store.stores.harness;
    expect(harness).toBeDefined();

    await harness!.saveSession(createSampleSessionRecord(), { ownerId: 'h-1', ifVersion: 0 });

    await harness!.appendWorkspaceActionJournalEntry(
      sampleWorkspaceActionJournalEntry({
        id: 'with-span',
        traceId: 'trace-1',
        spanId: 'span-1',
      }),
    );
    await harness!.appendWorkspaceActionJournalEntry(
      sampleWorkspaceActionJournalEntry({
        id: 'other-span',
        traceId: 'trace-2',
        spanId: 'span-2',
      }),
    );

    await expect(
      harness!.listWorkspaceActionJournalEntries({
        sessionId: 'session-1',
        resourceId: 'resource-1',
        traceId: 'trace-1',
        spanId: 'span-1',
        limit: 10,
      }),
    ).resolves.toEqual([
      expect.objectContaining({
        id: 'with-span',
        requestId: 'request-1',
        traceId: 'trace-1',
        spanId: 'span-1',
      }),
    ]);
    await expect(
      harness!.listWorkspaceActionJournalEntries({
        sessionId: 'session-1',
        resourceId: 'resource-1',
        traceId: 'trace-1',
        limit: 10,
      }),
    ).resolves.toEqual([expect.objectContaining({ id: 'with-span' })]);
    await expect(
      harness!.listWorkspaceActionJournalEntries({
        sessionId: 'session-1',
        resourceId: 'resource-1',
        traceId: 'trace-2',
        spanId: 'span-2',
        limit: 10,
      }),
    ).resolves.toEqual([expect.objectContaining({ id: 'other-span' })]);
    await expect(
      harness!.listWorkspaceActionJournalEntries({
        sessionId: 'session-1',
        resourceId: 'resource-1',
        spanId: 'span-2',
        limit: 10,
      }),
    ).rejects.toThrow('spanId filter requires traceId');
    await expect(
      harness!.appendWorkspaceActionJournalEntry(
        sampleWorkspaceActionJournalEntry({
          id: 'invalid-span',
          spanId: 'span-without-trace',
        }),
      ),
    ).rejects.toThrow('spanId requires traceId');
  });

  it('widens existing workspace action journal tables with observability columns', async () => {
    const schemaName = `pf_593_workspace_actions_${randomUUID().replaceAll('-', '_')}`;
    await store.db.none(`CREATE SCHEMA "${schemaName}"`);
    try {
      await store.db.none(`CREATE TABLE "${schemaName}"."${TABLE_HARNESS_WORKSPACE_ACTIONS}" (
        id TEXT NOT NULL,
        harness_name TEXT NOT NULL,
        session_id TEXT NOT NULL,
        resource_id TEXT NOT NULL,
        thread_id TEXT NOT NULL,
        action_kind TEXT NOT NULL,
        operation TEXT,
        action JSONB NOT NULL,
        policy_decision TEXT NOT NULL,
        policy_reasons JSONB NOT NULL,
        matched_rules JSONB NOT NULL,
        path JSONB,
        to_path JSONB,
        cwd JSONB,
        actor JSONB,
        request_id TEXT,
        result JSONB,
        created_at BIGINT NOT NULL,
        PRIMARY KEY (harness_name, session_id, id)
      )`);
      const harness = new HarnessPG({ client: store.db, schemaName });
      await harness.init();
      await harness.saveSession(createSampleSessionRecord(), { ownerId: 'h-1', ifVersion: 0 });
      await expect(
        harness.appendWorkspaceActionJournalEntry(
          sampleWorkspaceActionJournalEntry({
            id: 'with-span',
            traceId: 'trace-1',
            spanId: 'span-1',
          }),
        ),
      ).resolves.toEqual({ created: true });
      await expect(
        harness.listWorkspaceActionJournalEntries({
          sessionId: 'session-1',
          resourceId: 'resource-1',
          traceId: 'trace-1',
          limit: 10,
        }),
      ).resolves.toEqual([expect.objectContaining({ id: 'with-span', spanId: 'span-1' })]);
    } finally {
      await store.db.none(`DROP SCHEMA IF EXISTS "${schemaName}" CASCADE`);
    }
  });

  it('filters workspace action journal rows by request and affected path', async () => {
    const harness = store.stores.harness;
    expect(harness).toBeDefined();

    await harness!.saveSession(createSampleSessionRecord(), { ownerId: 'h-1', ifVersion: 0 });

    await harness!.appendWorkspaceActionJournalEntry(
      sampleWorkspaceActionJournalEntry({
        id: 'write-readme',
        requestId: 'turn-1',
        path: {
          rootId: 'project',
          rootPath: '/workspace',
          path: '/workspace/README.md',
          relativePath: 'README.md',
        },
        action: { kind: 'file', operation: 'write', path: 'README.md' },
        createdAt: 1000,
      }),
    );
    await harness!.appendWorkspaceActionJournalEntry(
      sampleWorkspaceActionJournalEntry({
        id: 'rename-source',
        requestId: 'turn-1',
        operation: 'rename',
        action: { kind: 'file', operation: 'rename', path: 'src/old.ts', toPath: 'src/new.ts' },
        path: {
          rootId: 'project',
          rootPath: '/workspace',
          path: '/workspace/src/old.ts',
          relativePath: 'src/old.ts',
        },
        toPath: {
          rootId: 'project',
          rootPath: '/workspace',
          path: '/workspace/src/new.ts',
          relativePath: 'src/new.ts',
        },
        createdAt: 1100,
      }),
    );
    await harness!.appendWorkspaceActionJournalEntry(
      sampleWorkspaceActionJournalEntry({
        id: 'write-docs-readme',
        requestId: 'turn-2',
        path: {
          rootId: 'project',
          rootPath: '/workspace',
          path: '/workspace/docs/README.md',
          relativePath: 'docs/README.md',
        },
        action: { kind: 'file', operation: 'write', path: 'docs/README.md' },
        createdAt: 1200,
      }),
    );
    await harness!.appendWorkspaceActionJournalEntry(
      sampleWorkspaceActionJournalEntry({
        id: 'run-command',
        requestId: 'turn-1',
        actionKind: 'command',
        operation: 'run',
        action: { kind: 'command', operation: 'run', command: 'pnpm test' },
        path: undefined,
        createdAt: 1300,
      }),
    );

    type HarnessJournalListInput = Parameters<NonNullable<typeof harness>['listWorkspaceActionJournalEntries']>[0];
    const listIds = async (overrides: Partial<HarnessJournalListInput>): Promise<string[]> =>
      (
        await harness!.listWorkspaceActionJournalEntries({
          sessionId: 'session-1',
          resourceId: 'resource-1',
          limit: 10,
          ...overrides,
        })
      ).map(entry => entry.id);

    await expect(listIds({ requestId: 'turn-1' })).resolves.toEqual(['write-readme', 'rename-source', 'run-command']);
    await expect(listIds({ requestId: 'turn-2', affectedPath: { relativePath: 'docs/README.md' } })).resolves.toEqual([
      'write-docs-readme',
    ]);
    await expect(listIds({ affectedPath: { rootId: 'project', relativePath: 'README.md' } })).resolves.toEqual([
      'write-readme',
    ]);
    await expect(listIds({ affectedPath: { path: '/workspace/src/old.ts' } })).resolves.toEqual(['rename-source']);
    await expect(listIds({ affectedPath: { relativePath: 'src/new.ts' } })).resolves.toEqual([]);
    await expect(listIds({ affectedPath: { relativePath: 'src/new.ts', includeToPath: true } })).resolves.toEqual([
      'rename-source',
    ]);
    await expect(listIds({ affectedPath: { rootId: 'other', relativePath: 'README.md' } })).resolves.toEqual([]);
    await expect(listIds({ affectedPath: {} })).resolves.toEqual([]);
    await expect(listIds({ affectedPath: { includeToPath: true } })).resolves.toEqual([]);
  });

  it('ignores duplicate or mismatched workspace action journal appends and deletes rows with the session', async () => {
    const harness = store.stores.harness;
    expect(harness).toBeDefined();

    await harness!.saveSession(createSampleSessionRecord({ closedAt: 2000, lastActivityAt: 2000 }), {
      ownerId: 'h-1',
      ifVersion: 0,
    });

    await expect(
      harness!.appendWorkspaceActionJournalEntry(sampleWorkspaceActionJournalEntry({ id: 'entry-1' })),
    ).resolves.toEqual({ created: true });
    await expect(
      harness!.appendWorkspaceActionJournalEntry(
        sampleWorkspaceActionJournalEntry({ id: 'entry-1', result: { status: 'changed' } }),
      ),
    ).resolves.toEqual({ created: false });
    await expect(
      harness!.appendWorkspaceActionJournalEntry(
        sampleWorkspaceActionJournalEntry({ id: 'wrong-resource', resourceId: 'other-resource' }),
      ),
    ).resolves.toEqual({ created: false });

    await expect(
      harness!.listWorkspaceActionJournalEntries({ sessionId: 'session-1', resourceId: 'resource-1', limit: 10 }),
    ).resolves.toEqual([expect.objectContaining({ id: 'entry-1', result: { status: 'ok' } })]);

    const closed = await harness!.loadSession({ sessionId: 'session-1' });
    if (!closed) throw new Error('expected closed session');
    await harness!.deleteSession({
      sessionId: 'session-1',
      ifVersion: closed.version,
      expectedResourceId: closed.resourceId,
      expectedThreadId: closed.threadId,
      expectedParentSessionId: closed.parentSessionId ?? null,
      expectedCreatedAt: closed.createdAt,
      requireClosed: true,
    });

    await expect(
      harness!.listWorkspaceActionJournalEntries({ sessionId: 'session-1', resourceId: 'resource-1', limit: 10 }),
    ).resolves.toEqual([]);
  });

  it('keeps §15 attachment-reference admission atomic and delete-guarded', async () => {
    const harness = store.stores.harness;
    expect(harness).toBeDefined();

    await harness!.saveSession(createSampleSessionRecord(), { ownerId: 'h', ifVersion: 0 });
    const initial = await harness!.loadSession({ sessionId: 'session-1' });
    if (!initial) throw new Error('expected session');

    await harness!.saveAttachment({
      sessionId: 'session-1',
      attachmentId: 'attachment-1',
      name: 'note.txt',
      mimeType: 'text/plain',
      source: 'preupload',
      data: new Uint8Array([1, 2, 3]),
    });
    await expect(
      harness!.saveSessionWithAttachmentReferences(
        { ...initial, state: { admitted: 'missing-ref' } },
        { ownerId: 'h', ifVersion: initial.version },
        [
          { sessionId: 'session-1', attachmentId: 'attachment-1', source: 'queued_item', sourceId: 'queued-valid' },
          { sessionId: 'session-1', attachmentId: 'missing', source: 'queued_item', sourceId: 'queued-missing' },
        ],
      ),
    ).rejects.toBeInstanceOf(HarnessStorageAttachmentUnavailableError);
    await expect(harness!.loadSession({ sessionId: 'session-1' })).resolves.toMatchObject({
      version: initial.version,
      state: {},
    });
    await expect(
      harness!.listAttachmentReferences({ sessionId: 'session-1', attachmentId: 'attachment-1' }),
    ).resolves.toEqual([]);

    const saved = await harness!.saveSessionWithAttachmentReferences(
      { ...initial, state: { admitted: 'queued-ref' } },
      { ownerId: 'h', ifVersion: initial.version },
      [{ sessionId: 'session-1', attachmentId: 'attachment-1', source: 'queued_item', sourceId: 'queued-1' }],
    );

    expect(saved.version).toBe(initial.version + 1);
    await expect(
      harness!.listAttachmentReferences({ sessionId: 'session-1', attachmentId: 'attachment-1' }),
    ).resolves.toEqual([{ source: 'queued_item', sourceId: 'queued-1' }]);
    await expect(
      harness!.deleteAttachment({ sessionId: 'session-1', attachmentId: 'attachment-1' }),
    ).rejects.toBeInstanceOf(HarnessStorageAttachmentInUseError);
    await expect(
      harness!.loadAttachment({ sessionId: 'session-1', attachmentId: 'attachment-1' }),
    ).resolves.toMatchObject({
      name: 'note.txt',
      data: new Uint8Array([1, 2, 3]),
    });
  });

  it('stores message result evidence and blocks active admission behind delete fences', async () => {
    const harness = store.stores.harness;
    expect(harness).toBeDefined();

    await harness!.writeMessageResultEvidence({
      harnessName: 'default',
      sessionId: 'session-1',
      resourceId: 'resource-1',
      threadId: 'thread-1',
      signalId: 'signal-1',
      runId: 'run-1',
      modeId: 'mode-1',
      modelId: 'model-1',
      admissionId: 'admission-1',
      admissionHash: 'hash-1',
      status: 'completed',
      result: { ok: true },
      createdAt: 1,
      updatedAt: 2,
    });

    await expect(
      harness!.loadMessageResultEvidence({
        sessionId: 'session-1',
        resourceId: 'resource-1',
        threadId: 'thread-1',
        signalId: 'signal-1',
      }),
    ).resolves.toMatchObject({
      status: 'completed',
      runId: 'run-1',
      modeId: 'mode-1',
      modelId: 'model-1',
      result: { ok: true },
    });

    await harness!.withThreadDeleteFence({ threadId: 'thread-1', ownerId: 'deleter', ttlMs: 30_000 }, async () => {
      await expect(
        harness!.createOrLoadActiveSession(createSampleSessionRecord(), {
          initialLease: { ownerId: 'harness-worker', ttlMs: 30_000 },
        }),
      ).rejects.toBeInstanceOf(HarnessStorageThreadDeleteFenceConflictError);
    });
  });

  it('atomically claims channel inbox and wakeup rows with PG claim metadata', async () => {
    const harness = store.stores.harness;
    expect(harness).toBeDefined();
    const now = Date.now();

    await harness!.saveChannelInboxItem({
      id: 'inbox-1',
      harnessName: 'default',
      channelId: 'slack',
      providerId: 'slack',
      idempotencyKey: 'provider-event-1',
      payloadHash: 'payload-hash-1',
      admissionId: 'admission-1',
      externalMessageId: 'external-1',
      receivedAt: now,
      updatedAt: now,
      status: 'received',
      attempts: 0,
      requestContext: {},
      content: 'hello',
      attachments: [],
    });
    await harness!.createOrLoadHarnessWakeupItem({
      id: 'wakeup-1',
      harnessName: 'default',
      source: 'schedule',
      sourceId: 'schedule-1',
      fireId: 'fire-1',
      idempotencyKey: 'wakeup-key-1',
      payloadHash: 'payload-hash-1',
      admissionId: 'admission-1',
      dueAt: now - 1,
      createdAt: now,
      updatedAt: now,
      status: 'due',
      yolo: true,
      attempts: 0,
      content: 'wake up',
      attachments: [],
    });

    await expect(
      harness!.claimChannelInboxItems({
        harnessName: 'default',
        statuses: ['received'],
        claimId: 'claim-inbox',
        limit: 5,
        now,
        claimTtlMs: 30_000,
      }),
    ).resolves.toEqual([expect.objectContaining({ id: 'inbox-1', claimId: 'claim-inbox' })]);
    await expect(
      harness!.claimHarnessWakeupItems({
        harnessName: 'default',
        statuses: ['due'],
        claimId: 'claim-wakeup',
        limit: 5,
        now,
        claimTtlMs: 30_000,
      }),
    ).resolves.toEqual([
      expect.objectContaining({
        id: 'wakeup-1',
        status: 'claimed',
        claimId: 'claim-wakeup',
        attempts: 1,
        yolo: true,
      }),
    ]);
  });

  it('lists resource and session scoped channel diagnostics rows', async () => {
    const harness = store.stores.harness;
    expect(harness).toBeDefined();

    await harness!.saveChannelInboxItem({
      id: 'inbox-root',
      harnessName: 'default',
      channelId: 'support',
      providerId: 'slack',
      idempotencyKey: 'event-root',
      payloadHash: 'payload-hash-root',
      admissionId: 'admission-root',
      resourceId: 'resource-1',
      threadId: 'thread-1',
      sessionId: 'session-1',
      externalMessageId: 'message-root',
      receivedAt: 1000,
      updatedAt: 1000,
      status: 'received',
      attempts: 0,
      requestContext: {},
      content: 'hello',
      attachments: [],
    });
    await harness!.saveChannelInboxItem({
      id: 'inbox-hidden',
      harnessName: 'default',
      channelId: 'support',
      providerId: 'slack',
      idempotencyKey: 'event-hidden',
      payloadHash: 'payload-hash-hidden',
      admissionId: 'admission-hidden',
      resourceId: 'resource-2',
      threadId: 'thread-1',
      sessionId: 'session-1',
      externalMessageId: 'message-hidden',
      receivedAt: 1000,
      updatedAt: 1000,
      status: 'received',
      attempts: 0,
      requestContext: {},
      content: 'hello',
      attachments: [],
    });
    await harness!.createOrLoadChannelActionToken({
      actionTokenId: 'action-token-1',
      harnessName: 'default',
      channelId: 'support',
      providerId: 'slack',
      resourceId: 'resource-1',
      owningSessionId: 'child-1',
      itemId: 'question-1',
      kind: 'question',
      bindingId: 'binding-1',
      bindingGeneration: 1,
      runId: 'run-1',
      pendingRequestedAt: 1000,
      audience: {},
      metadataHash: 'metadata-hash-1',
      transportHash: 'transport-hash-1',
      keyId: 'key-1',
      createdAt: 1000,
      updatedAt: 1000,
    });
    await harness!.createOrLoadChannelActionReceipt({
      id: 'receipt-1',
      harnessName: 'default',
      channelId: 'support',
      providerId: 'slack',
      actionTokenId: 'action-token-1',
      actionId: 'provider-action-1',
      bindingId: 'binding-1',
      bindingGeneration: 1,
      resourceId: 'resource-1',
      owningSessionId: 'child-1',
      itemId: 'question-1',
      kind: 'question',
      runId: 'run-1',
      pendingRequestedAt: 1000,
      audience: {},
      responseHash: 'response-hash-1',
      response: { answer: 'approved' },
      status: 'received',
      attempts: 0,
      createdAt: 1000,
      updatedAt: 1000,
    });
    await harness!.enqueueChannelOutbox({
      id: 'outbox-owned-child',
      harnessName: 'default',
      channelId: 'support',
      providerId: 'slack',
      bindingId: 'binding-1',
      bindingGeneration: 1,
      idempotencyKey: 'outbox-owned-child',
      payloadHash: 'payload-hash-1',
      resourceId: 'resource-1',
      threadId: 'thread-1',
      owningSessionId: 'child-1',
      target: {
        platform: 'slack',
        externalTenantId: 'tenant-1',
        externalChannelId: 'channel-1',
        externalThreadId: 'thread-ext-1',
      },
      kind: 'assistant-message',
      operationKind: 'message-create',
      payload: { text: 'hello' },
      deliverySemantics: 'native-idempotency',
      status: 'pending',
      attempts: 0,
      createdAt: 1000,
      updatedAt: 1000,
    });

    const rows = await harness!.listChannelDiagnosticsRows({
      harnessName: 'default',
      resourceId: 'resource-1',
      sessionIds: ['session-1', 'child-1'],
      limit: 10,
    });

    expect(rows.inbox.map(row => row.id)).toEqual(['inbox-root']);
    expect(rows.actionTokens.map(row => row.actionTokenId)).toEqual(['action-token-1']);
    expect(rows.actionReceipts.map(row => row.id)).toEqual(['receipt-1']);
    expect(rows.outbox.map(row => row.id)).toEqual(['outbox-owned-child']);
    await expect(
      harness!.listChannelDiagnosticsRows({
        harnessName: 'default',
        resourceId: 'resource-1',
        sessionIds: [],
        limit: 10,
      }),
    ).resolves.toEqual({ inbox: [], actionTokens: [], actionReceipts: [], outbox: [] });
    await expect(
      harness!.listChannelDiagnosticsRows({
        harnessName: 'default',
        resourceId: 'resource-1',
        sessionIds: ['session-1'],
        limit: 0,
      }),
    ).resolves.toEqual({ inbox: [], actionTokens: [], actionReceipts: [], outbox: [] });
  });

  it('replay state reflects single-epoch seq bounds unchanged', async () => {
    const harness = store.stores.harness;
    await harness!.saveSession(createSampleSessionRecord(), { ownerId: 'h-1', ifVersion: 0 });
    await harness!.appendSessionEvent({
      harnessName: 'default',
      sessionId: 'session-1',
      resourceId: 'resource-1',
      threadId: 'thread-1',
      eventId: 'harness-v1:epoch-1:1',
      epoch: 'epoch-1',
      sequence: 1,
      event: { type: 'app.event', id: 'harness-v1:epoch-1:1', timestamp: 1000 },
      emittedAt: 1000,
      storedAt: 1001,
    });
    await harness!.appendSessionEvent({
      harnessName: 'default',
      sessionId: 'session-1',
      resourceId: 'resource-1',
      threadId: 'thread-1',
      eventId: 'harness-v1:epoch-1:2',
      epoch: 'epoch-1',
      sequence: 2,
      event: { type: 'app.event', id: 'harness-v1:epoch-1:2', timestamp: 1002 },
      emittedAt: 1002,
      storedAt: 1003,
    });

    await expect(
      harness!.getSessionEventReplayState({
        harnessName: 'default',
        sessionId: 'session-1',
        resourceId: 'resource-1',
        threadId: 'thread-1',
      }),
    ).resolves.toEqual({ epoch: 'epoch-1', oldestSequence: 1, newestSequence: 2 });
  });

  it('collapses to null on a multi-epoch ledger, matching the authoritative in-memory adapter', async () => {
    // §10.5: replay state is anchored to the CURRENT in-memory Session
    // instance's epoch, which storage cannot observe. A multi-epoch ledger is
    // ambiguous, so storage returns null; the server then 412s the stale cursor
    // and the client recovers via the snapshot path. The in-memory reference
    // (packages/core/.../inmemory.ts: `if (epochs.size !== 1) return null;`)
    // returns null for the identical input, so pg MUST too.
    const harness = store.stores.harness;
    await harness!.saveSession(createSampleSessionRecord(), { ownerId: 'h-1', ifVersion: 0 });
    await harness!.appendSessionEvent({
      harnessName: 'default',
      sessionId: 'session-1',
      resourceId: 'resource-1',
      threadId: 'thread-1',
      eventId: 'harness-v1:epoch-prior:1',
      epoch: 'epoch-prior',
      sequence: 1,
      event: { type: 'agent_start', id: 'harness-v1:epoch-prior:1', timestamp: 1000 },
      emittedAt: 1000,
      storedAt: 1000,
    });
    await harness!.appendSessionEvent({
      harnessName: 'default',
      sessionId: 'session-1',
      resourceId: 'resource-1',
      threadId: 'thread-1',
      eventId: 'harness-v1:epoch-prior:2',
      epoch: 'epoch-prior',
      sequence: 2,
      event: { type: 'agent_end', id: 'harness-v1:epoch-prior:2', timestamp: 1010 },
      emittedAt: 1010,
      storedAt: 1010,
    });
    // Fresh, chronologically-newest epoch after a rehydrate.
    await harness!.appendSessionEvent({
      harnessName: 'default',
      sessionId: 'session-1',
      resourceId: 'resource-1',
      threadId: 'thread-1',
      eventId: 'harness-v1:aFresh:0',
      epoch: 'aFresh',
      sequence: 0,
      event: { type: 'agent_start', id: 'harness-v1:aFresh:0', timestamp: 2000 },
      emittedAt: 2000,
      storedAt: 2000,
    });

    await expect(
      harness!.getSessionEventReplayState({
        harnessName: 'default',
        sessionId: 'session-1',
        resourceId: 'resource-1',
        threadId: 'thread-1',
      }),
    ).resolves.toBeNull();
  });

  it('still collapses to null when the prior epoch and fresh epoch share a stored_at millisecond', async () => {
    // Edge case: stored_at is `Date.now()` and same-ms cross-epoch collisions are
    // routine; a stored_at/sequence tiebreak could otherwise misidentify the
    // "newest" epoch. Collapsing to null is independent of stored_at ordering,
    // so the ambiguous ledger stays null regardless of the clock collision.
    const harness = store.stores.harness;
    await harness!.saveSession(createSampleSessionRecord(), { ownerId: 'h-1', ifVersion: 0 });
    await harness!.appendSessionEvent({
      harnessName: 'default',
      sessionId: 'session-1',
      resourceId: 'resource-1',
      threadId: 'thread-1',
      eventId: 'harness-v1:epoch-prior:5',
      epoch: 'epoch-prior',
      sequence: 5,
      event: { type: 'agent_end', id: 'harness-v1:epoch-prior:5', timestamp: 1500 },
      emittedAt: 1500,
      storedAt: 1500,
    });
    // Fresh epoch's seq-0 event lands in the SAME millisecond as the prior
    // epoch's higher-sequence row.
    await harness!.appendSessionEvent({
      harnessName: 'default',
      sessionId: 'session-1',
      resourceId: 'resource-1',
      threadId: 'thread-1',
      eventId: 'harness-v1:aFresh:0',
      epoch: 'aFresh',
      sequence: 0,
      event: { type: 'agent_start', id: 'harness-v1:aFresh:0', timestamp: 1500 },
      emittedAt: 1500,
      storedAt: 1500,
    });

    await expect(
      harness!.getSessionEventReplayState({
        harnessName: 'default',
        sessionId: 'session-1',
        resourceId: 'resource-1',
        threadId: 'thread-1',
      }),
    ).resolves.toBeNull();
  });
});

function sampleChannelBinding(overrides: Partial<ChannelBinding> = {}): ChannelBinding {
  return {
    id: 'binding-1',
    harnessName: 'default',
    channelId: 'support',
    providerId: 'slack',
    status: 'active',
    platform: 'slack',
    externalTenantId: 'T1',
    externalChannelId: 'C1',
    externalThreadId: 'th-1',
    resourceId: 'resource-1',
    threadId: 'thread-1',
    sessionId: 'session-1',
    mode: 'per-user-resource',
    generation: 1,
    createdAt: 1000,
    updatedAt: 1000,
    ...overrides,
  };
}

describe('HarnessPG channel binding ledger (§5.1h / §14.1 / PF-824)', () => {
  const store = new PostgresStore({ ...TEST_CONFIG, id: 'pg-harness-channel-binding-test-store' });

  beforeAll(async () => {
    await store.init();
  });

  beforeEach(async () => {
    await store.stores.harness?.dangerouslyClearAll();
  });

  afterAll(async () => {
    await store.stores.harness?.dangerouslyClearAll().catch(() => {});
    await store.close();
  });

  it('saves a binding and loads it back by external tuple', async () => {
    const harness = store.stores.harness!;
    const binding = sampleChannelBinding();
    await harness.saveChannelBinding(binding);

    const byId = await harness.loadChannelBinding({ bindingId: binding.id });
    expect(byId).toEqual(binding);

    const byExternal = await harness.loadChannelBindingByExternal({
      harnessName: 'default',
      channelId: 'support',
      platform: 'slack',
      externalTenantId: 'T1',
      externalChannelId: 'C1',
      externalThreadId: 'th-1',
    });
    expect(byExternal).toEqual(binding);
  });

  it('excludes non-active rows from the external lookup', async () => {
    const harness = store.stores.harness!;
    await harness.saveChannelBinding(sampleChannelBinding({ id: 'fenced', status: 'replaced' }));

    const byExternal = await harness.loadChannelBindingByExternal({
      harnessName: 'default',
      channelId: 'support',
      platform: 'slack',
      externalTenantId: 'T1',
      externalChannelId: 'C1',
      externalThreadId: 'th-1',
    });
    expect(byExternal).toBeNull();

    // The fenced row is still addressable by id.
    const byId = await harness.loadChannelBinding({ bindingId: 'fenced' });
    expect(byId?.status).toBe('replaced');
  });

  it('rejects a second active binding for the same tuple (§5.2h)', async () => {
    const harness = store.stores.harness!;
    await harness.saveChannelBinding(sampleChannelBinding({ id: 'first' }));

    await expect(
      harness.saveChannelBinding(sampleChannelBinding({ id: 'second' })),
    ).rejects.toBeInstanceOf(HarnessStorageChannelBindingConflictError);

    // The conflict names the holder of the active row.
    await harness
      .saveChannelBinding(sampleChannelBinding({ id: 'third' }))
      .catch((err: HarnessStorageChannelBindingConflictError) => {
        expect(err).toBeInstanceOf(HarnessStorageChannelBindingConflictError);
        expect(err.heldBy).toBe('first');
      });

    // Re-saving the SAME id (an in-place update) is always allowed.
    await expect(
      harness.saveChannelBinding(sampleChannelBinding({ id: 'first', lastInboundAt: 5000 })),
    ).resolves.toBeUndefined();
    const reloaded = await harness.loadChannelBinding({ bindingId: 'first' });
    expect(reloaded?.lastInboundAt).toBe(5000);
  });

  it('treats a missing optional external id as a sentinel match (§14.1)', async () => {
    const harness = store.stores.harness!;
    // Persist a binding with no tenant/channel id, then look it up with the same
    // omitted ids — the sentinel makes them collide, not SQL NULL semantics.
    const binding = sampleChannelBinding({
      id: 'no-optional',
      externalTenantId: undefined,
      externalChannelId: undefined,
    });
    await harness.saveChannelBinding(binding);

    const matched = await harness.loadChannelBindingByExternal({
      harnessName: 'default',
      channelId: 'support',
      platform: 'slack',
      externalThreadId: 'th-1',
    });
    expect(matched).toEqual(binding);

    // A second active binding with the same omitted optional ids still conflicts.
    await expect(
      harness.saveChannelBinding(
        sampleChannelBinding({ id: 'no-optional-2', externalTenantId: undefined, externalChannelId: undefined }),
      ),
    ).rejects.toBeInstanceOf(HarnessStorageChannelBindingConflictError);

    // Supplying a real tenant id is a DIFFERENT tuple, so it does not match/conflict.
    const distinct = await harness.loadChannelBindingByExternal({
      harnessName: 'default',
      channelId: 'support',
      platform: 'slack',
      externalTenantId: 'T1',
      externalThreadId: 'th-1',
    });
    expect(distinct).toBeNull();
  });

  it('keeps an empty-string optional external id distinct from undefined (§14.1)', async () => {
    const harness = store.stores.harness!;
    // The out-of-band U+001F-prefixed sentinel (not '') means an EXPLICIT empty-string
    // tenant id round-trips faithfully and addresses a different tuple than an omitted
    // tenant id.
    const empty = sampleChannelBinding({ id: 'empty-tenant', externalTenantId: '', externalChannelId: '' });
    await harness.saveChannelBinding(empty);

    const reloaded = await harness.loadChannelBinding({ bindingId: 'empty-tenant' });
    expect(reloaded?.externalTenantId).toBe('');
    expect(reloaded?.externalChannelId).toBe('');

    // The empty-string tuple matches an empty-string lookup ...
    const matched = await harness.loadChannelBindingByExternal({
      harnessName: 'default',
      channelId: 'support',
      platform: 'slack',
      externalTenantId: '',
      externalChannelId: '',
      externalThreadId: 'th-1',
    });
    expect(matched?.id).toBe('empty-tenant');

    // ... but an OMITTED optional id is a distinct tuple and must not collide.
    const omitted = await harness.loadChannelBindingByExternal({
      harnessName: 'default',
      channelId: 'support',
      platform: 'slack',
      externalThreadId: 'th-1',
    });
    expect(omitted).toBeNull();
  });

  it('a literal single-space external id does not alias the missing-optional-id sentinel (§14.1)', async () => {
    const harness = store.stores.harness!;
    // Binding A omits the optional external ids (sentinel-normalised).
    await harness.saveChannelBinding(
      sampleChannelBinding({ id: 'missing', externalTenantId: undefined, externalChannelId: undefined }),
    );
    // Binding B uses real single-space external ids — a DIFFERENT tuple that must
    // not alias the missing-id sentinel. Both active rows coexist (no conflict).
    await expect(
      harness.saveChannelBinding(sampleChannelBinding({ id: 'space', externalTenantId: ' ', externalChannelId: ' ' })),
    ).resolves.toBeUndefined();

    // The single-space tuple round-trips faithfully and resolves to its own binding.
    const space = await harness.loadChannelBindingByExternal({
      harnessName: 'default',
      channelId: 'support',
      platform: 'slack',
      externalTenantId: ' ',
      externalChannelId: ' ',
      externalThreadId: 'th-1',
    });
    expect(space?.id).toBe('space');
    expect(space?.externalTenantId).toBe(' ');
    expect(space?.externalChannelId).toBe(' ');

    // The missing-id tuple resolves to the missing-id binding only — no aliasing.
    const missing = await harness.loadChannelBindingByExternal({
      harnessName: 'default',
      channelId: 'support',
      platform: 'slack',
      externalThreadId: 'th-1',
    });
    expect(missing?.id).toBe('missing');
  });

  it('advances inbound activity forward-only (§14.1)', async () => {
    const harness = store.stores.harness!;
    await harness.saveChannelBinding(sampleChannelBinding({ updatedAt: 1000 }));

    const forward = await harness.touchChannelBindingInbound({ bindingId: 'binding-1', at: 5000 });
    expect(forward?.lastInboundAt).toBe(5000);
    expect(forward?.updatedAt).toBe(5000);

    // An older ingress must NOT regress the marker.
    const backward = await harness.touchChannelBindingInbound({ bindingId: 'binding-1', at: 2000 });
    expect(backward?.lastInboundAt).toBe(5000);
    expect(backward?.updatedAt).toBe(5000);

    // A newer ingress advances it again.
    const newer = await harness.touchChannelBindingInbound({ bindingId: 'binding-1', at: 9000 });
    expect(newer?.lastInboundAt).toBe(9000);

    // Touching an unknown binding returns null.
    expect(await harness.touchChannelBindingInbound({ bindingId: 'missing', at: 1 })).toBeNull();
  });

  it('resolves idempotently and fences the prior active binding on replacement (§14.1)', async () => {
    const harness = store.stores.harness!;
    const first = await harness.resolveChannelBinding({ candidate: sampleChannelBinding({ id: 'gen-1' }) });
    expect(first).toEqual({ binding: expect.objectContaining({ id: 'gen-1', generation: 1 }), created: true });

    // No replacement: the existing active binding wins (idempotent), generation stays.
    const again = await harness.resolveChannelBinding({ candidate: sampleChannelBinding({ id: 'gen-1-dup' }) });
    expect(again.created).toBe(false);
    expect(again.binding.id).toBe('gen-1');

    // Replacement fences the prior active binding and bumps the generation.
    const replaced = await harness.resolveChannelBinding({
      candidate: sampleChannelBinding({ id: 'gen-2', updatedAt: 2000 }),
      replaceBindingId: 'gen-1',
    });
    expect(replaced.created).toBe(true);
    expect(replaced.binding.generation).toBe(2);
    expect(replaced.replacedBindingId).toBe('gen-1');

    const fenced = await harness.loadChannelBinding({ bindingId: 'gen-1' });
    expect(fenced?.status).toBe('replaced');
    expect(fenced?.replacedByBindingId).toBe('gen-2');

    // Exactly one active binding remains for the tuple.
    const active = await harness.loadChannelBindingByExternal({
      harnessName: 'default',
      channelId: 'support',
      platform: 'slack',
      externalTenantId: 'T1',
      externalChannelId: 'C1',
      externalThreadId: 'th-1',
    });
    expect(active?.id).toBe('gen-2');
  });

  it('lists active bindings for a scope ordered by activity then id, with cursor paging (§14.8)', async () => {
    const harness = store.stores.harness!;
    await harness.saveChannelBinding(
      sampleChannelBinding({ id: 'a', externalThreadId: 'th-a', lastOutboundAt: 100 }),
    );
    await harness.saveChannelBinding(
      sampleChannelBinding({ id: 'b', externalThreadId: 'th-b', lastOutboundAt: 300 }),
    );
    await harness.saveChannelBinding(
      sampleChannelBinding({ id: 'c', externalThreadId: 'th-c', lastInboundAt: 200 }),
    );

    const page1 = await harness.listActiveChannelBindingsForScope({ harnessName: 'default', limit: 2 });
    expect(page1.bindings.map(b => b.id)).toEqual(['b', 'c']);
    expect(page1.nextCursor).toBe('c');

    const page2 = await harness.listActiveChannelBindingsForScope({
      harnessName: 'default',
      limit: 2,
      cursor: page1.nextCursor,
    });
    expect(page2.bindings.map(b => b.id)).toEqual(['a']);
    expect(page2.nextCursor).toBeUndefined();

    // An unknown cursor pages to the end of the set rather than restarting.
    const stale = await harness.listActiveChannelBindingsForScope({
      harnessName: 'default',
      limit: 2,
      cursor: 'ghost',
    });
    expect(stale.bindings).toEqual([]);
  });

  it('removes the binding on deleteSessions and frees the tuple for rebind', async () => {
    const harness = store.stores.harness!;
    await harness.saveSession(createSampleSessionRecord({ id: 'session-1' }), { ownerId: 'h-1', ifVersion: 0 });
    const seeded = await harness.loadSession({ sessionId: 'session-1' });
    if (!seeded) throw new Error('expected seeded session');
    await harness.saveChannelBinding(sampleChannelBinding({ sessionId: 'session-1' }));

    await harness.deleteSessions({
      sessions: [
        {
          sessionId: 'session-1',
          ifVersion: seeded.version,
          expectedResourceId: seeded.resourceId,
          expectedThreadId: seeded.threadId,
          expectedCreatedAt: seeded.createdAt,
        },
      ],
    });

    // The session-scoped binding is gone.
    expect(await harness.loadChannelBinding({ bindingId: 'binding-1' })).toBeNull();
    expect(await harness.listChannelBindingsForSession({ sessionId: 'session-1' })).toEqual([]);

    // The conversation tuple is free, so a replacement session can rebind it.
    await expect(
      harness.saveChannelBinding(sampleChannelBinding({ id: 'rebind', sessionId: 'session-2' })),
    ).resolves.toBeUndefined();
    const rebound = await harness.loadChannelBindingByExternal({
      harnessName: 'default',
      channelId: 'support',
      platform: 'slack',
      externalTenantId: 'T1',
      externalChannelId: 'C1',
      externalThreadId: 'th-1',
    });
    expect(rebound?.id).toBe('rebind');
  });
});

function sampleWorkspaceActionJournalEntry(
  overrides: Partial<WorkspaceActionJournalEntry> = {},
): WorkspaceActionJournalEntry {
  return {
    id: 'workspace-action-1',
    harnessName: 'default',
    sessionId: 'session-1',
    resourceId: 'resource-1',
    threadId: 'thread-1',
    actionKind: 'file',
    operation: 'write',
    action: { kind: 'file', operation: 'write', path: 'notes.md' },
    policyDecision: 'ask',
    policyReasons: ['workspace.default_ask'],
    matchedRules: [],
    path: {
      rootId: 'project',
      rootPath: '/workspace',
      path: '/workspace/notes.md',
      relativePath: 'notes.md',
    },
    actor: { type: 'user', id: 'user-1' },
    requestId: 'request-1',
    result: { status: 'ok' },
    createdAt: 1000,
    ...overrides,
  };
}

function sampleProviderCallbackBinding(
  overrides: Partial<HarnessProviderCallbackBinding> = {},
): HarnessProviderCallbackBinding {
  return {
    id: 'callback-binding-1',
    providerId: 'slack',
    selectorKind: 'installation',
    selectorValue: 'installation-1',
    harnessName: 'default',
    channelId: 'support',
    origin: { route: 'support-events' },
    status: 'active',
    createdAt: 1000,
    updatedAt: 1000,
    ...overrides,
  };
}

describe('HarnessPG provider callback binding ledger (§5.1i)', () => {
  const store = new PostgresStore({ ...TEST_CONFIG, id: 'pg-harness-provider-callback-binding-test-store' });

  beforeAll(async () => {
    await store.init();
  });

  beforeEach(async () => {
    await store.stores.harness?.dangerouslyClearAll();
  });

  afterAll(async () => {
    await store.stores.harness?.dangerouslyClearAll().catch(() => {});
    await store.close();
  });

  it('dedupes exact active selector bindings and creates the active-selector indexes', async () => {
    const harness = store.stores.harness!;

    await expect(harness.resolveProviderCallbackBinding(sampleProviderCallbackBinding())).resolves.toMatchObject({
      duplicate: false,
      conflict: false,
      binding: { id: 'callback-binding-1', status: 'active' },
    });
    await expect(
      harness.resolveProviderCallbackBinding(sampleProviderCallbackBinding({ id: 'callback-binding-retry' })),
    ).resolves.toMatchObject({
      duplicate: true,
      conflict: false,
      binding: { id: 'callback-binding-1' },
    });

    const indexes = await store.db.manyOrNone<{ indexname: string; indexdef: string }>(
      `SELECT indexname, indexdef FROM pg_indexes
       WHERE schemaname = 'public'
         AND indexname = ANY($1)
       ORDER BY indexname`,
      [['idx_harness_provider_callback_active_selector', 'idx_harness_provider_callback_selector_status']],
    );
    expect(indexes.map(row => row.indexname)).toEqual([
      'idx_harness_provider_callback_active_selector',
      'idx_harness_provider_callback_selector_status',
    ]);
    // The active-selector index is a PARTIAL UNIQUE index (WHERE status = 'active').
    const activeSelectorDef = indexes.find(
      row => row.indexname === 'idx_harness_provider_callback_active_selector',
    )?.indexdef;
    expect(activeSelectorDef).toMatch(/UNIQUE INDEX/i);
    expect(activeSelectorDef).toMatch(/WHERE.*status.*=.*'active'/i);
  });

  it('reports same selector with a different target as a conflict without retargeting', async () => {
    const harness = store.stores.harness!;
    await harness.resolveProviderCallbackBinding(sampleProviderCallbackBinding());

    await expect(
      harness.resolveProviderCallbackBinding(
        sampleProviderCallbackBinding({
          id: 'callback-binding-2',
          channelId: 'sales',
          origin: { route: 'sales-events' },
        }),
      ),
    ).resolves.toMatchObject({
      duplicate: true,
      conflict: true,
      binding: { id: 'callback-binding-1', channelId: 'support' },
    });
    await expect(
      harness.loadProviderCallbackBindingBySelector({
        providerId: 'slack',
        selectorKind: 'installation',
        selectorValue: 'installation-1',
      }),
    ).resolves.toMatchObject({ id: 'callback-binding-1', channelId: 'support' });
  });

  it('replaces active selector bindings and keeps replaced rows terminal', async () => {
    const harness = store.stores.harness!;
    await harness.resolveProviderCallbackBinding(sampleProviderCallbackBinding());

    await expect(
      harness.resolveProviderCallbackBinding(sampleProviderCallbackBinding(), {
        replaceBindingId: 'callback-binding-1',
      }),
    ).rejects.toBeInstanceOf(HarnessStorageProviderCallbackBindingTransitionError);
    await expect(
      harness.resolveProviderCallbackBinding(
        sampleProviderCallbackBinding({
          id: 'callback-binding-disabled',
          status: 'disabled',
          harnessName: 'support-disabled',
          channelId: 'support-disabled',
          createdAt: 1500,
          updatedAt: 1500,
          origin: { route: 'support-events-disabled' },
        }),
        { replaceBindingId: 'callback-binding-1' },
      ),
    ).rejects.toBeInstanceOf(HarnessStorageProviderCallbackBindingTransitionError);
    await expect(
      harness.loadProviderCallbackBindingBySelector({
        providerId: 'slack',
        selectorKind: 'installation',
        selectorValue: 'installation-1',
      }),
    ).resolves.toMatchObject({ id: 'callback-binding-1', status: 'active' });
    await expect(
      harness.resolveProviderCallbackBinding(
        sampleProviderCallbackBinding({
          id: 'callback-binding-2',
          harnessName: 'support-v2',
          channelId: 'support-v2',
          createdAt: 2000,
          updatedAt: 2000,
          origin: { route: 'support-events-v2' },
        }),
        { replaceBindingId: 'callback-binding-1' },
      ),
    ).resolves.toMatchObject({
      duplicate: false,
      conflict: false,
      replacedBindingId: 'callback-binding-1',
      binding: { id: 'callback-binding-2', harnessName: 'support-v2', status: 'active' },
    });
    await expect(
      harness.loadProviderCallbackBindingBySelector({
        providerId: 'slack',
        selectorKind: 'installation',
        selectorValue: 'installation-1',
      }),
    ).resolves.toMatchObject({ id: 'callback-binding-2' });
    await expect(
      harness.resolveProviderCallbackBinding(
        sampleProviderCallbackBinding({
          id: 'callback-binding-2',
          harnessName: 'support-v2',
          channelId: 'support-v2',
          createdAt: 2000,
          updatedAt: 2000,
          origin: { route: 'support-events-v2' },
        }),
        { replaceBindingId: 'callback-binding-1' },
      ),
    ).resolves.toMatchObject({
      duplicate: true,
      conflict: false,
      replacedBindingId: 'callback-binding-1',
      binding: { id: 'callback-binding-2', status: 'active' },
    });
    await expect(
      harness.markProviderCallbackBindingStatus({ bindingId: 'callback-binding-1', status: 'active', updatedAt: 3000 }),
    ).rejects.toBeInstanceOf(HarnessStorageProviderCallbackBindingTransitionError);
  });

  it('rejects replacement retries when the previous owner was not replaced by the duplicate id', async () => {
    const harness = store.stores.harness!;
    await harness.resolveProviderCallbackBinding(sampleProviderCallbackBinding());
    const stalledReplacement = sampleProviderCallbackBinding({
      id: 'callback-binding-2',
      status: 'disabled',
      harnessName: 'support-v2',
      channelId: 'support-v2',
      createdAt: 2000,
      updatedAt: 2000,
      origin: { route: 'support-events-v2' },
    });
    // Seed a disabled row with the would-be replacement id that the replacement target
    // never transitioned to (replaced_by_binding_id stays NULL), so the unique-conflict
    // resolver must refuse to treat the retry as an idempotent success.
    await store.db.none(
      `INSERT INTO ${TABLE_HARNESS_PROVIDER_CALLBACK_BINDINGS}
            (id, provider_id, selector_kind, selector_value, harness_name, channel_id, origin, status,
             created_at, updated_at, replaced_at, replaced_by_binding_id, last_error)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)`,
      [
        stalledReplacement.id,
        stalledReplacement.providerId,
        stalledReplacement.selectorKind,
        stalledReplacement.selectorValue,
        stalledReplacement.harnessName,
        stalledReplacement.channelId,
        JSON.stringify(stalledReplacement.origin),
        stalledReplacement.status,
        stalledReplacement.createdAt,
        stalledReplacement.updatedAt,
        null,
        null,
        null,
      ],
    );

    await expect(
      harness.resolveProviderCallbackBinding(
        sampleProviderCallbackBinding({
          id: 'callback-binding-2',
          harnessName: 'support-v2',
          channelId: 'support-v2',
          createdAt: 2000,
          updatedAt: 2000,
          origin: { route: 'support-events-v2' },
        }),
        { replaceBindingId: 'callback-binding-1' },
      ),
    ).rejects.toBeInstanceOf(HarnessStorageProviderCallbackBindingTransitionError);
    await expect(
      harness.loadProviderCallbackBindingBySelector({
        providerId: 'slack',
        selectorKind: 'installation',
        selectorValue: 'installation-1',
      }),
    ).resolves.toMatchObject({ id: 'callback-binding-1', status: 'active' });
  });

  it('round-trips provider binding JSON columns through mark transitions', async () => {
    const harness = store.stores.harness!;
    await harness.resolveProviderCallbackBinding(sampleProviderCallbackBinding());
    await harness.markProviderCallbackBindingStatus({
      bindingId: 'callback-binding-1',
      status: 'undeliverable',
      updatedAt: 2000,
      lastError: { code: 'worker_unavailable', message: 'provider missing', retryable: true },
    });

    await expect(
      harness.markProviderCallbackBindingStatus({ bindingId: 'callback-binding-1', status: 'active', updatedAt: 3000 }),
    ).resolves.toMatchObject({
      id: 'callback-binding-1',
      origin: { route: 'support-events' },
      status: 'active',
      lastError: undefined,
    });
  });

  it('allows disabled or undeliverable bindings to reactivate when no active selector owner exists', async () => {
    const harness = store.stores.harness!;
    await harness.resolveProviderCallbackBinding(sampleProviderCallbackBinding());

    await expect(
      harness.markProviderCallbackBindingStatus({
        bindingId: 'callback-binding-1',
        status: 'disabled',
        updatedAt: 2000,
        lastError: { code: 'worker_unavailable', message: 'provider disabled', retryable: true },
      }),
    ).resolves.toMatchObject({ status: 'disabled', lastError: { code: 'worker_unavailable' } });
    await expect(
      harness.loadProviderCallbackBindingBySelector({
        providerId: 'slack',
        selectorKind: 'installation',
        selectorValue: 'installation-1',
      }),
    ).resolves.toBeNull();
    await expect(
      harness.markProviderCallbackBindingStatus({ bindingId: 'callback-binding-1', status: 'active', updatedAt: 3000 }),
    ).resolves.toMatchObject({ status: 'active', lastError: undefined });
    await expect(
      harness.markProviderCallbackBindingStatus({
        bindingId: 'callback-binding-1',
        status: 'undeliverable',
        updatedAt: 4000,
        lastError: { code: 'worker_unavailable', message: 'provider undeliverable', retryable: true },
      }),
    ).resolves.toMatchObject({ status: 'undeliverable', lastError: { code: 'worker_unavailable' } });
    await expect(
      harness.loadProviderCallbackBindingBySelector({
        providerId: 'slack',
        selectorKind: 'installation',
        selectorValue: 'installation-1',
      }),
    ).resolves.toBeNull();
    await expect(
      harness.markProviderCallbackBindingStatus({ bindingId: 'callback-binding-1', status: 'active', updatedAt: 5000 }),
    ).resolves.toMatchObject({ status: 'active', lastError: undefined });
  });
});

describe('HarnessPG renewSessionLeaseSubtree (§5.8 / PF-821)', () => {
  const store = new PostgresStore({ ...TEST_CONFIG, id: 'pg-harness-renew-subtree-test-store' });

  beforeAll(async () => {
    await store.init();
  });

  beforeEach(async () => {
    await store.stores.harness?.dangerouslyClearAll();
  });

  afterAll(async () => {
    await store.stores.harness?.dangerouslyClearAll().catch(() => {});
    await store.close();
  });

  async function seedOwned(
    id: string,
    ownerId: string,
    parentSessionId?: string,
    overrides: Partial<Parameters<typeof createSampleSessionRecord>[0]> = {},
  ) {
    const harness = store.stores.harness!;
    await harness.saveSession(createSampleSessionRecord({ id, threadId: `t-${id}`, parentSessionId, ...overrides }), {
      ownerId,
      ifVersion: 0,
    });
    await harness.acquireSessionLease({ sessionId: id, ownerId, ttlMs: 60_000 });
  }

  it('atomically renews the root and every active descendant (incl. a grandchild) to one capped expiry', async () => {
    const harness = store.stores.harness!;
    await seedOwned('root', 'h-1');
    await seedOwned('child', 'h-1', 'root');
    await seedOwned('grandchild', 'h-1', 'child');

    const before = Date.now();
    const result = await harness.renewSessionLeaseSubtree({ rootSessionId: 'root', ownerId: 'h-1', ttlMs: 120_000 });
    // child + grandchild — the recursive CTE must descend past the first level.
    expect(result.renewedDescendantCount).toBe(2);

    for (const id of ['root', 'child', 'grandchild']) {
      const rec = await harness.loadSession({ sessionId: id });
      expect(rec?.leaseExpiresAt).toBeGreaterThanOrEqual(before + 120_000 - 5_000);
    }
  });

  it('skips closed descendants (no live lease) but still renews active ones', async () => {
    const harness = store.stores.harness!;
    await seedOwned('root', 'h-1');
    await seedOwned('open-child', 'h-1', 'root');
    // A closed descendant holds no live lease and must not be counted/renewed.
    await harness.saveSession(
      createSampleSessionRecord({
        id: 'closed-child',
        threadId: 't-closed',
        parentSessionId: 'root',
        closedAt: 5000,
        lastActivityAt: 5000,
      }),
      { ownerId: 'h-1', ifVersion: 0 },
    );

    const result = await harness.renewSessionLeaseSubtree({ rootSessionId: 'root', ownerId: 'h-1', ttlMs: 120_000 });
    expect(result.renewedDescendantCount).toBe(1);
  });

  it('fences and renews NOTHING when an active descendant is owned by another instance (§5.8 all-or-nothing)', async () => {
    const harness = store.stores.harness!;
    await seedOwned('root', 'h-1');
    await seedOwned('foreign-child', 'h-2', 'root'); // split subtree

    const rootBefore = await harness.loadSession({ sessionId: 'root' });
    await expect(
      harness.renewSessionLeaseSubtree({ rootSessionId: 'root', ownerId: 'h-1', ttlMs: 120_000 }),
    ).rejects.toBeInstanceOf(HarnessStorageLeaseConflictError);

    // Root lease must be untouched — no parent-only partial commit.
    const rootAfter = await harness.loadSession({ sessionId: 'root' });
    expect(rootAfter?.leaseExpiresAt).toBe(rootBefore?.leaseExpiresAt);
  });

  it('throws SessionNotFound for an unknown root and LeaseConflict for a non-owned root', async () => {
    const harness = store.stores.harness!;
    await expect(
      harness.renewSessionLeaseSubtree({ rootSessionId: 'ghost', ownerId: 'h-1', ttlMs: 1000 }),
    ).rejects.toBeInstanceOf(HarnessStorageSessionNotFoundError);

    await seedOwned('root', 'h-2'); // owned by someone else
    await expect(
      harness.renewSessionLeaseSubtree({ rootSessionId: 'root', ownerId: 'h-1', ttlMs: 1000 }),
    ).rejects.toBeInstanceOf(HarnessStorageLeaseConflictError);
  });
});
