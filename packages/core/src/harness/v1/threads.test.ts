/**
 * Harness v1 — `createHarnessOperatorThreadController(harness).*` API (§4.4 / §5.2).
 *
 * Covers:
 *   - CRUD: create / list / get / rename / clone / delete
 *   - resource scoping: cross-resource reads return null, cross-resource
 *     writes throw `HarnessThreadNotFoundError`, cross-resource deletes are
 *     silent no-ops
 *   - cascade-on-delete: deleting a thread force-deletes sessions rooted on
 *     that thread before deleting the thread row
 *   - lifecycle events: `thread_created`, `thread_renamed`, `thread_cloned`,
 *     `thread_deleted` fire on the harness emitter with the right payload
 */

import { describe, expect, it, vi } from 'vitest';

import { Mastra } from '../../mastra';
import {
  HarnessStorageThreadDeleteFenceConflictError,
  HarnessStorageThreadDeleteFenceUnsupportedError,
} from '../../storage/domains/harness/base';
import { InMemoryHarness } from '../../storage/domains/harness/inmemory';
import { InMemoryDB } from '../../storage/domains/inmemory-db';
import { InMemoryMemory } from '../../storage/domains/memory/inmemory';
import { InMemoryStore } from '../../storage/mock';
import { MockAgent, setupHarness } from './__test-utils__';
import { HarnessConfigError, HarnessStorageError, HarnessThreadNotFoundError } from './errors';
import { createHarnessOperatorThreadController, Harness } from './harness';

const externalSessionStorageOwnerMetadataKey = '__mastraHarnessExternalSessionStorageOwner';
const harnessThreadDeleteInProgressMetadataKey = '__mastraHarnessThreadDeleteInProgress';

// Default `setupHarness()` builds a standalone Harness with a default
// `InMemoryStore`, which gives us both the harness storage domain and the
// memory storage domain (used by thread CRUD) backed by a single in-memory
// db. That's the exact wiring the thread API needs.

// ---------------------------------------------------------------------------
// CRUD
// ---------------------------------------------------------------------------

function setupHarnessWithHarnessStore(storage: InMemoryHarness): { harness: Harness; storage: InMemoryHarness } {
  const agent = new MockAgent({ id: 'default' });
  const store = new InMemoryStore();
  store.stores.harness = storage;
  const harness = new Harness({
    agents: { default: agent } as any,
    storage: store,
    modes: [{ id: 'default', agentId: 'default' }],
    defaultModeId: 'default',
  });
  return { harness, storage };
}

function setupMastraWithMemory(opts: { memory: InMemoryMemory; harness?: InMemoryHarness }) {
  const agent = new MockAgent({ id: 'default' });
  return {
    getAgent: () => agent,
    _assertDirectHarnessRegistrationAvailable: vi.fn(),
    _assertHarnessAgentChannelToolCollisionsForHarness: vi.fn(),
    _registerDirectHarness: vi.fn(),
    _unregisterDirectHarness: vi.fn(),
    getStorage: () => ({
      stores: {
        memory: opts.memory,
        ...(opts.harness ? { harness: opts.harness } : {}),
      },
      getStore: async (name: string) => {
        if (name === 'memory') return opts.memory;
        if (name === 'harness') return opts.harness;
        return undefined;
      },
    }),
  } as unknown as Mastra;
}

describe('harness.threads — CRUD', () => {
  it('creates a thread with a minted id and round-trips through get()', async () => {
    const { harness } = setupHarness();

    const thread = await createHarnessOperatorThreadController(harness).create({
      resourceId: 'r1',
      title: 'first',
      metadata: { color: 'red' },
    });
    expect(thread.id).toMatch(/^thread-/);
    expect(thread.resourceId).toBe('r1');
    expect(thread.title).toBe('first');
    expect(thread.metadata).toEqual({ color: 'red' });

    const fetched = await createHarnessOperatorThreadController(harness).get({ resourceId: 'r1', threadId: thread.id });
    expect(fetched).not.toBeNull();
    expect(fetched!.id).toBe(thread.id);
    expect(fetched!.title).toBe('first');
  });

  it('honors a caller-supplied threadId when creating', async () => {
    const { harness } = setupHarness();
    const thread = await createHarnessOperatorThreadController(harness).create({
      resourceId: 'r1',
      threadId: 'thread-explicit-1',
      title: 'pinned',
    });
    expect(thread.id).toBe('thread-explicit-1');
  });

  it('does not overwrite a foreign resource thread for caller-supplied threadId creation', async () => {
    const { harness } = setupHarness();
    const original = await createHarnessOperatorThreadController(harness).create({
      resourceId: 'r1',
      threadId: 'thread-shared-explicit',
      title: 'owned',
    });

    await expect(
      createHarnessOperatorThreadController(harness).create({
        resourceId: 'r2',
        threadId: 'thread-shared-explicit',
        title: 'foreign-overwrite',
      }),
    ).rejects.toBeInstanceOf(HarnessThreadNotFoundError);

    await expect(
      createHarnessOperatorThreadController(harness).get({ resourceId: 'r1', threadId: original.id }),
    ).resolves.toMatchObject({
      resourceId: 'r1',
      title: 'owned',
    });
    await expect(
      createHarnessOperatorThreadController(harness).get({ resourceId: 'r2', threadId: original.id }),
    ).resolves.toBeNull();
  });

  it('keeps caller-supplied thread creation compatible with storage adapters that do not implement delete fences', async () => {
    class LegacyHarnessStorage extends InMemoryHarness {
      override async withThreadDeleteFence<T>(): Promise<T> {
        throw new Error('HarnessStorage.withThreadDeleteFence must be implemented by this storage adapter');
      }
    }
    const storage = new LegacyHarnessStorage({ db: new InMemoryDB() });
    const { harness } = setupHarnessWithHarnessStore(storage);

    const thread = await createHarnessOperatorThreadController(harness).create({
      resourceId: 'r1',
      threadId: 'thread-legacy-explicit',
      title: 'legacy',
    });

    expect(thread.id).toBe('thread-legacy-explicit');
  });

  it('keeps caller-supplied thread creation compatible with memory-only thread storage', async () => {
    const memory = new InMemoryMemory({ db: new InMemoryDB() });
    const harness = new Harness({
      mastra: setupMastraWithMemory({ memory }),
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
    });

    const thread = await createHarnessOperatorThreadController(harness).create({
      resourceId: 'r1',
      threadId: 'thread-memory-only',
      title: 'memory-only',
    });

    expect(thread.id).toBe('thread-memory-only');
    await expect(
      createHarnessOperatorThreadController(harness).get({ resourceId: 'r1', threadId: 'thread-memory-only' }),
    ).resolves.toMatchObject({
      id: 'thread-memory-only',
    });
  });

  it('fails closed for thread deletion with memory-only thread storage', async () => {
    const memory = new InMemoryMemory({ db: new InMemoryDB() });
    const harness = new Harness({
      mastra: setupMastraWithMemory({ memory }),
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
    });
    const thread = await createHarnessOperatorThreadController(harness).create({
      resourceId: 'r1',
      threadId: 'thread-memory-delete',
      title: 'memory-only-delete',
    });

    await expect(
      createHarnessOperatorThreadController(harness).delete({ resourceId: 'r1', threadId: thread.id }),
    ).rejects.toBeInstanceOf(HarnessConfigError);

    await expect(
      createHarnessOperatorThreadController(harness).get({ resourceId: 'r1', threadId: thread.id }),
    ).resolves.not.toBeNull();
  });

  it('lists threads for a resource', async () => {
    const { harness } = setupHarness();
    await createHarnessOperatorThreadController(harness).create({ resourceId: 'r1', title: 'a' });
    await createHarnessOperatorThreadController(harness).create({ resourceId: 'r1', title: 'b' });
    await createHarnessOperatorThreadController(harness).create({ resourceId: 'other', title: 'foreign' });

    const out = await createHarnessOperatorThreadController(harness).list({ resourceId: 'r1' });
    expect(out.threads).toHaveLength(2);
    expect(new Set(out.threads.map(t => t.title))).toEqual(new Set(['a', 'b']));
  });

  it('renames a thread, persists the new title, and returns the updated record', async () => {
    const { harness } = setupHarness();
    const created = await createHarnessOperatorThreadController(harness).create({ resourceId: 'r1', title: 'old' });

    const renamed = await createHarnessOperatorThreadController(harness).rename({
      resourceId: 'r1',
      threadId: created.id,
      title: 'new',
    });
    expect(renamed.title).toBe('new');

    const fetched = await createHarnessOperatorThreadController(harness).get({
      resourceId: 'r1',
      threadId: created.id,
    });
    expect(fetched!.title).toBe('new');
  });

  it('rename merges metadata patches over existing metadata', async () => {
    const { harness } = setupHarness();
    const created = await createHarnessOperatorThreadController(harness).create({
      resourceId: 'r1',
      title: 't',
      metadata: { keep: 1, override: 'old' },
    });
    const renamed = await createHarnessOperatorThreadController(harness).rename({
      resourceId: 'r1',
      threadId: created.id,
      title: 't2',
      metadata: { override: 'new', extra: true },
    });
    expect(renamed.metadata).toMatchObject({ keep: 1, override: 'new', extra: true });
  });

  it('rejects caller writes to reserved Harness metadata keys', async () => {
    const { harness } = setupHarness();
    const created = await createHarnessOperatorThreadController(harness).create({
      resourceId: 'r1',
      title: 'reserved',
    });

    await expect(
      createHarnessOperatorThreadController(harness).create({
        resourceId: 'r1',
        title: 'spoof',
        metadata: { [externalSessionStorageOwnerMetadataKey]: true },
      }),
    ).rejects.toBeInstanceOf(HarnessConfigError);
    await expect(
      createHarnessOperatorThreadController(harness).rename({
        resourceId: 'r1',
        threadId: created.id,
        title: 'renamed',
        metadata: { [externalSessionStorageOwnerMetadataKey]: false },
      }),
    ).rejects.toBeInstanceOf(HarnessConfigError);
    await expect(
      createHarnessOperatorThreadController(harness).setSettings({
        resourceId: 'r1',
        threadId: created.id,
        patch: { [externalSessionStorageOwnerMetadataKey]: undefined },
      }),
    ).rejects.toBeInstanceOf(HarnessConfigError);
  });

  it('clones a thread into a new id under the same resource', async () => {
    const { harness } = setupHarness();
    const source = await createHarnessOperatorThreadController(harness).create({ resourceId: 'r1', title: 'orig' });

    const clone = await createHarnessOperatorThreadController(harness).clone({
      resourceId: 'r1',
      threadId: source.id,
      title: 'orig (clone)',
    });
    expect(clone.id).not.toBe(source.id);
    expect(clone.resourceId).toBe('r1');
    expect(clone.title).toBe('orig (clone)');

    // Both threads visible in list().
    const out = await createHarnessOperatorThreadController(harness).list({ resourceId: 'r1' });
    expect(out.threads.map(t => t.id).sort()).toEqual([source.id, clone.id].sort());
  });

  it('deletes a thread and removes it from list()', async () => {
    const { harness } = setupHarness();
    const created = await createHarnessOperatorThreadController(harness).create({ resourceId: 'r1', title: 't' });
    await createHarnessOperatorThreadController(harness).delete({ resourceId: 'r1', threadId: created.id });

    const fetched = await createHarnessOperatorThreadController(harness).get({
      resourceId: 'r1',
      threadId: created.id,
    });
    expect(fetched).toBeNull();
  });

  it('clears thread-scoped observational memory when deleting a thread', async () => {
    const { harness } = setupHarness();
    const created = await createHarnessOperatorThreadController(harness).create({
      resourceId: 'r1',
      title: 'with observations',
    });
    const memory = (await harness._internalTryGetMemoryStorage())!;
    await memory.initializeObservationalMemory({
      threadId: created.id,
      resourceId: 'r1',
      scope: 'thread',
      config: {},
    });

    await expect(memory.getObservationalMemory(created.id, 'r1')).resolves.not.toBeNull();
    await createHarnessOperatorThreadController(harness).delete({ resourceId: 'r1', threadId: created.id });

    await expect(memory.getObservationalMemory(created.id, 'r1')).resolves.toBeNull();
  });
});

// ---------------------------------------------------------------------------
// Resource scoping
// ---------------------------------------------------------------------------

describe('harness.threads — resource scoping', () => {
  it('get() returns null for a thread owned by a different resource', async () => {
    const { harness } = setupHarness();
    const created = await createHarnessOperatorThreadController(harness).create({ resourceId: 'r1', title: 't' });
    const fetched = await createHarnessOperatorThreadController(harness).get({
      resourceId: 'r2',
      threadId: created.id,
    });
    expect(fetched).toBeNull();
  });

  it('list() filters strictly by resourceId', async () => {
    const { harness } = setupHarness();
    await createHarnessOperatorThreadController(harness).create({ resourceId: 'r1', title: 'mine' });
    await createHarnessOperatorThreadController(harness).create({ resourceId: 'r2', title: 'theirs' });

    const out1 = await createHarnessOperatorThreadController(harness).list({ resourceId: 'r1' });
    expect(out1.threads.map(t => t.title)).toEqual(['mine']);

    const out2 = await createHarnessOperatorThreadController(harness).list({ resourceId: 'r2' });
    expect(out2.threads.map(t => t.title)).toEqual(['theirs']);
  });

  it('rename() throws HarnessThreadNotFoundError for cross-resource access', async () => {
    const { harness } = setupHarness();
    const created = await createHarnessOperatorThreadController(harness).create({ resourceId: 'r1', title: 't' });
    await expect(
      createHarnessOperatorThreadController(harness).rename({ resourceId: 'r2', threadId: created.id, title: 'x' }),
    ).rejects.toThrow(HarnessThreadNotFoundError);
  });

  it('clone() throws HarnessThreadNotFoundError for cross-resource access', async () => {
    const { harness } = setupHarness();
    const created = await createHarnessOperatorThreadController(harness).create({ resourceId: 'r1', title: 't' });
    await expect(
      createHarnessOperatorThreadController(harness).clone({ resourceId: 'r2', threadId: created.id }),
    ).rejects.toThrow(HarnessThreadNotFoundError);
  });

  it('delete() is a silent no-op for cross-resource access', async () => {
    const { harness } = setupHarness();
    const created = await createHarnessOperatorThreadController(harness).create({ resourceId: 'r1', title: 't' });

    // Cross-tenant delete must not leak existence.
    await expect(
      createHarnessOperatorThreadController(harness).delete({ resourceId: 'r2', threadId: created.id }),
    ).resolves.toBeUndefined();

    // Thread should still exist for its real owner.
    const fetched = await createHarnessOperatorThreadController(harness).get({
      resourceId: 'r1',
      threadId: created.id,
    });
    expect(fetched).not.toBeNull();
  });

  it('rename/clone on a totally missing thread throws HarnessThreadNotFoundError', async () => {
    const { harness } = setupHarness();
    await expect(
      createHarnessOperatorThreadController(harness).rename({
        resourceId: 'r1',
        threadId: 'thread-missing',
        title: 'x',
      }),
    ).rejects.toThrow(HarnessThreadNotFoundError);
    await expect(
      createHarnessOperatorThreadController(harness).clone({ resourceId: 'r1', threadId: 'thread-missing' }),
    ).rejects.toThrow(HarnessThreadNotFoundError);
  });
});

// ---------------------------------------------------------------------------
// Cascade on delete
// ---------------------------------------------------------------------------

describe('harness.threads — cascade-on-delete', () => {
  it('force-deletes a live session bound to the thread before deleting', async () => {
    const { harness, storage } = setupHarness();
    const thread = await createHarnessOperatorThreadController(harness).create({ resourceId: 'r1', title: 't' });

    // Open a session that adopts this thread.
    const session = await harness.session({ threadId: thread.id, resourceId: 'r1' });
    expect(session.isClosed).toBe(false);

    await createHarnessOperatorThreadController(harness).delete({ resourceId: 'r1', threadId: thread.id });

    // The live session must have been cascade-closed locally and deleted from storage.
    expect(session.isClosed).toBe(true);
    await expect(storage.loadSession({ sessionId: session.id, harnessName: 'default' })).resolves.toBeNull();

    // The thread must be gone.
    const fetched = await createHarnessOperatorThreadController(harness).get({ resourceId: 'r1', threadId: thread.id });
    expect(fetched).toBeNull();
  });

  it('uses thread-scoped storage lookup before cascade-deleting sessions', async () => {
    const { harness, storage } = setupHarness();
    const thread = await createHarnessOperatorThreadController(harness).create({ resourceId: 'r1', title: 't' });
    const parent = await harness.session({ threadId: thread.id, resourceId: 'r1' });
    await harness.session({
      threadId: { fresh: true },
      resourceId: 'r1',
      parentSessionId: parent.id,
    });
    const originalListSessions = storage.listSessions.bind(storage);
    storage.listSessions = (async (opts: Parameters<typeof storage.listSessions>[0]) => {
      if (opts.parentSessionId === undefined && opts.includeClosed === true) {
        throw new Error('threads.delete must not scan every session in the resource');
      }
      return originalListSessions(opts);
    }) as typeof storage.listSessions;

    await createHarnessOperatorThreadController(harness).delete({ resourceId: 'r1', threadId: thread.id });

    await expect(storage.loadSession({ sessionId: parent.id, harnessName: 'default' })).resolves.toBeNull();
  });

  it('fails closed when thread deletion uses storage adapters that do not implement delete fences', async () => {
    class LegacyHarnessStorage extends InMemoryHarness {
      override async withThreadDeleteFence<T>(): Promise<T> {
        throw new HarnessStorageThreadDeleteFenceUnsupportedError();
      }
    }
    const storage = new LegacyHarnessStorage({ db: new InMemoryDB() });
    const { harness } = setupHarnessWithHarnessStore(storage);
    const thread = await createHarnessOperatorThreadController(harness).create({ resourceId: 'r1', title: 'legacy' });
    const session = await harness.session({ threadId: thread.id, resourceId: 'r1' });

    await expect(
      createHarnessOperatorThreadController(harness).delete({ resourceId: 'r1', threadId: thread.id }),
    ).rejects.toBeInstanceOf(HarnessStorageThreadDeleteFenceUnsupportedError);

    await expect(storage.loadSession({ sessionId: session.id, harnessName: 'default' })).resolves.not.toBeNull();
    await expect(
      createHarnessOperatorThreadController(harness).get({ resourceId: 'r1', threadId: thread.id }),
    ).resolves.not.toBeNull();
  });

  it('fails closed before deleting sessions when storage adapters cannot prove active thread ownership', async () => {
    class LegacyHarnessStorage extends InMemoryHarness {
      override async listActiveSessionsByThread(): Promise<never> {
        throw new Error('HarnessStorage.listActiveSessionsByThread must be implemented by this storage adapter');
      }
    }
    const storage = new LegacyHarnessStorage({ db: new InMemoryDB() });
    const { harness } = setupHarnessWithHarnessStore(storage);
    const thread = await createHarnessOperatorThreadController(harness).create({
      resourceId: 'r1',
      title: 'legacy-active-lookup',
    });
    const session = await harness.session({ threadId: thread.id, resourceId: 'r1' });

    await expect(
      createHarnessOperatorThreadController(harness).delete({ resourceId: 'r1', threadId: thread.id }),
    ).rejects.toThrow('HarnessStorage.listActiveSessionsByThread must be implemented by this storage adapter');

    await expect(storage.loadSession({ sessionId: session.id, harnessName: 'default' })).resolves.not.toBeNull();
    await expect(
      createHarnessOperatorThreadController(harness).get({ resourceId: 'r1', threadId: thread.id }),
    ).resolves.not.toBeNull();
  });

  it('waits for an active first-party delete fence before completing a concurrent delete', async () => {
    const { harness, storage } = setupHarness();
    const thread = await createHarnessOperatorThreadController(harness).create({
      resourceId: 'r1',
      title: 'shared-root',
    });
    let deleteSettled = false;
    let deletePromise: Promise<void> | undefined;

    await storage.withThreadDeleteFence(
      { threadId: thread.id, ownerId: 'harness-test:thread-delete:first', ttlMs: 30_000 },
      async () => {
        deletePromise = createHarnessOperatorThreadController(harness).delete({
          resourceId: 'r1',
          threadId: thread.id,
        });
        deletePromise.then(
          () => {
            deleteSettled = true;
          },
          () => {
            deleteSettled = true;
          },
        );
        await new Promise(resolve => setTimeout(resolve, 50));
        expect(deleteSettled).toBe(false);
      },
    );

    if (!deletePromise) throw new Error('concurrent delete was not started');
    await expect(deletePromise).resolves.toBeUndefined();
    await expect(
      createHarnessOperatorThreadController(harness).get({ resourceId: 'r1', threadId: thread.id }),
    ).resolves.toBeNull();
  });

  it('continues root deletion when a descendant thread is already fenced by another delete', async () => {
    const { harness, storage } = setupHarness();
    const thread = await createHarnessOperatorThreadController(harness).create({ resourceId: 'r1', title: 'parent' });
    const parent = await harness.session({ threadId: thread.id, resourceId: 'r1' });
    const child = await harness.session({
      threadId: { fresh: true },
      resourceId: 'r1',
      parentSessionId: parent.id,
    });
    await createHarnessOperatorThreadController(harness).create({
      resourceId: 'r1',
      threadId: child.threadId,
      title: 'child',
    });
    await child.close();

    const originalFence = storage.withThreadDeleteFence.bind(storage);
    storage.withThreadDeleteFence = (async (...args: Parameters<typeof storage.withThreadDeleteFence>) => {
      if (args[0].threadId === child.threadId) {
        throw new HarnessStorageThreadDeleteFenceConflictError(child.threadId);
      }
      return originalFence(...args);
    }) as typeof storage.withThreadDeleteFence;

    await expect(
      createHarnessOperatorThreadController(harness).delete({ resourceId: 'r1', threadId: thread.id }),
    ).resolves.toBeUndefined();

    await expect(storage.loadSession({ sessionId: parent.id, harnessName: 'default' })).resolves.toBeNull();
    await expect(storage.loadSession({ sessionId: child.id, harnessName: 'default' })).resolves.toBeNull();
    await expect(
      createHarnessOperatorThreadController(harness).get({ resourceId: 'r1', threadId: thread.id }),
    ).resolves.toBeNull();
    await expect(
      createHarnessOperatorThreadController(harness).get({ resourceId: 'r1', threadId: child.threadId }),
    ).resolves.not.toBeNull();
  });

  it('force-deletes descendant sessions even when they use separate threads', async () => {
    const { harness, storage } = setupHarness();
    const thread = await createHarnessOperatorThreadController(harness).create({ resourceId: 'r1', title: 'parent' });
    const childThread = await createHarnessOperatorThreadController(harness).create({
      resourceId: 'r1',
      title: 'child',
    });
    const parent = await harness.session({ threadId: thread.id, resourceId: 'r1' });
    const child = await harness.session({
      threadId: childThread.id,
      resourceId: 'r1',
      parentSessionId: parent.id,
    });

    await createHarnessOperatorThreadController(harness).delete({ resourceId: 'r1', threadId: thread.id });

    await expect(storage.loadSession({ sessionId: parent.id, harnessName: 'default' })).resolves.toBeNull();
    await expect(storage.loadSession({ sessionId: child.id, harnessName: 'default' })).resolves.toBeNull();
    await expect(
      createHarnessOperatorThreadController(harness).get({ resourceId: 'r1', threadId: thread.id }),
    ).resolves.toBeNull();
    await expect(
      createHarnessOperatorThreadController(harness).get({ resourceId: 'r1', threadId: childThread.id }),
    ).resolves.not.toBeNull();
  });

  it('reopens a closed descendant on same-resource thread lookup rather than forking a new owner (§5.3)', async () => {
    // Under §5.3 reopen, a same-resource/same-namespace thread lookup after
    // close reopens the SAME child session (it cannot become an independent new
    // owner on that thread). The "independent session keeps the thread" guard is
    // covered by the cross-resource and cross-namespace cases below.
    const { harness } = setupHarness();
    const thread = await createHarnessOperatorThreadController(harness).create({ resourceId: 'r1', title: 'parent' });
    const parent = await harness.session({ threadId: thread.id, resourceId: 'r1' });
    const child = await harness.session({
      threadId: { fresh: true },
      resourceId: 'r1',
      parentSessionId: parent.id,
    });
    await createHarnessOperatorThreadController(harness).create({
      resourceId: 'r1',
      threadId: child.threadId,
      title: 'child',
    });
    await child.close();

    const reused = await harness.session({ threadId: child.threadId, resourceId: 'r1' });
    expect(reused.id).toBe(child.id);
    expect(reused.isClosed).toBe(false);
  });

  it('keeps an owned descendant thread when a different resource has an active session on that thread id', async () => {
    const { harness, storage } = setupHarness();
    const thread = await createHarnessOperatorThreadController(harness).create({ resourceId: 'r1', title: 'parent' });
    const parent = await harness.session({ threadId: thread.id, resourceId: 'r1' });
    const child = await harness.session({
      threadId: { fresh: true },
      resourceId: 'r1',
      parentSessionId: parent.id,
    });
    await createHarnessOperatorThreadController(harness).create({
      resourceId: 'r1',
      threadId: child.threadId,
      title: 'child',
    });
    await child.close();

    const crossResource = await harness.session({ threadId: child.threadId, resourceId: 'r2' });

    await createHarnessOperatorThreadController(harness).delete({ resourceId: 'r1', threadId: thread.id });

    await expect(storage.loadSession({ sessionId: parent.id, harnessName: 'default' })).resolves.toBeNull();
    await expect(storage.loadSession({ sessionId: child.id, harnessName: 'default' })).resolves.toBeNull();
    await expect(storage.loadSession({ sessionId: crossResource.id, harnessName: 'default' })).resolves.not.toBeNull();
    await expect(
      createHarnessOperatorThreadController(harness).get({ resourceId: 'r1', threadId: child.threadId }),
    ).resolves.not.toBeNull();
  });

  it('keeps an owned descendant thread when another harness namespace has an active session on that thread id', async () => {
    const { harness, storage } = setupHarness();
    const thread = await createHarnessOperatorThreadController(harness).create({ resourceId: 'r1', title: 'parent' });
    const parent = await harness.session({ threadId: thread.id, resourceId: 'r1' });
    const child = await harness.session({
      threadId: { fresh: true },
      resourceId: 'r1',
      parentSessionId: parent.id,
    });
    await createHarnessOperatorThreadController(harness).create({
      resourceId: 'r1',
      threadId: child.threadId,
      title: 'child',
    });
    await child.close();
    await storage.saveSession(
      {
        ...child.getRecord(),
        harnessName: 'other',
        id: 'other-harness-session',
        resourceId: 'r2',
        closedAt: undefined,
      },
      { harnessName: 'other', ownerId: 'other', ifVersion: 0 },
    );

    await createHarnessOperatorThreadController(harness).delete({ resourceId: 'r1', threadId: thread.id });

    await expect(storage.loadSession({ sessionId: parent.id, harnessName: 'default' })).resolves.toBeNull();
    await expect(storage.loadSession({ sessionId: child.id, harnessName: 'default' })).resolves.toBeNull();
    await expect(
      storage.loadSession({ sessionId: 'other-harness-session', harnessName: 'other' }),
    ).resolves.not.toBeNull();
    await expect(
      createHarnessOperatorThreadController(harness).get({ resourceId: 'r1', threadId: child.threadId }),
    ).resolves.not.toBeNull();
  });

  it('keeps the root thread when a different resource has an active session on that thread id', async () => {
    const { harness, storage } = setupHarness();
    const thread = await createHarnessOperatorThreadController(harness).create({
      resourceId: 'r1',
      title: 'shared-root',
    });
    const owner = await harness.session({ threadId: thread.id, resourceId: 'r1' });
    const crossResource = await harness.session({ threadId: thread.id, resourceId: 'r2' });

    await createHarnessOperatorThreadController(harness).delete({ resourceId: 'r1', threadId: thread.id });

    await expect(storage.loadSession({ sessionId: owner.id, harnessName: 'default' })).resolves.toBeNull();
    await expect(storage.loadSession({ sessionId: crossResource.id, harnessName: 'default' })).resolves.not.toBeNull();
    await expect(
      createHarnessOperatorThreadController(harness).get({ resourceId: 'r1', threadId: thread.id }),
    ).resolves.not.toBeNull();
  });

  it('keeps the root thread when another harness namespace has an active session on that thread id', async () => {
    const { harness, storage } = setupHarness();
    const thread = await createHarnessOperatorThreadController(harness).create({
      resourceId: 'r1',
      title: 'shared-root',
    });
    const owner = await harness.session({ threadId: thread.id, resourceId: 'r1' });
    await storage.saveSession(
      {
        ...owner.getRecord(),
        harnessName: 'other',
        id: 'other-harness-session',
        resourceId: 'r2',
        closedAt: undefined,
      },
      { harnessName: 'other', ownerId: 'other', ifVersion: 0 },
    );

    await createHarnessOperatorThreadController(harness).delete({ resourceId: 'r1', threadId: thread.id });

    await expect(storage.loadSession({ sessionId: owner.id, harnessName: 'default' })).resolves.toBeNull();
    await expect(
      storage.loadSession({ sessionId: 'other-harness-session', harnessName: 'other' }),
    ).resolves.not.toBeNull();
    await expect(
      createHarnessOperatorThreadController(harness).get({ resourceId: 'r1', threadId: thread.id }),
    ).resolves.not.toBeNull();
  });

  it('keeps the root thread when a different resource has a closed session on that thread id', async () => {
    const { harness, storage } = setupHarness();
    const thread = await createHarnessOperatorThreadController(harness).create({
      resourceId: 'r1',
      title: 'shared-root',
    });
    const owner = await harness.session({ threadId: thread.id, resourceId: 'r1' });
    const crossResource = await harness.session({ threadId: thread.id, resourceId: 'r2' });
    await crossResource.close();

    await createHarnessOperatorThreadController(harness).delete({ resourceId: 'r1', threadId: thread.id });

    await expect(storage.loadSession({ sessionId: owner.id, harnessName: 'default' })).resolves.toBeNull();
    await expect(storage.loadSession({ sessionId: crossResource.id, harnessName: 'default' })).resolves.not.toBeNull();
    await expect(
      createHarnessOperatorThreadController(harness).get({ resourceId: 'r1', threadId: thread.id }),
    ).resolves.not.toBeNull();
  });

  it('fails closed before deleting sessions through a separate session storage override', async () => {
    const storage = new InMemoryHarness({ db: new InMemoryDB() });
    const harness = new Harness({
      agents: { default: new MockAgent({ id: 'default' }) } as any,
      storage: new InMemoryStore(),
      sessions: { storage },
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
    });
    const thread = await createHarnessOperatorThreadController(harness).create({
      resourceId: 'r1',
      title: 'separate-storage',
    });
    const session = await harness.session({ threadId: thread.id, resourceId: 'r1' });

    await expect(
      createHarnessOperatorThreadController(harness).delete({ resourceId: 'r1', threadId: thread.id }),
    ).rejects.toBeInstanceOf(HarnessConfigError);

    await expect(storage.loadSession({ sessionId: session.id, harnessName: 'default' })).resolves.not.toBeNull();
    await expect(
      createHarnessOperatorThreadController(harness).get({ resourceId: 'r1', threadId: thread.id }),
    ).resolves.not.toBeNull();
  });

  it('fails closed when another registered harness uses a separate session storage override on the same memory store', async () => {
    const agent = new MockAgent({ id: 'default' });
    const sharedStorage = new InMemoryStore();
    const overrideStorage = new InMemoryHarness({ db: new InMemoryDB() });
    const primary = new Harness({
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
    });
    const override = new Harness({
      sessions: { storage: overrideStorage },
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
    });
    new Mastra({
      agents: { default: agent } as any,
      storage: sharedStorage,
      harnesses: { primary, override },
    });
    const thread = await createHarnessOperatorThreadController(primary).create({
      resourceId: 'r1',
      title: 'shared-memory',
    });
    const visible = await primary.session({ threadId: thread.id, resourceId: 'r1' });
    const hidden = await override.session({ threadId: thread.id, resourceId: 'r2' });

    await expect(
      createHarnessOperatorThreadController(primary).delete({ resourceId: 'r1', threadId: thread.id }),
    ).rejects.toBeInstanceOf(HarnessConfigError);

    await expect(
      sharedStorage.stores.harness!.loadSession({ sessionId: visible.id, harnessName: 'primary' }),
    ).resolves.not.toBeNull();
    await expect(
      overrideStorage.loadSession({ sessionId: hidden.id, harnessName: 'override' }),
    ).resolves.not.toBeNull();
    await expect(
      createHarnessOperatorThreadController(primary).get({ resourceId: 'r1', threadId: thread.id }),
    ).resolves.not.toBeNull();
  });

  it('fails closed when another Mastra-bound harness uses a separate session storage override on the same memory store', async () => {
    const agent = new MockAgent({ id: 'default' });
    const sharedStorage = new InMemoryStore();
    const overrideStorage = new InMemoryHarness({ db: new InMemoryDB() });
    const override = new Harness({
      sessions: { storage: overrideStorage },
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
    });
    const mastra = new Mastra({
      agents: { default: agent } as any,
      storage: sharedStorage,
      harnesses: { override },
    });
    const primary = new Harness({
      mastra,
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
    });
    const thread = await createHarnessOperatorThreadController(primary).create({
      resourceId: 'r1',
      title: 'direct-bound',
    });
    const visible = await primary.session({ threadId: thread.id, resourceId: 'r1' });
    const hidden = await override.session({ threadId: thread.id, resourceId: 'r2' });

    await expect(
      createHarnessOperatorThreadController(primary).delete({ resourceId: 'r1', threadId: thread.id }),
    ).rejects.toBeInstanceOf(HarnessConfigError);

    await expect(
      sharedStorage.stores.harness!.loadSession({ sessionId: visible.id, harnessName: 'default' }),
    ).resolves.not.toBeNull();
    await expect(
      overrideStorage.loadSession({ sessionId: hidden.id, harnessName: 'override' }),
    ).resolves.not.toBeNull();
    await expect(
      createHarnessOperatorThreadController(primary).get({ resourceId: 'r1', threadId: thread.id }),
    ).resolves.not.toBeNull();
  });

  it('fails closed when a memory-only harness shares memory with a storage-backed harness', async () => {
    const memory = new InMemoryMemory({ db: new InMemoryDB() });
    const mastra = setupMastraWithMemory({ memory });
    const memoryOnly = new Harness({
      mastra,
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
    });
    const overrideStorage = new InMemoryHarness({ db: new InMemoryDB() });
    const override = new Harness({
      mastra,
      sessions: { storage: overrideStorage },
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
    });
    const thread = await createHarnessOperatorThreadController(memoryOnly).create({
      resourceId: 'r1',
      threadId: 'shared-memory-only',
      title: 'root',
    });
    const hidden = await override.session({ threadId: thread.id, resourceId: 'r2' });
    await expect(memory.getThreadById({ threadId: thread.id })).resolves.toMatchObject({
      metadata: expect.objectContaining({ [externalSessionStorageOwnerMetadataKey]: true }),
    });

    await expect(
      createHarnessOperatorThreadController(memoryOnly).delete({ resourceId: 'r1', threadId: thread.id }),
    ).rejects.toBeInstanceOf(HarnessConfigError);

    await expect(overrideStorage.loadSession({ sessionId: hidden.id, harnessName: 'default' })).resolves.not.toBeNull();
    await expect(
      createHarnessOperatorThreadController(memoryOnly).get({ resourceId: 'r1', threadId: thread.id }),
    ).resolves.not.toBeNull();
  });

  it('fails closed when a different Mastra instance shares memory with a session storage override', async () => {
    const memory = new InMemoryMemory({ db: new InMemoryDB() });
    const primaryStorage = new InMemoryHarness({ db: new InMemoryDB() });
    const primary = new Harness({
      mastra: setupMastraWithMemory({ memory, harness: primaryStorage }),
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
    });
    const overrideStorage = new InMemoryHarness({ db: new InMemoryDB() });
    const override = new Harness({
      mastra: setupMastraWithMemory({ memory }),
      sessions: { storage: overrideStorage },
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
    });
    const thread = await createHarnessOperatorThreadController(primary).create({
      resourceId: 'r1',
      title: 'shared-memory-cross-mastra',
    });
    const visible = await primary.session({ threadId: thread.id, resourceId: 'r1' });
    const hidden = await override.session({ threadId: thread.id, resourceId: 'r2' });
    await expect(memory.getThreadById({ threadId: thread.id })).resolves.toMatchObject({
      metadata: expect.objectContaining({ [externalSessionStorageOwnerMetadataKey]: true }),
    });

    await expect(
      createHarnessOperatorThreadController(primary).delete({ resourceId: 'r1', threadId: thread.id }),
    ).rejects.toBeInstanceOf(HarnessConfigError);

    await expect(primaryStorage.loadSession({ sessionId: visible.id, harnessName: 'default' })).resolves.not.toBeNull();
    await expect(overrideStorage.loadSession({ sessionId: hidden.id, harnessName: 'default' })).resolves.not.toBeNull();
    await expect(
      createHarnessOperatorThreadController(primary).get({ resourceId: 'r1', threadId: thread.id }),
    ).resolves.not.toBeNull();
  });

  it('blocks separate session storage attachment while a thread delete is in progress', async () => {
    const memory = new InMemoryMemory({ db: new InMemoryDB() });
    const primary = new Harness({
      mastra: setupMastraWithMemory({ memory }),
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
    });
    const overrideStorage = new InMemoryHarness({ db: new InMemoryDB() });
    const override = new Harness({
      mastra: setupMastraWithMemory({ memory }),
      sessions: { storage: overrideStorage },
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
    });
    const thread = await createHarnessOperatorThreadController(primary).create({
      resourceId: 'r1',
      title: 'delete-in-progress',
    });
    await memory.updateThread({
      id: thread.id,
      title: thread.title,
      metadata: { [harnessThreadDeleteInProgressMetadataKey]: true },
    });

    await expect(override.session({ threadId: thread.id, resourceId: 'r2' })).rejects.toBeInstanceOf(
      HarnessConfigError,
    );
    await expect(overrideStorage.listSessionsByThread({ threadId: thread.id, includeClosed: true })).resolves.toEqual(
      [],
    );
  });

  it('fails closed when separate session storage targets a missing memory thread', async () => {
    const memory = new InMemoryMemory({ db: new InMemoryDB() });
    const overrideStorage = new InMemoryHarness({ db: new InMemoryDB() });
    const override = new Harness({
      mastra: setupMastraWithMemory({ memory }),
      sessions: { storage: overrideStorage },
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
    });

    await expect(override.session({ threadId: 'missing-thread', resourceId: 'r1' })).rejects.toBeInstanceOf(
      HarnessConfigError,
    );
    await expect(
      overrideStorage.listSessionsByThread({ threadId: 'missing-thread', includeClosed: true }),
    ).resolves.toEqual([]);
  });

  it('marks an existing separate session storage owner when hydrating stored sessions', async () => {
    const memory = new InMemoryMemory({ db: new InMemoryDB() });
    const primaryStorage = new InMemoryHarness({ db: new InMemoryDB() });
    const primary = new Harness({
      mastra: setupMastraWithMemory({ memory, harness: primaryStorage }),
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
    });
    const overrideStorage = new InMemoryHarness({ db: new InMemoryDB() });
    const override = new Harness({
      mastra: setupMastraWithMemory({ memory }),
      sessions: { storage: overrideStorage },
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
    });
    const thread = await createHarnessOperatorThreadController(primary).create({
      resourceId: 'r1',
      title: 'hydrate-marker',
    });
    const hidden = await override.session({ threadId: thread.id, resourceId: 'r2' });
    await overrideStorage.releaseSessionLease({
      sessionId: hidden.id,
      ownerId: override.ownerId,
      harnessName: 'default',
    });
    await memory.updateThread({
      id: thread.id,
      title: thread.title,
      metadata: { [externalSessionStorageOwnerMetadataKey]: false },
    });
    const resumedOverride = new Harness({
      mastra: setupMastraWithMemory({ memory }),
      sessions: { storage: overrideStorage },
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
    });

    await expect(resumedOverride.session({ threadId: thread.id, resourceId: 'r2' })).resolves.toMatchObject({
      id: hidden.id,
    });

    await expect(memory.getThreadById({ threadId: thread.id })).resolves.toMatchObject({
      metadata: expect.objectContaining({ [externalSessionStorageOwnerMetadataKey]: true }),
    });
    await expect(
      createHarnessOperatorThreadController(primary).delete({ resourceId: 'r1', threadId: thread.id }),
    ).rejects.toBeInstanceOf(HarnessConfigError);
  });

  it('fails closed when a memory thread has a durable external session storage owner marker', async () => {
    const { harness, storage } = setupHarness();
    const thread = await createHarnessOperatorThreadController(harness).create({
      resourceId: 'r1',
      title: 'externally-owned',
    });
    const owner = await harness.session({ threadId: thread.id, resourceId: 'r1' });
    const memory = await harness.mastra.getStorage()!.getStore('memory');
    await memory!.updateThread({
      id: thread.id,
      title: thread.title,
      metadata: { [externalSessionStorageOwnerMetadataKey]: true },
    });

    await expect(
      createHarnessOperatorThreadController(harness).delete({ resourceId: 'r1', threadId: thread.id }),
    ).rejects.toBeInstanceOf(HarnessConfigError);

    await expect(storage.loadSession({ sessionId: owner.id, harnessName: 'default' })).resolves.not.toBeNull();
    await expect(
      createHarnessOperatorThreadController(harness).get({ resourceId: 'r1', threadId: thread.id }),
    ).resolves.not.toBeNull();
    await expect(
      createHarnessOperatorThreadController(harness).getSettings({ resourceId: 'r1', threadId: thread.id }),
    ).resolves.toEqual({});
  });

  it('preserves reserved ownership metadata when recreating an existing thread id', async () => {
    const memory = new InMemoryMemory({ db: new InMemoryDB() });
    const primaryStorage = new InMemoryHarness({ db: new InMemoryDB() });
    const primary = new Harness({
      mastra: setupMastraWithMemory({ memory, harness: primaryStorage }),
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
    });
    const overrideStorage = new InMemoryHarness({ db: new InMemoryDB() });
    const override = new Harness({
      mastra: setupMastraWithMemory({ memory }),
      sessions: { storage: overrideStorage },
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
    });
    const thread = await createHarnessOperatorThreadController(primary).create({ resourceId: 'r1', title: 'owned' });
    await override.session({ threadId: thread.id, resourceId: 'r2' });

    await expect(
      createHarnessOperatorThreadController(primary).create({
        resourceId: 'r1',
        threadId: thread.id,
        title: 'recreated',
        metadata: { color: 'blue' },
      }),
    ).resolves.toMatchObject({ title: 'recreated', metadata: { color: 'blue' } });
    await expect(memory.getThreadById({ threadId: thread.id })).resolves.toMatchObject({
      metadata: expect.objectContaining({
        color: 'blue',
        [externalSessionStorageOwnerMetadataKey]: true,
      }),
    });
    await expect(
      createHarnessOperatorThreadController(primary).delete({ resourceId: 'r1', threadId: thread.id }),
    ).rejects.toBeInstanceOf(HarnessConfigError);
  });

  it('does not keep shutdown harnesses in shared-memory delete ownership checks', async () => {
    const memory = new InMemoryMemory({ db: new InMemoryDB() });
    const primaryStorage = new InMemoryHarness({ db: new InMemoryDB() });
    const primary = new Harness({
      mastra: setupMastraWithMemory({ memory, harness: primaryStorage }),
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
    });
    const overrideStorage = new InMemoryHarness({ db: new InMemoryDB() });
    const temporaryOverride = new Harness({
      mastra: setupMastraWithMemory({ memory }),
      sessions: { storage: overrideStorage },
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
    });
    const thread = await createHarnessOperatorThreadController(primary).create({ resourceId: 'r1', title: 'owned' });
    const session = await primary.session({ threadId: thread.id, resourceId: 'r1' });

    await temporaryOverride.shutdown();
    await createHarnessOperatorThreadController(primary).delete({ resourceId: 'r1', threadId: thread.id });

    await expect(primaryStorage.loadSession({ sessionId: session.id, harnessName: 'default' })).resolves.toBeNull();
    await expect(
      createHarnessOperatorThreadController(primary).get({ resourceId: 'r1', threadId: thread.id }),
    ).resolves.toBeNull();
  });

  it('rolls back the external owner marker when attach loses a thread-delete race', async () => {
    const memory = new InMemoryMemory({ db: new InMemoryDB() });
    const primaryStorage = new InMemoryHarness({ db: new InMemoryDB() });
    const primary = new Harness({
      mastra: setupMastraWithMemory({ memory, harness: primaryStorage }),
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
    });
    const overrideStorage = new InMemoryHarness({ db: new InMemoryDB() });
    const override = new Harness({
      mastra: setupMastraWithMemory({ memory }),
      sessions: { storage: overrideStorage },
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
    });
    const thread = await createHarnessOperatorThreadController(primary).create({ resourceId: 'r1', title: 'owned' });
    const originalUpdateThread = memory.updateThread.bind(memory);
    let injectedDeleteRace = false;
    const updateThreadSpy = vi
      .spyOn(memory, 'updateThread')
      .mockImplementation(async (...args: Parameters<typeof memory.updateThread>) => {
        const [update] = args;
        const updated = await originalUpdateThread(...args);
        if (
          update.id === thread.id &&
          !injectedDeleteRace &&
          update.metadata?.[externalSessionStorageOwnerMetadataKey] === true
        ) {
          injectedDeleteRace = true;
          await originalUpdateThread({
            id: thread.id,
            title: updated.title ?? '',
            metadata: { [harnessThreadDeleteInProgressMetadataKey]: true },
          });
        }
        return updated;
      });

    await expect(override.session({ threadId: thread.id, resourceId: 'r2' })).rejects.toBeInstanceOf(
      HarnessConfigError,
    );

    expect(injectedDeleteRace).toBe(true);
    await expect(memory.getThreadById({ threadId: thread.id })).resolves.toMatchObject({
      metadata: expect.objectContaining({
        [externalSessionStorageOwnerMetadataKey]: false,
        [harnessThreadDeleteInProgressMetadataKey]: true,
      }),
    });
    updateThreadSpy.mockRestore();
  });

  it('rechecks root thread ownership before deleting the global memory thread', async () => {
    const { harness, storage } = setupHarness();
    const thread = await createHarnessOperatorThreadController(harness).create({
      resourceId: 'r1',
      title: 'shared-root',
    });
    const owner = await harness.session({ threadId: thread.id, resourceId: 'r1' });
    const memory = (await harness.mastra.getStorage()!.getStore('memory')) as any;
    const originalGetThreadById = memory.getThreadById.bind(memory);
    let calls = 0;
    const getThreadSpy = vi.spyOn(memory, 'getThreadById').mockImplementation(async (opts: { threadId: string }) => {
      calls += 1;
      if (opts.threadId === thread.id && calls > 1) {
        return { ...thread, resourceId: 'r2' };
      }
      return originalGetThreadById(opts);
    });
    const deleteThreadSpy = vi.spyOn(memory, 'deleteThread');

    await createHarnessOperatorThreadController(harness).delete({ resourceId: 'r1', threadId: thread.id });

    expect(deleteThreadSpy).not.toHaveBeenCalledWith({ threadId: thread.id });
    await expect(storage.loadSession({ sessionId: owner.id, harnessName: 'default' })).resolves.toBeNull();
    getThreadSpy.mockRestore();
    deleteThreadSpy.mockRestore();
    await expect(
      createHarnessOperatorThreadController(harness).get({ resourceId: 'r1', threadId: thread.id }),
    ).resolves.not.toBeNull();
  });

  it('fences root thread admission while cascade delete is running', async () => {
    const { harness, storage } = setupHarness();
    const thread = await createHarnessOperatorThreadController(harness).create({
      resourceId: 'r1',
      title: 'shared-root',
    });
    const owner = await harness.session({ threadId: thread.id, resourceId: 'r1' });
    const originalListSessionsByThread = storage.listSessionsByThread.bind(storage);
    let attemptedConcurrentAdmission = false;
    storage.listSessionsByThread = (async (...args: Parameters<typeof storage.listSessionsByThread>) => {
      if (args[0].threadId === thread.id && !attemptedConcurrentAdmission) {
        attemptedConcurrentAdmission = true;
        await expect(harness.session({ threadId: thread.id, resourceId: 'r2' })).rejects.toBeInstanceOf(
          HarnessStorageError,
        );
      }
      return originalListSessionsByThread(...args);
    }) as typeof storage.listSessionsByThread;

    await createHarnessOperatorThreadController(harness).delete({ resourceId: 'r1', threadId: thread.id });

    expect(attemptedConcurrentAdmission).toBe(true);
    await expect(storage.loadSession({ sessionId: owner.id, harnessName: 'default' })).resolves.toBeNull();
    await expect(
      createHarnessOperatorThreadController(harness).get({ resourceId: 'r1', threadId: thread.id }),
    ).resolves.toBeNull();
  });

  it('fences caller-supplied thread creation while a delete is active for that id', async () => {
    const { harness, storage } = setupHarness();

    await storage.withThreadDeleteFence({ threadId: 'shared-root', ownerId: 'deleter', ttlMs: 30_000 }, async () => {
      await expect(
        createHarnessOperatorThreadController(harness).create({
          resourceId: 'r2',
          threadId: 'shared-root',
          title: 'replacement',
        }),
      ).rejects.toThrow('currently fenced for deletion');
    });
  });

  it('reports thread delete contention when caller-supplied thread creation owns the fence', async () => {
    const { harness, storage } = setupHarness();
    const thread = await createHarnessOperatorThreadController(harness).create({
      resourceId: 'r1',
      title: 'shared-root',
    });
    const owner = await harness.session({ threadId: thread.id, resourceId: 'r1' });

    await storage.withThreadDeleteFence(
      { threadId: thread.id, ownerId: 'harness-test:thread-create:replacement', ttlMs: 30_000 },
      async () => {
        await expect(
          createHarnessOperatorThreadController(harness).delete({ resourceId: 'r1', threadId: thread.id }),
        ).rejects.toThrow('currently fenced for deletion');
      },
    );

    await expect(storage.loadSession({ sessionId: owner.id, harnessName: 'default' })).resolves.not.toBeNull();
    await expect(
      createHarnessOperatorThreadController(harness).get({ resourceId: 'r1', threadId: thread.id }),
    ).resolves.not.toBeNull();
  });

  it('deletes cleanly when no live session exists', async () => {
    const { harness } = setupHarness();
    const thread = await createHarnessOperatorThreadController(harness).create({ resourceId: 'r1', title: 't' });
    await expect(
      createHarnessOperatorThreadController(harness).delete({ resourceId: 'r1', threadId: thread.id }),
    ).resolves.toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// Lifecycle events
// ---------------------------------------------------------------------------

describe('harness.threads — no public lifecycle events (§4.1/§10.2)', () => {
  it('thread create/rename/clone/delete emit no public HarnessEventV1 events', async () => {
    const { harness } = setupHarness();
    const eventTypes: string[] = [];
    harness.subscribe(ev => eventTypes.push((ev as { type: string }).type));

    const ctrl = createHarnessOperatorThreadController(harness);
    const t = await ctrl.create({ resourceId: 'r1', title: 'first' });
    await ctrl.rename({ resourceId: 'r1', threadId: t.id, title: 'second' });
    const clone = await ctrl.clone({ resourceId: 'r1', threadId: t.id });
    await ctrl.delete({ resourceId: 'r1', threadId: clone.id });
    await ctrl.delete({ resourceId: 'r1', threadId: t.id });

    // §4.1/§10.2: thread CRUD is an internal/operator op — the closed
    // HarnessEventV1 union has no thread_* family, so nothing reaches the
    // public stream. (Operations still apply — verified by getters below and by
    // the behavioral thread tests above.)
    for (const banned of [
      'thread_created',
      'thread_renamed',
      'thread_cloned',
      'thread_deleted',
      'thread_settings_changed',
    ]) {
      expect(eventTypes).not.toContain(banned);
    }
    expect(await ctrl.get({ resourceId: 'r1', threadId: t.id })).toBeNull();
    expect(await ctrl.get({ resourceId: 'r1', threadId: clone.id })).toBeNull();
  });

  it('deleting a thread with a live session cascades the session close', async () => {
    const { harness } = setupHarness();
    const ctrl = createHarnessOperatorThreadController(harness);
    const t = await ctrl.create({ resourceId: 'r1', title: 't' });
    const session = await harness.session({ threadId: t.id, resourceId: 'r1' });

    await ctrl.delete({ resourceId: 'r1', threadId: t.id });

    // Cascade behavior (formerly asserted via the removed thread_deleted event):
    // the live session is no longer active and the thread is gone.
    expect(session.isClosed).toBe(true);
    expect(await ctrl.get({ resourceId: 'r1', threadId: t.id })).toBeNull();
  });
});
