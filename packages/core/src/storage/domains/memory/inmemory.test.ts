import { AsyncLocalStorage } from 'node:async_hooks';
import { describe, expect, it, beforeEach } from 'vitest';
import type { MastraDBMessage } from '../../../memory/types';
import { setRunFenceContext } from '../../run-fencing';
import type { RunFence, RunFenceScope } from '../../run-fencing';
import { InMemoryDB } from '../inmemory-db';
import { InMemoryMemory } from './inmemory';

// This mirrors createMessagesListIncludeResourceScopeTest in @internal/storage-test-utils.
// @mastra/core cannot depend on that package, because it would make the workspace
// dependency graph circular, so the same contract is asserted here by hand.

const makeMessage = ({
  id,
  threadId,
  resourceId,
  text,
  minute,
}: {
  id: string;
  threadId: string;
  resourceId: string;
  text: string;
  minute: number;
}): MastraDBMessage =>
  ({
    id,
    threadId,
    resourceId,
    role: 'user',
    type: 'text',
    createdAt: new Date(Date.UTC(2024, 0, 1, 0, minute)),
    content: { format: 2, parts: [{ type: 'text', text }] },
  }) as MastraDBMessage;

describe('InMemoryMemory listMessages include resource scope', () => {
  let store: InMemoryMemory;

  beforeEach(async () => {
    store = new InMemoryMemory({ db: new InMemoryDB() });
    await store.saveMessages({
      messages: [
        // resource-a owns thread-a1 and thread-a2.
        makeMessage({ id: 'a1', threadId: 'thread-a1', resourceId: 'resource-a', text: 'a first', minute: 0 }),
        makeMessage({ id: 'a2', threadId: 'thread-a1', resourceId: 'resource-a', text: 'a target', minute: 1 }),
        makeMessage({ id: 'a3', threadId: 'thread-a1', resourceId: 'resource-a', text: 'a last', minute: 2 }),
        makeMessage({ id: 'a4', threadId: 'thread-a2', resourceId: 'resource-a', text: 'a other thread', minute: 3 }),
        // resource-b owns thread-b1.
        makeMessage({ id: 'b1', threadId: 'thread-b1', resourceId: 'resource-b', text: 'b message', minute: 4 }),
      ],
    });
  });

  it('does not return messages owned by another resource', async () => {
    const result = await store.listMessages({
      threadId: 'thread-b1',
      resourceId: 'resource-b',
      include: [{ id: 'a2', withPreviousMessages: 2, withNextMessages: 2 }],
    });

    expect(result.messages.map(message => message.id)).toEqual(['b1']);
  });

  it('still returns a cross-thread include from the same resource', async () => {
    const result = await store.listMessages({
      threadId: 'thread-a2',
      resourceId: 'resource-a',
      include: [{ id: 'a2', withPreviousMessages: 1, withNextMessages: 1 }],
    });

    expect(result.messages.map(message => message.id)).toEqual(['a1', 'a2', 'a3', 'a4']);
  });

  it('does not return another resource on the semantic recall fast path', async () => {
    const result = await store.listMessages({
      threadId: 'thread-b1',
      resourceId: 'resource-b',
      perPage: 0,
      include: [{ id: 'a2', withPreviousMessages: 2, withNextMessages: 2 }],
    });

    expect(result.messages).toEqual([]);
  });

  it('keeps cross-resource includes when no resourceId is given', async () => {
    const result = await store.listMessages({
      threadId: 'thread-b1',
      include: [{ id: 'a2', withPreviousMessages: 1, withNextMessages: 1 }],
    });

    expect(result.messages.map(message => message.id)).toEqual(['a1', 'a2', 'a3', 'b1']);
  });

  it('reads the context window from the thread that owns the target message', async () => {
    // The include entry names a thread that the target message does not belong to.
    // The window must still come from the target message's own thread.
    const result = await store.listMessages({
      threadId: 'thread-a2',
      resourceId: 'resource-a',
      include: [{ id: 'a2', threadId: 'thread-b1', withPreviousMessages: 1, withNextMessages: 1 }],
    });

    expect(result.messages.map(message => message.id)).toEqual(['a1', 'a2', 'a3', 'a4']);
  });

  it('does not return messages owned by another resource from listMessagesByResourceId', async () => {
    const result = await store.listMessagesByResourceId({
      resourceId: 'resource-b',
      include: [{ id: 'a2', withPreviousMessages: 2, withNextMessages: 2 }],
    });

    expect(result.messages.map(message => message.id)).toEqual(['b1']);
  });

  it('does not return another resource from listMessagesByResourceId on the fast path', async () => {
    const result = await store.listMessagesByResourceId({
      resourceId: 'resource-b',
      perPage: 0,
      include: [{ id: 'a2', withPreviousMessages: 2, withNextMessages: 2 }],
    });

    expect(result.messages).toEqual([]);
  });

  it('returns the include context window from listMessagesByResourceId', async () => {
    const result = await store.listMessagesByResourceId({
      resourceId: 'resource-a',
      perPage: 0,
      include: [{ id: 'a2', withPreviousMessages: 1, withNextMessages: 1 }],
    });

    expect(result.messages.map(message => message.id)).toEqual(['a1', 'a2', 'a3']);
  });

  it('treats an empty resourceId in listMessagesByResourceId as a real scope', async () => {
    // The main query of listMessagesByResourceId compares the resourceId exactly, so an
    // empty string selects nothing. The include lookup must not be looser than that.
    const result = await store.listMessagesByResourceId({
      resourceId: '',
      include: [{ id: 'a2', withPreviousMessages: 2, withNextMessages: 2 }],
    });

    expect(result.messages).toEqual([]);
  });

  it('keeps an empty resourceId in listMessages unscoped, like its main query', async () => {
    const result = await store.listMessages({
      threadId: 'thread-b1',
      resourceId: '',
      include: [{ id: 'a2', withPreviousMessages: 1, withNextMessages: 1 }],
    });

    expect(result.messages.map(message => message.id)).toEqual(['a1', 'a2', 'a3', 'b1']);
  });
});

describe('InMemoryMemory observational-memory clear ownership', () => {
  it('rejects a stale owner coordinate without deleting the current thread record', async () => {
    const memory = new InMemoryMemory({ db: new InMemoryDB() });
    const threadId = 'clear-owner-thread';
    const resourceId = 'clear-owner-current';
    const record = await memory.initializeObservationalMemory({
      threadId,
      resourceId,
      scope: 'thread',
      config: {},
    });

    await expect(memory.clearObservationalMemory(threadId, 'clear-owner-stale')).rejects.toThrow(
      /resource.*does not own/i,
    );
    await expect(memory.getObservationalMemory(threadId, resourceId)).resolves.toMatchObject({ id: record.id });
  });

  it('rejects a recreated record with the same owner when its identity changed', async () => {
    const memory = new InMemoryMemory({ db: new InMemoryDB() });
    const threadId = 'clear-incarnation-thread';
    const resourceId = 'clear-incarnation-resource';
    const prior = await memory.initializeObservationalMemory({ threadId, resourceId, scope: 'thread', config: {} });
    await memory.clearObservationalMemory(threadId, resourceId);
    const current = await memory.initializeObservationalMemory({ threadId, resourceId, scope: 'thread', config: {} });

    await expect(memory.clearObservationalMemory(threadId, resourceId, { expectedRecordId: prior.id })).rejects.toThrow(
      /record changed/i,
    );
    await expect(memory.getObservationalMemory(threadId, resourceId)).resolves.toMatchObject({ id: current.id });
  });
});

describe('InMemoryMemory listMessages hasMore with include and a date filter', () => {
  let store: InMemoryMemory;
  const ids = Array.from({ length: 10 }, (_, minute) => `m${minute}`);
  // The filter keeps m4..m9; m0..m3 are outside it.
  const dateRange = { start: new Date(Date.UTC(2024, 0, 1, 0, 4)) };

  beforeEach(async () => {
    store = new InMemoryMemory({ db: new InMemoryDB() });
    await store.saveMessages({
      messages: ids.map((id, minute) =>
        makeMessage({ id, threadId: 'thread-1', resourceId: 'resource-1', text: id, minute }),
      ),
    });
  });

  it('reports more pages when include adds messages from outside the filter', async () => {
    const result = await store.listMessages({
      threadId: 'thread-1',
      perPage: 2,
      page: 0,
      filter: { dateRange },
      include: ['m0', 'm1', 'm2', 'm3'].map(id => ({ id })),
    });

    expect(result.total).toBe(6);
    expect(result.messages.map(message => message.id)).toEqual(['m0', 'm1', 'm2', 'm3', 'm4', 'm5']);
    expect(result.hasMore).toBe(true);
  });

  it('reports no more pages on the last page when include adds a message from outside the filter', async () => {
    const result = await store.listMessages({
      threadId: 'thread-1',
      perPage: 2,
      page: 2,
      filter: { dateRange },
      include: [{ id: 'm0' }],
    });

    expect(result.total).toBe(6);
    expect(result.messages.map(message => message.id)).toEqual(['m0', 'm8', 'm9']);
    expect(result.hasMore).toBe(false);
  });

  it('reports no more pages when include returns every filtered message', async () => {
    const result = await store.listMessages({
      threadId: 'thread-1',
      perPage: 2,
      page: 0,
      filter: { dateRange },
      include: ['m6', 'm7', 'm8', 'm9'].map(id => ({ id })),
    });

    expect(result.messages.map(message => message.id)).toEqual(['m4', 'm5', 'm6', 'm7', 'm8', 'm9']);
    expect(result.hasMore).toBe(false);
  });
});

describe('InMemoryMemory updateThread partial updates', () => {
  it('leaves the stored title alone when only metadata is provided', async () => {
    const memory = new InMemoryMemory({ db: new InMemoryDB() });
    await memory.saveThread({
      thread: {
        id: 'thread-1',
        resourceId: 'resource-1',
        title: 'Generated title',
        metadata: { a: 1 },
        createdAt: new Date(),
        updatedAt: new Date(),
      },
    });

    const updated = await memory.updateThread({ id: 'thread-1', metadata: { b: 2 } });

    expect(updated.title).toBe('Generated title');
    expect(updated.metadata).toEqual({ a: 1, b: 2 });
  });
});

describe('InMemoryMemory thread storage boundaries', () => {
  it('isolates nested clone metadata across clone ingress, clone results, and reads', async () => {
    class RuntimeHandle {
      readonly id = 'live-runtime';
    }

    const db = new InMemoryDB();
    const memory = new InMemoryMemory({ db });
    const createdAt = new Date('2026-01-01T00:00:00.000Z');
    const runtimeHandle = new RuntimeHandle();
    const cloneMetadata = {
      cloneOptions: {
        labels: ['persisted'],
        runtimeHandle,
      },
    };
    await memory.saveThread({
      thread: {
        id: 'metadata-source',
        resourceId: 'metadata-resource',
        createdAt,
        updatedAt: createdAt,
      },
    });
    const clone = await memory.cloneThread({
      sourceThreadId: 'metadata-source',
      newThreadId: 'metadata-clone',
      metadata: cloneMetadata,
    });

    cloneMetadata.cloneOptions.labels.push('caller-input-mutation');
    const returnedCloneOptions = clone.thread.metadata?.cloneOptions as {
      labels: string[];
      runtimeHandle: RuntimeHandle;
    };
    expect(returnedCloneOptions.labels).toEqual(['persisted']);
    expect(returnedCloneOptions.runtimeHandle).toBe(runtimeHandle);

    returnedCloneOptions.labels.push('clone-result-mutation');
    const fetched = await memory.getThreadById({ threadId: 'metadata-clone' });
    if (!fetched) throw new Error('Expected cloned thread.');
    const fetchedCloneOptions = fetched.metadata?.cloneOptions as { labels: string[] };
    expect(fetchedCloneOptions.labels).toEqual(['persisted']);

    fetchedCloneOptions.labels.push('getter-mutation');
    await expect(memory.getThreadById({ threadId: 'metadata-clone' })).resolves.toMatchObject({
      metadata: { cloneOptions: { labels: ['persisted'] } },
    });
  });

  it('keeps cloned governed metadata separate from caller and persisted objects', async () => {
    const db = new InMemoryDB();
    const memory = new InMemoryMemory({ db });
    const createdAt = new Date('2026-01-01T00:00:00.000Z');
    await memory.saveThread({
      thread: {
        id: 'source-thread',
        resourceId: 'source-resource',
        title: 'Source',
        createdAt,
        updatedAt: createdAt,
      },
    });
    const workingMemoryControl = {
      revision: 1,
      protectedPaths: ['/profile/name'],
      provenance: {
        '/profile/name': {
          source: 'owner',
          revision: 1,
          updatedAt: '2026-01-01T00:00:00.000Z',
        },
      },
    };

    const clone = await memory.cloneThread({
      sourceThreadId: 'source-thread',
      newThreadId: 'cloned-thread',
      metadata: {
        workingMemory: '{"profile":{"name":"Ada"}}',
        mastra: { workingMemory: workingMemoryControl },
      },
    });
    const stored = db.threads.get('cloned-thread');
    if (!stored) throw new Error('Expected cloned thread to be persisted.');
    const returnedControl = (clone.thread.metadata?.mastra as Record<string, unknown>)
      .workingMemory as typeof workingMemoryControl;
    const storedControl = (stored.metadata?.mastra as Record<string, unknown>)
      .workingMemory as typeof workingMemoryControl;

    expect(clone.thread).not.toBe(stored);
    expect(clone.thread.metadata).not.toBe(stored.metadata);
    expect(returnedControl).not.toBe(storedControl);
    workingMemoryControl.revision = 99;
    workingMemoryControl.protectedPaths.push('/tampered');
    expect(storedControl).toMatchObject({ revision: 1, protectedPaths: ['/profile/name'] });
    expect(() => returnedControl.protectedPaths.push('/caller-mutation')).toThrow(TypeError);
    await expect(memory.getThreadById({ threadId: 'cloned-thread' })).resolves.toMatchObject({
      metadata: {
        mastra: {
          workingMemory: { revision: 1, protectedPaths: ['/profile/name'] },
        },
      },
    });
  });

  it('freezes cyclic controls without overflowing before validation rejects them', async () => {
    const db = new InMemoryDB();
    const memory = new InMemoryMemory({ db });
    const createdAt = new Date('2026-01-01T00:00:00.000Z');
    const cyclicControl = () => {
      const provenance: Record<string, unknown> = {};
      provenance['/invalid'] = provenance;
      return { revision: 1, protectedPaths: [], provenance };
    };

    const thread = await memory.saveThread({
      thread: {
        id: 'cyclic-control-thread',
        resourceId: 'cyclic-control-resource',
        metadata: { mastra: { workingMemory: cyclicControl() } },
        createdAt,
        updatedAt: createdAt,
      },
    });
    const resource = await memory.saveResource({
      resource: {
        id: 'cyclic-control-resource',
        metadata: { mastra: { workingMemory: cyclicControl() } },
        createdAt,
        updatedAt: createdAt,
      },
    });

    expect(Object.isFrozen((thread.metadata?.mastra as Record<string, unknown>).workingMemory)).toBe(true);
    expect(Object.isFrozen((resource.metadata?.mastra as Record<string, unknown>).workingMemory)).toBe(true);
    await expect(
      memory.getWorkingMemorySnapshot({
        scope: 'thread',
        resourceId: 'cyclic-control-resource',
        threadId: 'cyclic-control-thread',
      }),
    ).rejects.toThrow('Stored working-memory controls are invalid.');
    await expect(
      memory.getWorkingMemorySnapshot({ scope: 'resource', resourceId: 'cyclic-control-resource' }),
    ).rejects.toThrow('Stored working-memory controls are invalid.');
  });

  it('keeps metadata undefined unless existing governed controls must be preserved', async () => {
    const db = new InMemoryDB();
    const memory = new InMemoryMemory({ db });
    const createdAt = new Date('2026-01-01T00:00:00.000Z');
    const thread = {
      id: 'shape-thread',
      resourceId: 'shape-resource',
      title: 'No metadata',
      createdAt,
      updatedAt: createdAt,
    };

    const saved = await memory.saveThread({ thread });

    expect(saved.metadata).toBeUndefined();
    expect(db.threads.get(thread.id)?.metadata).toBeUndefined();
    await expect(memory.getThreadById({ threadId: thread.id })).resolves.toMatchObject({ metadata: undefined });

    const governed = await memory.applyWorkingMemoryUpdate({
      scope: 'thread',
      resourceId: thread.resourceId,
      threadId: thread.id,
      value: '{"preference":"concise"}',
      expectedRevision: 0,
      source: 'owner',
      protectPaths: ['/preference'],
    });
    const resaved = await memory.saveThread({
      thread: { ...thread, updatedAt: new Date('2026-01-02T00:00:00.000Z') },
    });

    expect(resaved.metadata).toMatchObject({
      workingMemory: '{"preference":"concise"}',
      mastra: { workingMemory: { revision: 1, protectedPaths: ['/preference'] } },
    });
    await expect(
      memory.getWorkingMemorySnapshot({ scope: 'thread', resourceId: thread.resourceId, threadId: thread.id }),
    ).resolves.toEqual(governed);
  });
});

describe('InMemoryMemory swapBufferedReflectionToActive under a takeover', () => {
  const scopes = new AsyncLocalStorage<RunFenceScope | undefined>();
  const context = setRunFenceContext({ current: () => scopes.getStore(), run: (s, f) => scopes.run(s, f) });
  const inFence = <T>(fence: RunFence, fn: () => T): T => context.run({ fenceFor: () => fence }, fn);
  const bufferReflection = (memory: InMemoryMemory, id: string, reflection: string) =>
    memory.updateBufferedReflection({
      id,
      reflection,
      tokenCount: 1,
      inputTokenCount: 1,
      reflectedObservationLineCount: 0,
    });

  it('keeps a reflection the new owner buffers while a superseded swap is in flight', async () => {
    const memory = new InMemoryMemory({ db: new InMemoryDB() });
    const fenceA = { runId: 'run-1', generation: 1, ownerId: 'owner-a' };
    const fenceB = { runId: 'run-1', generation: 2, ownerId: 'owner-b' };
    await memory.raiseRunFence(fenceA);
    const record = await memory.initializeObservationalMemory({
      threadId: null,
      resourceId: 'resource-1',
      scope: 'resource',
      config: { observationThreshold: 5000, reflectionThreshold: 40000 },
    });
    await inFence(fenceA, () => bufferReflection(memory, record.id, 'a reflection'));

    // Start A's swap, then take over and buffer as B before the swap settles.
    const swap = inFence(fenceA, () => memory.swapBufferedReflectionToActive({ currentRecord: record, tokenCount: 1 }));
    const takeover = memory.raiseRunFence(fenceB);
    const write = inFence(fenceB, () => bufferReflection(memory, record.id, 'b reflection'));
    await Promise.all([swap, takeover, write]);

    const history = await memory.getObservationalMemoryHistory(null, 'resource-1');
    expect(history.find(r => r.id === record.id)?.bufferedReflection).toBe('b reflection');
  });
});
