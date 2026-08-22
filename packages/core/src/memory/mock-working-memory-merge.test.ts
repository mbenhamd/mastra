import { describe, it, expect, vi } from 'vitest';
import { z } from 'zod/v4';
import { InMemoryStore } from '../storage';
import { MockMemory } from './mock';

describe('MockMemory working memory merge semantics', () => {
  const threadId = 'thread-1';
  const resourceId = 'resource-1';

  async function setupMemory(useSchema: boolean) {
    const options: ConstructorParameters<typeof MockMemory>[0] = {
      enableWorkingMemory: true,
    };

    if (useSchema) {
      options.options = {
        workingMemory: {
          enabled: true,
          schema: z.object({
            name: z.string().optional(),
            age: z.number().optional(),
            location: z.string().optional(),
          }),
        },
      };
    }

    const memory = new MockMemory(options);

    // Create a thread so the tool doesn't error
    await memory.createThread({ threadId, resourceId });

    return memory;
  }

  async function callUpdateTool(memory: MockMemory, input: string) {
    const config = (memory as any).getMergedThreadConfig();
    const tools = memory.listTools(config);
    const tool = tools.updateWorkingMemory;
    if (!tool) throw new Error('updateWorkingMemory tool not found');

    await (tool as any).execute({ memory: input }, { agent: { threadId, resourceId }, memory });
  }

  it('round-trips thread-scoped working memory through thread metadata', async () => {
    const memory = new MockMemory({
      enableWorkingMemory: true,
      options: { workingMemory: { enabled: true, scope: 'thread' } },
    });
    await memory.createThread({ threadId, resourceId });

    await memory.updateWorkingMemory({
      threadId,
      resourceId,
      workingMemory: 'thread-scoped value',
    });

    await expect(memory.getWorkingMemory({ threadId, resourceId })).resolves.toBe('thread-scoped value');
  });

  it('rejects thread-scoped working memory access from a different supplied resource', async () => {
    const memory = new MockMemory({
      enableWorkingMemory: true,
      options: { workingMemory: { enabled: true, scope: 'thread' } },
    });
    await memory.createThread({ threadId, resourceId });
    await memory.updateWorkingMemory({
      threadId,
      resourceId,
      workingMemory: 'thread-scoped value',
    });

    await expect(memory.getWorkingMemory({ threadId, resourceId: 'resource-2' })).rejects.toThrow(
      'Working-memory thread does not belong to the requested resource.',
    );
  });

  it('does not resurrect an owner-forgotten value after an observer revision conflict', async () => {
    const storage = new InMemoryStore({ id: 'mock-owner-forget-conflict' });
    const memory = new MockMemory({ storage, enableWorkingMemory: true });
    await memory.createThread({ threadId, resourceId });
    await memory.updateWorkingMemory({ threadId, resourceId, workingMemory: 'initial value' });

    const memoryStore = await storage.getStore('memory');
    if (!memoryStore) throw new Error('Expected in-memory storage domain.');
    const applyUpdate = memoryStore.applyWorkingMemoryUpdate.bind(memoryStore);
    vi.spyOn(memoryStore, 'applyWorkingMemoryUpdate').mockImplementationOnce(async input => {
      const current = await memoryStore.getWorkingMemorySnapshot(input);
      await applyUpdate({
        scope: input.scope,
        resourceId: input.resourceId,
        ...(input.threadId ? { threadId: input.threadId } : {}),
        value: null,
        expectedRevision: current.revision,
        source: 'owner',
      });
      return applyUpdate(input);
    });

    await expect(
      memory.updateWorkingMemory({ threadId, resourceId, workingMemory: 'stale observer proposal' }),
    ).rejects.toMatchObject({ name: 'WorkingMemoryRevisionConflictError' });
    await expect(memory.getWorkingMemorySnapshot({ threadId, resourceId })).resolves.toMatchObject({ value: null });
  });

  it('does not overwrite a newer observer value after an observer revision conflict', async () => {
    const storage = new InMemoryStore({ id: 'mock-observer-conflict' });
    const memory = new MockMemory({ storage, enableWorkingMemory: true });
    await memory.createThread({ threadId, resourceId });
    await memory.updateWorkingMemory({ threadId, resourceId, workingMemory: 'initial value' });

    const memoryStore = await storage.getStore('memory');
    if (!memoryStore) throw new Error('Expected in-memory storage domain.');
    const applyUpdate = memoryStore.applyWorkingMemoryUpdate.bind(memoryStore);
    vi.spyOn(memoryStore, 'applyWorkingMemoryUpdate').mockImplementationOnce(async input => {
      const current = await memoryStore.getWorkingMemorySnapshot(input);
      await applyUpdate({
        scope: input.scope,
        resourceId: input.resourceId,
        ...(input.threadId ? { threadId: input.threadId } : {}),
        value: 'newer observer value',
        expectedRevision: current.revision,
        source: 'observer',
      });
      return applyUpdate(input);
    });

    await expect(
      memory.updateWorkingMemory({ threadId, resourceId, workingMemory: 'stale observer proposal' }),
    ).rejects.toMatchObject({ name: 'WorkingMemoryRevisionConflictError' });
    await expect(memory.getWorkingMemorySnapshot({ threadId, resourceId })).resolves.toMatchObject({
      value: 'newer observer value',
    });
  });

  it('replaces working memory entirely for template-based (no schema)', async () => {
    const memory = await setupMemory(false);

    await callUpdateTool(memory, JSON.stringify({ name: 'Alice', age: 30, location: 'NYC' }));
    await callUpdateTool(memory, JSON.stringify({ location: 'LA' }));

    const wm = await memory.getWorkingMemory({ threadId, resourceId });
    const parsed = JSON.parse(wm!);
    expect(parsed).toEqual({ location: 'LA' });
    expect(parsed.name).toBeUndefined();
  });

  it('merges working memory for schema-based configs', async () => {
    const memory = await setupMemory(true);

    await callUpdateTool(memory, JSON.stringify({ name: 'Alice', age: 30, location: 'NYC' }));
    await callUpdateTool(memory, JSON.stringify({ location: 'LA' }));

    const wm = await memory.getWorkingMemory({ threadId, resourceId });
    const parsed = JSON.parse(wm!);
    expect(parsed).toEqual({ name: 'Alice', age: 30, location: 'LA' });
  });

  it('overwrites fields in schema-based merge when explicitly provided', async () => {
    const memory = await setupMemory(true);

    await callUpdateTool(memory, JSON.stringify({ name: 'Alice', age: 30 }));
    await callUpdateTool(memory, JSON.stringify({ name: 'Bob', age: 25 }));

    const wm = await memory.getWorkingMemory({ threadId, resourceId });
    const parsed = JSON.parse(wm!);
    expect(parsed).toEqual({ name: 'Bob', age: 25 });
  });

  it('handles first write with no existing data in schema mode', async () => {
    const memory = await setupMemory(true);

    await callUpdateTool(memory, JSON.stringify({ name: 'Alice' }));

    const wm = await memory.getWorkingMemory({ threadId, resourceId });
    const parsed = JSON.parse(wm!);
    expect(parsed).toEqual({ name: 'Alice' });
  });

  it('deep-merges nested objects in schema mode', async () => {
    const memory = new MockMemory({
      enableWorkingMemory: true,
      options: {
        workingMemory: {
          enabled: true,
          schema: z.object({
            user: z.object({
              name: z.string().optional(),
              address: z
                .object({
                  city: z.string().optional(),
                  state: z.string().optional(),
                })
                .optional(),
            }),
          }),
        },
      },
    });
    await memory.createThread({ threadId, resourceId });

    await callUpdateTool(memory, JSON.stringify({ user: { name: 'Alice', address: { city: 'NYC', state: 'NY' } } }));
    await callUpdateTool(memory, JSON.stringify({ user: { address: { city: 'LA' } } }));

    const wm = await memory.getWorkingMemory({ threadId, resourceId });
    const parsed = JSON.parse(wm!);
    // Deep merge: name preserved, state preserved, only city changed
    expect(parsed).toEqual({ user: { name: 'Alice', address: { city: 'LA', state: 'NY' } } });
  });

  it('deletes keys set to null in schema mode', async () => {
    const memory = await setupMemory(true);

    await callUpdateTool(memory, JSON.stringify({ name: 'Alice', age: 30, location: 'NYC' }));
    await callUpdateTool(memory, JSON.stringify({ age: null }));

    const wm = await memory.getWorkingMemory({ threadId, resourceId });
    const parsed = JSON.parse(wm!);
    // null deletes the key
    expect(parsed).toEqual({ name: 'Alice', location: 'NYC' });
    expect(parsed.age).toBeUndefined();
  });

  it('deletes keys set to null on the first write in schema mode', async () => {
    const memory = await setupMemory(true);

    await callUpdateTool(memory, JSON.stringify({ name: 'Alice', age: null }));

    const wm = await memory.getWorkingMemory({ threadId, resourceId });
    const parsed = JSON.parse(wm!);
    expect(parsed).toEqual({ name: 'Alice' });
    expect('age' in parsed).toBe(false);
  });

  it('deletes keys set to null inside a newly created nested object in schema mode', async () => {
    const memory = new MockMemory({
      enableWorkingMemory: true,
      options: {
        workingMemory: {
          enabled: true,
          schema: z.object({
            name: z.string().optional(),
            user: z
              .object({
                name: z.string().optional(),
                city: z.string().optional(),
              })
              .optional(),
          }),
        },
      },
    });
    await memory.createThread({ threadId, resourceId });

    await callUpdateTool(memory, JSON.stringify({ name: 'Alice' }));
    await callUpdateTool(memory, JSON.stringify({ user: { name: 'Bob', city: null } }));

    const wm = await memory.getWorkingMemory({ threadId, resourceId });
    const parsed = JSON.parse(wm!);
    expect(parsed).toEqual({ name: 'Alice', user: { name: 'Bob' } });
  });

  it('replaces arrays entirely in schema mode', async () => {
    const memory = new MockMemory({
      enableWorkingMemory: true,
      options: {
        workingMemory: {
          enabled: true,
          schema: z.object({
            tags: z.array(z.string()).optional(),
            count: z.number().optional(),
          }),
        },
      },
    });
    await memory.createThread({ threadId, resourceId });

    await callUpdateTool(memory, JSON.stringify({ tags: ['a', 'b', 'c'], count: 3 }));
    await callUpdateTool(memory, JSON.stringify({ tags: ['x'] }));

    const wm = await memory.getWorkingMemory({ threadId, resourceId });
    const parsed = JSON.parse(wm!);
    // Arrays replace, count preserved
    expect(parsed).toEqual({ tags: ['x'], count: 3 });
  });
});
