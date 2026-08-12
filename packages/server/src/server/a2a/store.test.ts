import type { Task } from '@mastra/core/a2a';
import { describe, expect, it } from 'vitest';
import { InMemoryTaskStore } from './store';

function createTask(overrides: Partial<Task> & Pick<Task, 'id'> = { id: 'task-1' }): Task {
  return {
    id: overrides.id,
    contextId: overrides.contextId ?? 'context-1',
    status: overrides.status ?? {
      state: 'working',
      timestamp: '2025-05-08T11:47:38.458Z',
    },
    artifacts: overrides.artifacts ?? [],
    metadata: overrides.metadata,
    kind: 'task',
  };
}

describe('InMemoryTaskStore', () => {
  it('returns a task and version atomically via loadWithVersion', async () => {
    const store = new InMemoryTaskStore();
    const task = createTask();

    await store.save({ agentId: 'agent-1', data: task });

    expect(store.loadWithVersion({ agentId: 'agent-1', taskId: 'task-1' })).toEqual({
      task,
      version: 1,
    });
  });

  it('rejects a save when the expected version is stale', async () => {
    const store = new InMemoryTaskStore();
    const task = createTask();

    await store.save({ agentId: 'agent-1', data: task });
    await store.save({
      agentId: 'agent-1',
      data: createTask({ id: 'task-1', status: { state: 'completed' } }),
      expectedVersion: 1,
    });

    await expect(
      store.save({
        agentId: 'agent-1',
        data: createTask({ id: 'task-1', status: { state: 'failed' } }),
        expectedVersion: 1,
      }),
    ).rejects.toMatchObject({ name: 'TaskStoreVersionConflictError' });
    expect((await store.load({ agentId: 'agent-1', taskId: 'task-1' }))?.status.state).toBe('completed');
  });

  it('waitForNextUpdate resolves immediately when a newer version already exists', async () => {
    const store = new InMemoryTaskStore();
    const task = createTask();

    await store.save({ agentId: 'agent-1', data: task });
    await store.save({
      agentId: 'agent-1',
      data: createTask({
        id: 'task-1',
        status: {
          state: 'completed',
          timestamp: '2025-05-08T11:48:38.458Z',
        },
      }),
    });

    await expect(
      store.waitForNextUpdate({
        agentId: 'agent-1',
        taskId: 'task-1',
        afterVersion: 1,
      }),
    ).resolves.toEqual({
      task: createTask({
        id: 'task-1',
        status: {
          state: 'completed',
          timestamp: '2025-05-08T11:48:38.458Z',
        },
      }),
      version: 2,
    });
  });

  it('keeps a canceled task when an opt-in stale save tries to overwrite it', async () => {
    const store = new InMemoryTaskStore();

    await store.save({
      agentId: 'agent-1',
      data: createTask({
        id: 'task-1',
        status: {
          state: 'canceled',
          timestamp: '2025-05-08T11:48:38.458Z',
        },
      }),
    });

    const storedTask = await store.save({
      agentId: 'agent-1',
      skipIfCanceled: true,
      data: createTask({
        id: 'task-1',
        status: {
          state: 'completed',
          timestamp: '2025-05-08T11:49:38.458Z',
        },
      }),
    });

    const canceledTask = createTask({
      id: 'task-1',
      status: {
        state: 'canceled',
        timestamp: '2025-05-08T11:48:38.458Z',
      },
    });

    expect(storedTask).toEqual(canceledTask);
    await expect(store.load({ agentId: 'agent-1', taskId: 'task-1' })).resolves.toEqual(canceledTask);
    expect(store.getVersion({ agentId: 'agent-1', taskId: 'task-1' })).toBe(1);
  });

  it('isolates agent and task identifiers that would collide with delimiter-based keys', async () => {
    const store = new InMemoryTaskStore();
    const first = createTask({ id: 'b-c', metadata: { owner: 'a' } });
    const second = createTask({ id: 'c', metadata: { owner: 'a-b' } });

    await store.save({ agentId: 'a', data: first });
    await store.save({ agentId: 'a-b', data: second });

    await expect(store.load({ agentId: 'a', taskId: 'b-c' })).resolves.toEqual(first);
    await expect(store.load({ agentId: 'a-b', taskId: 'c' })).resolves.toEqual(second);
    expect(store.list({ agentId: 'a' })).toEqual([first]);
    expect(store.list({ agentId: 'a-b' })).toEqual([second]);
    expect(store.getVersion({ agentId: 'a', taskId: 'b-c' })).toBe(1);
    expect(store.getVersion({ agentId: 'a-b', taskId: 'c' })).toBe(1);
  });

  it('aborts every concurrent controller registered for one task', () => {
    const store = new InMemoryTaskStore();
    const first = new AbortController();
    const second = new AbortController();

    store.registerAbortController({ agentId: 'agent-1', taskId: 'task-1', controller: first });
    store.registerAbortController({ agentId: 'agent-1', taskId: 'task-1', controller: second });
    store.abortTask({ agentId: 'agent-1', taskId: 'task-1', reason: 'canceled' });

    expect(first.signal.aborted).toBe(true);
    expect(first.signal.reason).toBe('canceled');
    expect(second.signal.aborted).toBe(true);
    expect(second.signal.reason).toBe('canceled');
  });

  it('unregisters one controller without removing its concurrent sibling', () => {
    const store = new InMemoryTaskStore();
    const first = new AbortController();
    const second = new AbortController();
    const unregisterFirst = store.registerAbortController({
      agentId: 'agent-1',
      taskId: 'task-1',
      controller: first,
    });
    store.registerAbortController({ agentId: 'agent-1', taskId: 'task-1', controller: second });

    unregisterFirst();
    store.abortTask({ agentId: 'agent-1', taskId: 'task-1' });

    expect(first.signal.aborted).toBe(false);
    expect(second.signal.aborted).toBe(true);
  });

  it('waitForNextUpdate removes listeners and rejects on abort', async () => {
    const store = new InMemoryTaskStore();
    const task = createTask();

    await store.save({ agentId: 'agent-1', data: task });

    const controller = new AbortController();
    const wait = store.waitForNextUpdate({
      agentId: 'agent-1',
      taskId: 'task-1',
      afterVersion: 1,
      signal: controller.signal,
    });

    const taskKey = JSON.stringify(['agent-1', 'task-1']);
    expect(((store as any).listeners.get(taskKey) as Set<unknown> | undefined)?.size).toBe(1);

    controller.abort();

    await expect(wait).rejects.toMatchObject({ name: 'AbortError' });
    expect((store as any).listeners.has(taskKey)).toBe(false);
  });
});
