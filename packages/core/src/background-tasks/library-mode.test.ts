import { describe, it, expect, vi } from 'vitest';
import { EventEmitterPubSub } from '../events/event-emitter';
import { Mastra } from '../mastra';
import { MockStore } from '../storage';
import { createTool } from '../tools';
import { MastraWorker } from '../worker';
import { createBackgroundTask } from './create';
import type { BackgroundTask, TaskContext } from './types';

/**
 * "Library mode" coverage: the app constructs a Mastra and dispatches
 * background tasks without ever calling `mastra.startWorkers()` (no server,
 * no `mastra dev`). Dispatch/resume must lazily start the execution workers
 * or the task's evented workflow has no consumer and the task sits at
 * `running` forever.
 */

function ctx(executeFn: (args: any, opts?: any) => Promise<any>): TaskContext {
  return { executor: { execute: executeFn } };
}

const tick = (ms = 200) => new Promise(resolve => setTimeout(resolve, ms));

class PushOnlyEventEmitterPubSub extends EventEmitterPubSub {
  override get supportedModes(): ReadonlyArray<'push'> {
    return ['push'];
  }
}

class ControlledWorker extends MastraWorker {
  #running = false;
  readonly startSpy = vi.fn();
  readonly stopSpy = vi.fn();

  constructor(
    readonly name: string,
    private readonly startImplementation: () => Promise<void> = async () => {},
  ) {
    super();
  }

  async start(): Promise<void> {
    if (this.#running) return;
    this.startSpy();
    await this.startImplementation();
    this.#running = true;
  }

  async stop(): Promise<void> {
    if (!this.#running) return;
    this.stopSpy();
    this.#running = false;
  }

  get isRunning(): boolean {
    return this.#running;
  }
}

function makeLibraryModeMastra(overrides: ConstructorParameters<typeof Mastra>[0] = {}) {
  // NOTE: deliberately no `startWorkers()` call anywhere in this file's
  // happy paths — that is the scenario under test.
  return new Mastra({
    logger: false,
    storage: new MockStore(),
    backgroundTasks: { enabled: true },
    ...overrides,
  });
}

describe('background tasks in library mode (no startWorkers)', () => {
  it('completes a dispatched task', async () => {
    const mastra = makeLibraryModeMastra();
    const manager = mastra.backgroundTaskManager!;

    const executeFn = vi.fn().mockResolvedValue({ ok: true });
    const handle = createBackgroundTask(manager, {
      toolName: 'lib-tool',
      toolCallId: 'call-1',
      args: { q: 'x' },
      agentId: 'agent-1',
      runId: 'run-1',
      context: ctx(executeFn),
    });

    const { task } = await handle.dispatch();
    const completed = await manager.waitForNextTask([task.id], { timeoutMs: 5000 });

    expect(completed.status).toBe('completed');
    expect(completed.result).toEqual({ ok: true });
    expect(executeFn).toHaveBeenCalledTimes(1);

    await mastra.stopWorkers();
  });

  it('completes concurrent first dispatches on a cold instance, each executor exactly once', async () => {
    const mastra = makeLibraryModeMastra();
    const manager = mastra.backgroundTaskManager!;

    const executeA = vi.fn().mockResolvedValue('a');
    const executeB = vi.fn().mockResolvedValue('b');

    const [{ task: taskA }, { task: taskB }] = await Promise.all([
      manager.enqueue({ toolName: 't-a', toolCallId: 'ca', args: {}, agentId: 'a1', runId: 'r1' }, ctx(executeA)),
      manager.enqueue({ toolName: 't-b', toolCallId: 'cb', args: {}, agentId: 'a1', runId: 'r1' }, ctx(executeB)),
    ]);

    const [doneA, doneB] = await Promise.all([
      manager.waitForNextTask([taskA.id], { timeoutMs: 5000 }),
      manager.waitForNextTask([taskB.id], { timeoutMs: 5000 }),
    ]);

    expect(doneA.status).toBe('completed');
    expect(doneB.status).toBe('completed');
    expect(executeA).toHaveBeenCalledTimes(1);
    expect(executeB).toHaveBeenCalledTimes(1);

    await mastra.stopWorkers();
  });

  it('suspends and resumes a task', async () => {
    const mastra = makeLibraryModeMastra();
    const manager = mastra.backgroundTaskManager!;

    const executeFn = vi.fn(async (_args, opts: any) => {
      if (!opts.resumeData) {
        await opts.suspend({ awaiting: 'approval' });
        return undefined;
      }
      return { approvedBy: (opts.resumeData as { user: string }).user };
    });

    const { task } = await manager.enqueue(
      { toolName: 't', toolCallId: 'c1', args: {}, agentId: 'a1', runId: 'r1' },
      ctx(executeFn),
    );
    await tick();
    expect((await manager.getTask(task.id))?.status).toBe('suspended');

    await manager.resume(task.id, { user: 'alice' });
    const completed = await manager.waitForNextTask([task.id], { timeoutMs: 5000 });

    expect(completed.status).toBe('completed');
    expect(completed.result).toEqual({ approvedBy: 'alice' });

    await mastra.stopWorkers();
  });

  it('completes a task dispatched after stopWorkers()', async () => {
    const mastra = makeLibraryModeMastra();
    const manager = mastra.backgroundTaskManager!;

    await mastra.startWorkers();
    await mastra.stopWorkers();

    const executeFn = vi.fn().mockResolvedValue('after-stop');
    const { task } = await manager.enqueue(
      { toolName: 't', toolCallId: 'c1', args: {}, agentId: 'a1', runId: 'r1' },
      ctx(executeFn),
    );

    const completed = await manager.waitForNextTask([task.id], { timeoutMs: 5000 });
    expect(completed.status).toBe('completed');
    expect(executeFn).toHaveBeenCalledTimes(1);

    await mastra.stopWorkers();
  });

  it('completes a task when only the backgroundTasks worker was started by name', async () => {
    const mastra = makeLibraryModeMastra();
    const manager = mastra.backgroundTaskManager!;

    // A named partial start marks the boot path as run but never starts the
    // orchestration worker or push wiring — the lazy path must still supply
    // the missing workflow consumer.
    await mastra.startWorkers('backgroundTasks');

    const executeFn = vi.fn().mockResolvedValue('ok');
    const { task } = await manager.enqueue(
      { toolName: 't', toolCallId: 'c1', args: {}, agentId: 'a1', runId: 'r1' },
      ctx(executeFn),
    );

    const completed = await manager.waitForNextTask([task.id], { timeoutMs: 5000 });
    expect(completed.status).toBe('completed');
    expect(executeFn).toHaveBeenCalledTimes(1);

    await mastra.stopWorkers();
  });

  it('recovers and completes a pending task from a previous process via the static tool registry', async () => {
    // Seed a pending task (as if a previous process enqueued it and died
    // before dispatch) BEFORE constructing Mastra, so the manager's
    // constructor-time recovery is what picks it up.
    const storage = new MockStore();
    const bgStore = await storage.getStore('backgroundTasks');
    const staleTask: BackgroundTask = {
      id: 'stale-1',
      status: 'pending',
      toolName: 'recoverable-tool',
      toolCallId: 'call-stale',
      args: {},
      agentId: 'agent-1',
      runId: 'run-stale',
      retryCount: 0,
      maxRetries: 0,
      timeoutMs: 5000,
      createdAt: new Date(),
    };
    await bgStore!.createTask(staleTask);

    const executeFn = vi.fn().mockResolvedValue({ recovered: true });
    const mastra = makeLibraryModeMastra({
      storage,
      tools: {
        'recoverable-tool': createTool({
          id: 'recoverable-tool',
          description: 'recoverable',
          execute: executeFn,
        }),
      },
    });

    const manager = mastra.backgroundTaskManager!;
    const completed = await manager.waitForNextTask([staleTask.id], { timeoutMs: 5000 });

    expect(completed.status).toBe('completed');
    expect(executeFn).toHaveBeenCalledTimes(1);

    await mastra.stopWorkers();
  });

  it('tears down workers started by a lazy start still in flight when stopWorkers() is called', async () => {
    const pubsub = new EventEmitterPubSub();
    const mastra = makeLibraryModeMastra({ pubsub });
    const manager = mastra.backgroundTaskManager!;

    // Gate the orchestration worker's subscription so the lazy start is
    // reliably still in flight when stopWorkers() runs.
    const originalSubscribe = pubsub.subscribe.bind(pubsub);
    let release!: () => void;
    const gate = new Promise<void>(resolve => (release = resolve));
    const subscribeSpy = vi.spyOn(pubsub, 'subscribe').mockImplementation(async (topic, cb, options) => {
      if (options?.group === 'mastra-orchestration') await gate;
      return originalSubscribe(topic, cb, options);
    });
    const unsubscribeSpy = vi.spyOn(pubsub, 'unsubscribe');

    const enqueued = manager.enqueue(
      { toolName: 't', toolCallId: 'c1', args: {}, agentId: 'a1', runId: 'r1' },
      ctx(vi.fn().mockResolvedValue('x')),
    );
    await vi.waitFor(() => {
      expect(subscribeSpy.mock.calls.some(call => call[2]?.group === 'mastra-orchestration')).toBe(true);
    });

    // Stop while the lazy start is parked on the gated subscribe, then let
    // the start finish. stopWorkers() must not leave the freshly started
    // workers running behind its back.
    const stopping = mastra.stopWorkers();
    release();
    await stopping;
    await enqueued;
    await tick();

    const orchestrationCbs = subscribeSpy.mock.calls
      .filter(call => call[2]?.group === 'mastra-orchestration')
      .map(call => call[1]);
    expect(orchestrationCbs.length).toBeGreaterThan(0);
    const unsubscribed = unsubscribeSpy.mock.calls.map(call => call[1]);
    for (const cb of orchestrationCbs) {
      expect(unsubscribed).toContain(cb);
    }

    await mastra.stopWorkers();
  });

  it('serializes full and lazy startup without rolling back the lazy-owned push consumer', async () => {
    const pubsub = new PushOnlyEventEmitterPubSub();
    const originalSubscribe = pubsub.subscribe.bind(pubsub);
    const startupFailure = new Error('custom listener failed to subscribe');
    const subscribeSpy = vi.spyOn(pubsub, 'subscribe').mockImplementation(async (topic, cb, options) => {
      if (topic === 'custom') throw startupFailure;
      return originalSubscribe(topic, cb, options);
    });
    const unsubscribeSpy = vi.spyOn(pubsub, 'unsubscribe');
    let releaseFirstWorker!: () => void;
    const firstWorkerGate = new Promise<void>(resolve => {
      releaseFirstWorker = resolve;
    });
    const orchestration = new ControlledWorker('orchestration', () => firstWorkerGate);
    const backgroundTasks = new ControlledWorker('backgroundTasks');
    const mastra = new Mastra({
      logger: false,
      storage: new MockStore(),
      pubsub,
      workers: [orchestration, backgroundTasks],
      events: { custom: async () => {} },
    });

    const lazyStart = mastra.__ensureExecutionWorkersStarted();
    await vi.waitFor(() => expect(orchestration.startSpy).toHaveBeenCalledOnce());
    const fullStart = mastra.startWorkers();
    const fullStartRejection = expect(fullStart).rejects.toBe(startupFailure);

    releaseFirstWorker();
    await lazyStart;
    await fullStartRejection;

    const workflowPushSubscriptions = subscribeSpy.mock.calls.filter(
      ([topic, , options]) => topic === 'workflows' && options?.group === undefined,
    );
    expect(workflowPushSubscriptions).toHaveLength(1);
    expect(unsubscribeSpy.mock.calls.map(([, cb]) => cb)).not.toContain(workflowPushSubscriptions[0]![1]);
    expect(orchestration.isRunning).toBe(true);
    expect(backgroundTasks.isRunning).toBe(true);

    await mastra.__ensureExecutionWorkersStarted();
    expect(orchestration.startSpy).toHaveBeenCalledOnce();
    expect(backgroundTasks.startSpy).toHaveBeenCalledOnce();
    expect(subscribeSpy.mock.calls.filter(([topic]) => topic === 'workflows')).toHaveLength(1);

    await mastra.stopWorkers();
  });

  it('rolls back execution workers started before a later lazy worker fails', async () => {
    const startupFailure = new Error('background worker failed to start');
    const orchestration = new ControlledWorker('orchestration');
    const backgroundTasks = new ControlledWorker('backgroundTasks', async () => {
      throw startupFailure;
    });
    const mastra = new Mastra({
      logger: false,
      storage: new MockStore(),
      workers: [orchestration, backgroundTasks],
    });

    await expect(mastra.__ensureExecutionWorkersStarted()).rejects.toBe(startupFailure);
    expect(orchestration.startSpy).toHaveBeenCalledOnce();
    expect(orchestration.stopSpy).toHaveBeenCalledOnce();
    expect(orchestration.isRunning).toBe(false);
    expect(backgroundTasks.isRunning).toBe(false);

    await mastra.stopWorkers();
  });

  it('creates a producer-only background-task manager when workers: false', () => {
    // `workers: false` means the consumer lives in a separate worker process.
    // The manager is still created in producer mode so this instance can
    // dispatch tasks, but it never subscribes to the dispatch topic.
    const mastra = makeLibraryModeMastra({ workers: false });
    const manager = mastra.backgroundTaskManager;
    expect(manager).toBeDefined();
    expect(manager!.config.mode).toBe('producer');
  });

  it('does not lazily start execution workers excluded by the MASTRA_WORKERS filter', async () => {
    process.env.MASTRA_WORKERS = 'scheduler';
    let mastra: Mastra;
    try {
      mastra = makeLibraryModeMastra();
    } finally {
      delete process.env.MASTRA_WORKERS;
    }
    const manager = mastra.backgroundTaskManager!;

    const executeFn = vi.fn().mockResolvedValue('never');
    const { task } = await manager.enqueue(
      { toolName: 't', toolCallId: 'c1', args: {}, agentId: 'a1', runId: 'r1' },
      ctx(executeFn),
    );
    await tick(500);

    // With orchestration/backgroundTasks filtered out, the task's evented
    // workflow has no local consumer — a separate worker process is expected
    // to pick it up, so this instance must not execute it.
    expect(executeFn).not.toHaveBeenCalled();
    expect((await manager.getTask(task.id))?.status).not.toBe('completed');

    await mastra.stopWorkers();
  });
});
