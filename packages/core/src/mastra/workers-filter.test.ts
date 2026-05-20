import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { Harness } from '../harness/v1/harness';
import { InMemoryHarness } from '../storage/domains/harness/inmemory';
import { InMemoryDB } from '../storage/domains/inmemory-db';
import { MockStore } from '../storage/mock';
import { HarnessWakeupWorker } from '../worker';
import { Mastra } from './index';

const ORIGINAL_ENV = process.env.MASTRA_WORKERS;

describe('Mastra workers filter (MASTRA_WORKERS env)', () => {
  beforeEach(() => {
    delete process.env.MASTRA_WORKERS;
  });

  afterEach(() => {
    if (ORIGINAL_ENV === undefined) {
      delete process.env.MASTRA_WORKERS;
    } else {
      process.env.MASTRA_WORKERS = ORIGINAL_ENV;
    }
    vi.restoreAllMocks();
  });

  it('starts only the named workers when MASTRA_WORKERS=a,b is set', async () => {
    process.env.MASTRA_WORKERS = 'scheduler,backgroundTasks';

    const mastra = new Mastra({
      storage: new MockStore(),
      backgroundTasks: { enabled: true },
      logger: false,
    });

    // Spy on each worker's start method.
    const starts = mastra.workers.map(w => ({
      name: w.name,
      spy: vi.spyOn(w, 'start').mockResolvedValue(undefined),
      initSpy: vi.spyOn(w, 'init').mockResolvedValue(undefined),
    }));

    await mastra.startWorkers();

    const started = starts.filter(s => s.spy.mock.calls.length > 0).map(s => s.name);
    expect(started.sort()).toEqual(['backgroundTasks', 'scheduler']);

    // orchestration was not started
    const orchestration = starts.find(s => s.name === 'orchestration');
    expect(orchestration?.spy).not.toHaveBeenCalled();
  });

  it('starts all workers when MASTRA_WORKERS is unset', async () => {
    const mastra = new Mastra({
      storage: new MockStore(),
      backgroundTasks: { enabled: true },
      logger: false,
    });

    const starts = mastra.workers.map(w => ({
      name: w.name,
      spy: vi.spyOn(w, 'start').mockResolvedValue(undefined),
      initSpy: vi.spyOn(w, 'init').mockResolvedValue(undefined),
    }));

    await mastra.startWorkers();

    for (const s of starts) {
      expect(s.spy, `worker ${s.name} should have started`).toHaveBeenCalled();
    }
  });

  it('registers harness wakeup worker for harness session storage without top-level storage', () => {
    const mastra = new Mastra({
      harness: new Harness({
        modes: [],
        sessions: { storage: new InMemoryHarness({ db: new InMemoryDB() }) },
      }),
      logger: false,
    });

    expect(mastra.workers.map(worker => worker.name)).toContain('harnessWakeups');
  });

  it('registers harness wakeup worker when storage is attached after construction', () => {
    const mastra = new Mastra({
      harness: new Harness({ modes: [] }),
      logger: false,
    });

    expect(mastra.workers.map(worker => worker.name)).not.toContain('harnessWakeups');

    mastra.setStorage(new MockStore());

    expect(mastra.workers.map(worker => worker.name)).toContain('harnessWakeups');
  });

  it('starts a late-registered harness wakeup worker when workers are already running', async () => {
    const init = vi.spyOn(HarnessWakeupWorker.prototype, 'init').mockResolvedValue(undefined);
    const start = vi.spyOn(HarnessWakeupWorker.prototype, 'start').mockResolvedValue(undefined);
    const mastra = new Mastra({
      harness: new Harness({ modes: [] }),
      logger: false,
    });

    await mastra.startWorkers();
    mastra.setStorage(new MockStore());

    await vi.waitFor(() => {
      expect(start).toHaveBeenCalledTimes(1);
    });
    expect(init).toHaveBeenCalledTimes(1);
  });

  it('starts a late-registered harness wakeup worker while workers are still starting', async () => {
    const init = vi.spyOn(HarnessWakeupWorker.prototype, 'init').mockResolvedValue(undefined);
    const start = vi.spyOn(HarnessWakeupWorker.prototype, 'start').mockResolvedValue(undefined);
    const mastra = new Mastra({
      harness: new Harness({ modes: [] }),
      logger: false,
    });
    const initialWorker = mastra.workers[0];
    if (!initialWorker) {
      throw new Error('expected an initial worker to hold startWorkers in flight');
    }
    let releaseStart!: () => void;
    vi.spyOn(initialWorker, 'init').mockResolvedValue(undefined);
    const initialStart = vi.spyOn(initialWorker, 'start').mockImplementation(
      () =>
        new Promise<void>(resolve => {
          releaseStart = resolve;
        }),
    );

    const starting = mastra.startWorkers();
    await vi.waitFor(() => {
      expect(initialStart).toHaveBeenCalledTimes(1);
    });
    mastra.setStorage(new MockStore());

    await vi.waitFor(() => {
      expect(start).toHaveBeenCalledTimes(1);
    });
    expect(init).toHaveBeenCalledTimes(1);

    releaseStart();
    await starting;
    expect(start).toHaveBeenCalledTimes(1);
    expect(init).toHaveBeenCalledTimes(1);
  });

  it('does not start a late-registered harness wakeup worker after workers are stopped', async () => {
    const init = vi.spyOn(HarnessWakeupWorker.prototype, 'init').mockResolvedValue(undefined);
    const start = vi.spyOn(HarnessWakeupWorker.prototype, 'start').mockResolvedValue(undefined);
    const mastra = new Mastra({
      harness: new Harness({ modes: [] }),
      logger: false,
    });

    await mastra.startWorkers();
    await mastra.stopWorkers();
    mastra.setStorage(new MockStore());

    expect(mastra.workers.map(worker => worker.name)).toContain('harnessWakeups');
    await new Promise(resolve => setTimeout(resolve, 0));
    expect(init).not.toHaveBeenCalled();
    expect(start).not.toHaveBeenCalled();
  });

  it('does not keep late worker startup armed after all-worker startup fails', async () => {
    const init = vi.spyOn(HarnessWakeupWorker.prototype, 'init').mockResolvedValue(undefined);
    const start = vi.spyOn(HarnessWakeupWorker.prototype, 'start').mockResolvedValue(undefined);
    const mastra = new Mastra({
      harness: new Harness({ modes: [] }),
      logger: false,
    });
    const initialWorker = mastra.workers[0];
    if (!initialWorker) {
      throw new Error('expected an initial worker to fail startWorkers');
    }
    vi.spyOn(initialWorker, 'init').mockResolvedValue(undefined);
    vi.spyOn(initialWorker, 'start').mockRejectedValue(new Error('worker startup failed'));

    await expect(mastra.startWorkers()).rejects.toThrow('worker startup failed');
    mastra.setStorage(new MockStore());

    expect(mastra.workers.map(worker => worker.name)).toContain('harnessWakeups');
    await new Promise(resolve => setTimeout(resolve, 0));
    expect(init).not.toHaveBeenCalled();
    expect(start).not.toHaveBeenCalled();
  });

  it('does not start a late-registered harness wakeup worker while workers are stopping', async () => {
    let resolveInit!: () => void;
    const init = vi.spyOn(HarnessWakeupWorker.prototype, 'init').mockImplementation(
      () =>
        new Promise<void>(resolve => {
          resolveInit = resolve;
        }),
    );
    const start = vi.spyOn(HarnessWakeupWorker.prototype, 'start').mockResolvedValue(undefined);
    const mastra = new Mastra({
      harness: new Harness({ modes: [] }),
      logger: false,
    });

    await mastra.startWorkers();
    mastra.setStorage(new MockStore());
    await vi.waitFor(() => {
      expect(init).toHaveBeenCalledTimes(1);
    });

    const stopped = mastra.stopWorkers();
    resolveInit();
    await stopped;

    expect(start).not.toHaveBeenCalled();
  });

  it('disables all workers when MASTRA_WORKERS=false', async () => {
    process.env.MASTRA_WORKERS = 'false';

    const mastra = new Mastra({
      backgroundTasks: { enabled: true },
      logger: false,
    });

    expect(mastra.workers).toEqual([]);
    mastra.setStorage(new MockStore());
    expect(mastra.workers).toEqual([]);
  });

  it('warns when MASTRA_WORKERS filter matches no workers', async () => {
    process.env.MASTRA_WORKERS = 'nonexistent';

    const warn = vi.fn();
    const mastra = new Mastra({
      backgroundTasks: { enabled: true },
      logger: false,
    });
    mastra.setLogger({
      logger: { warn, info: vi.fn(), debug: vi.fn(), error: vi.fn(), trackException: vi.fn() } as any,
    });
    for (const w of mastra.workers) {
      vi.spyOn(w, 'start').mockResolvedValue(undefined);
      vi.spyOn(w, 'init').mockResolvedValue(undefined);
    }

    await mastra.startWorkers();
    // Should not throw, should not start any worker, and must have warned
    // about the empty filter so users know MASTRA_WORKERS was misspelled.
    for (const w of mastra.workers) {
      expect((w as any).start.mock.calls.length).toBe(0);
    }
    expect(warn).toHaveBeenCalledWith(expect.stringContaining('MASTRA_WORKERS=nonexistent'));
  });
});
