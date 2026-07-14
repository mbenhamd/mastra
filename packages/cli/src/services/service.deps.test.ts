import { beforeEach, describe, expect, it, vi } from 'vitest';

const mocks = vi.hoisted(() => ({
  execa: vi.fn(),
}));

vi.mock('execa', () => ({
  execa: mocks.execa,
}));

import type { PackageManager } from '../utils/package-manager.js';
import { DepsService } from './service.deps.js';

function deferredSubprocess(pid: number) {
  let resolve!: (value: { all: string }) => void;
  let reject!: (error: Error) => void;
  const promise = new Promise<{ all: string }>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });

  return {
    subprocess: Object.assign(promise, { pid, kill: vi.fn(() => true) }),
    resolve,
    reject,
  };
}

describe('DepsService.installPackages', () => {
  beforeEach(() => {
    mocks.execa.mockReset();
    mocks.execa.mockResolvedValue({ all: 'installed' });
  });

  it.each([
    {
      packageManager: 'npm',
      installArgs: [
        'install',
        '--audit=false',
        '--fund=false',
        '--loglevel=error',
        '--progress=false',
        '--update-notifier=false',
      ],
    },
    { packageManager: 'yarn', installArgs: ['add'] },
    { packageManager: 'pnpm', installArgs: ['add', '--loglevel=error'] },
    { packageManager: 'bun', installArgs: ['add'] },
  ] satisfies Array<{ packageManager: PackageManager; installArgs: string[] }>)(
    'passes package specs as discrete $packageManager arguments without a shell',
    async ({ packageManager, installArgs }) => {
      const service = new DepsService(packageManager);
      const packageSpec = '@scope/pkg@1.0.0; echo not-a-command with spaces';

      await expect(service.installPackages([packageSpec])).resolves.toEqual({ all: 'installed' });

      expect(mocks.execa).toHaveBeenCalledWith(packageManager, [...installArgs, packageSpec], {
        all: true,
        stdio: 'pipe',
        timeout: undefined,
        cancelSignal: undefined,
        killSignal: 'SIGTERM',
        forceKillAfterDelay: 1_000,
        detached: process.platform !== 'win32',
      });
      expect(mocks.execa.mock.calls[0]?.[2]).not.toHaveProperty('shell');
    },
  );

  it('rejects package-manager options presented as package specs', async () => {
    const service = new DepsService('npm');

    await expect(service.installPackages(['--script-shell=/tmp/attacker'])).rejects.toThrow(
      'Package specs cannot start with "-"',
    );

    expect(mocks.execa).not.toHaveBeenCalled();
  });

  it.each([
    { packageManager: 'pnpm', installArgs: ['add', '--loglevel=error', '-D', 'typescript'] },
    { packageManager: 'bun', installArgs: ['add', '-d', 'typescript'] },
  ] satisfies Array<{ packageManager: PackageManager; installArgs: string[] }>)(
    'threads the $packageManager dev flag, timeout, and cancellation into the owned subprocess',
    async ({ packageManager, installArgs }) => {
      const service = new DepsService(packageManager);
      const controller = new AbortController();

      await service.installPackages(['typescript'], {
        dev: true,
        timeout: 5_000,
        cancelSignal: controller.signal,
      });

      expect(mocks.execa).toHaveBeenCalledWith(packageManager, installArgs, {
        all: true,
        stdio: 'pipe',
        timeout: 5_000,
        cancelSignal: controller.signal,
        killSignal: 'SIGTERM',
        forceKillAfterDelay: 1_000,
        detached: process.platform !== 'win32',
      });
    },
  );

  it.runIf(process.platform !== 'win32')('terminates the install process group when timeout expires', async () => {
    vi.useFakeTimers();
    const deferred = deferredSubprocess(43_210);
    mocks.execa.mockReturnValueOnce(deferred.subprocess);
    const kill = vi.spyOn(process, 'kill').mockReturnValue(true);

    try {
      const installation = new DepsService('npm').installPackages(['typescript'], { timeout: 500 });
      await vi.advanceTimersByTimeAsync(1_500);

      expect(kill).toHaveBeenCalledWith(-43_210, 'SIGTERM');
      expect(kill).toHaveBeenCalledWith(-43_210, 'SIGKILL');
      deferred.reject(new Error('timed out'));
      await expect(installation).rejects.toThrow('timed out');
    } finally {
      kill.mockRestore();
      vi.useRealTimers();
    }
  });

  it.runIf(process.platform !== 'win32')('treats timeout zero as no install timeout', async () => {
    vi.useFakeTimers();
    const deferred = deferredSubprocess(43_211);
    mocks.execa.mockReturnValueOnce(deferred.subprocess);
    const kill = vi.spyOn(process, 'kill').mockReturnValue(true);

    try {
      const installation = new DepsService('npm').installPackages(['typescript'], { timeout: 0 });
      await vi.advanceTimersByTimeAsync(5_000);

      expect(kill).not.toHaveBeenCalled();
      deferred.resolve({ all: 'installed' });
      await expect(installation).resolves.toEqual({ all: 'installed' });
    } finally {
      kill.mockRestore();
      vi.useRealTimers();
    }
  });

  it.runIf(process.platform !== 'win32')('awaits forced install-tree cleanup before rejecting', async () => {
    vi.useFakeTimers();
    const deferred = deferredSubprocess(43_212);
    mocks.execa.mockReturnValueOnce(deferred.subprocess);
    const kill = vi.spyOn(process, 'kill').mockReturnValue(true);

    try {
      let settled = false;
      const installation = new DepsService('npm').installPackages(['typescript'], { timeout: 500 });
      void installation.then(
        () => {
          settled = true;
        },
        () => {
          settled = true;
        },
      );

      await vi.advanceTimersByTimeAsync(500);
      deferred.reject(new Error('timed out'));
      await Promise.resolve();
      expect(kill).toHaveBeenCalledWith(-43_212, 'SIGTERM');
      expect(settled).toBe(false);

      await vi.advanceTimersByTimeAsync(999);
      expect(settled).toBe(false);
      await vi.advanceTimersByTimeAsync(1);

      await expect(installation).rejects.toThrow('timed out');
      expect(kill).toHaveBeenCalledWith(-43_212, 'SIGKILL');
      expect(settled).toBe(true);
    } finally {
      kill.mockRestore();
      vi.useRealTimers();
    }
  });

  it.runIf(process.platform !== 'win32')(
    'terminates the install process group for an already-aborted signal',
    async () => {
      const controller = new AbortController();
      controller.abort();
      vi.useFakeTimers();
      const deferred = deferredSubprocess(43_213);
      mocks.execa.mockReturnValueOnce(deferred.subprocess);
      const kill = vi.spyOn(process, 'kill').mockReturnValue(true);

      try {
        const installation = new DepsService('npm').installPackages(['typescript'], {
          cancelSignal: controller.signal,
        });
        expect(kill).toHaveBeenCalledWith(-43_213, 'SIGTERM');
        await vi.advanceTimersByTimeAsync(1_000);
        deferred.reject(new Error('aborted'));
        await expect(installation).rejects.toThrow('aborted');
      } finally {
        kill.mockRestore();
        vi.useRealTimers();
      }
    },
  );

  it.runIf(process.platform !== 'win32')('kills a detached install tree when the CLI exits', async () => {
    const deferred = deferredSubprocess(43_214);
    mocks.execa.mockReturnValueOnce(deferred.subprocess);
    const kill = vi.spyOn(process, 'kill').mockReturnValue(true);
    const once = vi.spyOn(process, 'once');

    try {
      const installation = new DepsService('npm').installPackages(['typescript']);
      const exitListener = once.mock.calls.find(([event]) => event === 'exit')?.[1] as
        | ((code: number) => void)
        | undefined;
      expect(exitListener).toBeDefined();

      exitListener?.(1);
      expect(kill).toHaveBeenCalledWith(-43_214, 'SIGKILL');

      deferred.resolve({ all: 'installed' });
      await expect(installation).resolves.toEqual({ all: 'installed' });
    } finally {
      once.mockRestore();
      kill.mockRestore();
    }
  });
});
