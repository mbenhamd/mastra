import { beforeEach, describe, expect, it, vi } from 'vitest';

const mocks = vi.hoisted(() => ({
  execa: vi.fn(),
}));

vi.mock('execa', () => ({
  execa: mocks.execa,
}));

import type { PackageManager } from '../utils/package-manager.js';
import { DepsService } from './service.deps.js';

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
    const subprocess = Object.assign(new Promise<never>(() => {}), {
      pid: 43_210,
      kill: vi.fn(() => true),
    });
    mocks.execa.mockReturnValueOnce(subprocess);
    const kill = vi.spyOn(process, 'kill').mockReturnValue(true);

    try {
      void new DepsService('npm').installPackages(['typescript'], { timeout: 500 });
      await vi.advanceTimersByTimeAsync(1_500);

      expect(kill).toHaveBeenCalledWith(-43_210, 'SIGTERM');
      expect(kill).toHaveBeenCalledWith(-43_210, 'SIGKILL');
    } finally {
      kill.mockRestore();
      vi.useRealTimers();
    }
  });

  it.runIf(process.platform !== 'win32')('terminates the install process group for an already-aborted signal', () => {
    const controller = new AbortController();
    controller.abort();
    const subprocess = Object.assign(new Promise<never>(() => {}), {
      pid: 43_211,
      kill: vi.fn(() => true),
    });
    mocks.execa.mockReturnValueOnce(subprocess);
    const kill = vi.spyOn(process, 'kill').mockReturnValue(true);

    try {
      void new DepsService('npm').installPackages(['typescript'], { cancelSignal: controller.signal });
      expect(kill).toHaveBeenCalledWith(-43_211, 'SIGTERM');
    } finally {
      kill.mockRestore();
    }
  });
});
