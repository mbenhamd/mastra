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

  it('threads dev mode, timeout, and cancellation into the owned subprocess', async () => {
    const service = new DepsService('pnpm');
    const controller = new AbortController();

    await service.installPackages(['typescript'], {
      dev: true,
      timeout: 5_000,
      cancelSignal: controller.signal,
    });

    expect(mocks.execa).toHaveBeenCalledWith('pnpm', ['add', '--loglevel=error', '-D', 'typescript'], {
      all: true,
      stdio: 'pipe',
      timeout: 5_000,
      cancelSignal: controller.signal,
      killSignal: 'SIGTERM',
      forceKillAfterDelay: 1_000,
    });
  });
});
