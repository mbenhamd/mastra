import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

const mocks = vi.hoisted(() => ({
  createChildProcessLogger: vi.fn(),
  runProcess: vi.fn(),
}));

vi.mock('../deploy/log.js', () => ({
  createChildProcessLogger: mocks.createChildProcessLogger,
}));

import { Deps } from './deps.js';

type PackageManager = 'npm' | 'yarn' | 'pnpm' | 'bun';

const fixtures: string[] = [];

async function createDeps(packageManager: PackageManager) {
  const root = await mkdtemp(join(tmpdir(), 'mastra-deployer-deps-'));
  fixtures.push(root);
  const deps = new Deps(root);
  Object.defineProperty(deps, 'packageManager', { value: packageManager });
  return { deps, root };
}

afterEach(async () => {
  vi.unstubAllEnvs();
  await Promise.all(fixtures.splice(0).map(root => rm(root, { recursive: true, force: true })));
});

beforeEach(() => {
  vi.clearAllMocks();
  mocks.runProcess.mockResolvedValue({ success: true });
  mocks.createChildProcessLogger.mockReturnValue(mocks.runProcess);
});

describe('Deps process arguments', () => {
  it.each(['npm', 'pnpm', 'yarn', 'bun'] satisfies PackageManager[])(
    'passes %s pack values as absolute, discrete arguments',
    async pm => {
      const { deps, root } = await createDeps(pm);
      const destination = '-output path; $(not-a-command)';
      const resolvedDestination = resolve(root, destination);
      const sanitizedName = pm === 'yarn' ? 'scope-package' : '';
      const expectedArgs =
        pm === 'yarn'
          ? ['pack', '--out', join(resolvedDestination, 'scope-package-%v.tgz')]
          : pm === 'bun'
            ? ['pm', 'pack', '--destination', resolvedDestination]
            : ['pack', '--pack-destination', resolvedDestination];

      await deps.pack({ dir: root, destination, sanitizedName });

      expect(mocks.runProcess).toHaveBeenCalledWith({
        cmd: pm,
        args: expectedArgs,
        env: expect.objectContaining({ PATH: process.env.PATH! }),
      });
    },
  );

  it('preserves Windows process bootstrap variables in the narrow pack environment', async () => {
    vi.stubEnv('SystemRoot', String.raw`C:\Windows`);
    vi.stubEnv('ComSpec', String.raw`C:\Windows\System32\cmd.exe`);
    vi.stubEnv('PATHEXT', '.COM;.EXE;.BAT;.CMD');
    vi.stubEnv('WINDIR', String.raw`C:\Windows`);
    const { deps, root } = await createDeps('npm');

    await deps.pack({ dir: root, destination: 'output', sanitizedName: '' });

    expect(mocks.runProcess).toHaveBeenCalledWith({
      cmd: 'npm',
      args: ['pack', '--pack-destination', resolve(root, 'output')],
      env: expect.objectContaining({
        PATH: process.env.PATH,
        SystemRoot: String.raw`C:\Windows`,
        ComSpec: String.raw`C:\Windows\System32\cmd.exe`,
        PATHEXT: '.COM;.EXE;.BAT;.CMD',
        WINDIR: String.raw`C:\Windows`,
      }),
    });
  });

  it('preserves home, proxy, and private-registry package-manager configuration', async () => {
    vi.stubEnv('HOME', '/home/mastra');
    vi.stubEnv('HTTPS_PROXY', 'https://proxy.example.com');
    vi.stubEnv('NPM_TOKEN', 'test-npm-token');
    vi.stubEnv('NODE_AUTH_TOKEN', 'test-node-auth-token');
    vi.stubEnv('npm_config_registry', 'https://registry.example.com');
    vi.stubEnv('npm_config_//registry.example.com/:_authToken', 'test-private-registry-token');
    vi.stubEnv('PNPM_CONFIG_@private:registry', 'https://registry.example.com');
    vi.stubEnv('npm_config_script_shell', '/tmp/attacker');
    const { deps } = await createDeps('pnpm');

    await deps.installPackages(['@private/package@1.0.0']);

    expect(mocks.runProcess).toHaveBeenCalledWith({
      cmd: 'pnpm',
      args: ['add', '--loglevel=error', '@private/package@1.0.0'],
      env: expect.objectContaining({
        HOME: '/home/mastra',
        HTTPS_PROXY: 'https://proxy.example.com',
        NPM_TOKEN: 'test-npm-token',
        NODE_AUTH_TOKEN: 'test-node-auth-token',
        npm_config_registry: 'https://registry.example.com',
        'npm_config_//registry.example.com/:_authToken': 'test-private-registry-token',
        'PNPM_CONFIG_@private:registry': 'https://registry.example.com',
      }),
    });
    expect(mocks.runProcess.mock.calls[0]?.[0].env).not.toHaveProperty('npm_config_script_shell');
  });

  it.each(['', '.', '..', '../outside', String.raw`..\outside`])(
    'rejects an unsafe sanitized package name: %j',
    async sanitizedName => {
      const { deps, root } = await createDeps('yarn');

      await expect(deps.pack({ dir: root, destination: 'output', sanitizedName })).rejects.toThrow(
        'must be a filename segment',
      );

      expect(mocks.createChildProcessLogger).not.toHaveBeenCalled();
    },
  );

  it.each([
    [
      'npm',
      ['add', '--audit=false', '--fund=false', '--loglevel=error', '--progress=false', '--update-notifier=false'],
    ],
    ['pnpm', ['add', '--loglevel=error']],
    ['yarn', ['add']],
    ['bun', ['add']],
  ] satisfies Array<[PackageManager, string[]]>)(
    'passes %s package specs as data after add flags',
    async (pm, args) => {
      const { deps } = await createDeps(pm);
      const packageSpec = '@scope/package@1.0.0; echo not-a-command';

      await deps.installPackages([packageSpec]);

      expect(mocks.runProcess).toHaveBeenCalledWith({
        cmd: pm,
        args: [...args, packageSpec],
        env: expect.objectContaining({ PATH: process.env.PATH! }),
      });
    },
  );

  it.each([
    [
      'npm',
      ['install', '--audit=false', '--fund=false', '--loglevel=error', '--progress=false', '--update-notifier=false'],
    ],
    ['pnpm', ['install', '--loglevel=error']],
    ['yarn', ['install']],
    ['bun', ['install']],
  ] satisfies Array<[PackageManager, string[]]>)('passes %s install flags as discrete arguments', async (pm, args) => {
    const { deps, root } = await createDeps(pm);

    await deps.install({ dir: root });

    expect(mocks.runProcess).toHaveBeenCalledWith({
      cmd: pm,
      args,
      env: process.env,
    });
  });

  it('rejects package-manager options presented as package specs', async () => {
    const { deps } = await createDeps('npm');

    await expect(deps.installPackages(['--script-shell=/tmp/attacker'])).rejects.toThrow(
      'Package specs cannot start with "-"',
    );

    expect(mocks.createChildProcessLogger).not.toHaveBeenCalled();
  });
});
