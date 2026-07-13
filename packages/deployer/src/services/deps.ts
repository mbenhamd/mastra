import fs from 'node:fs';
import fsPromises from 'node:fs/promises';
import path, { dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { MastraBase } from '@mastra/core/base';
import { readJSON, writeJSON, ensureFile } from 'fs-extra/esm';
import type { PackageJson } from 'type-fest';

import { createChildProcessLogger } from '../deploy/log.js';

type PackageManager = 'npm' | 'yarn' | 'pnpm' | 'bun';

const PROCESS_BOOTSTRAP_ENV_KEYS = [
  'PATH',
  'SystemRoot',
  'ComSpec',
  'PATHEXT',
  'WINDIR',
  'HOME',
  'USERPROFILE',
  'HOMEDRIVE',
  'HOMEPATH',
  'APPDATA',
  'LOCALAPPDATA',
  'HTTP_PROXY',
  'HTTPS_PROXY',
  'NO_PROXY',
  'ALL_PROXY',
  'http_proxy',
  'https_proxy',
  'no_proxy',
  'all_proxy',
  'NPM_TOKEN',
  'NODE_AUTH_TOKEN',
  'YARN_NPM_AUTH_TOKEN',
  'YARN_NPM_AUTH_IDENT',
] as const;

const PACKAGE_MANAGER_NETWORK_CONFIG_KEYS = new Set([
  'registry',
  'proxy',
  'https_proxy',
  'https-proxy',
  'noproxy',
  'no_proxy',
  'no-proxy',
  'strict_ssl',
  'strict-ssl',
  'cafile',
  'cert',
  'key',
]);

function isPackageManagerNetworkOrAuthConfig(key: string): boolean {
  const normalizedKey = key.toLowerCase();
  const prefix = normalizedKey.startsWith('npm_config_')
    ? 'npm_config_'
    : normalizedKey.startsWith('pnpm_config_')
      ? 'pnpm_config_'
      : undefined;
  if (!prefix) {
    return false;
  }

  const configKey = normalizedKey.slice(prefix.length);
  return (
    PACKAGE_MANAGER_NETWORK_CONFIG_KEYS.has(configKey) ||
    /^@[^:]+:registry$/.test(configKey) ||
    /^\/\/.+\/:(_auth|_authtoken|username|_password|email|always-auth)$/.test(configKey)
  );
}

function getProcessBootstrapEnv(): Record<string, string> {
  const env: Record<string, string> = {};
  for (const key of PROCESS_BOOTSTRAP_ENV_KEYS) {
    const value = process.env[key];
    if (value) {
      env[key] = value;
    }
  }
  for (const [key, value] of Object.entries(process.env)) {
    // Package-manager config also includes execution-changing settings such as
    // script-shell. Forward only the network and authentication subset needed
    // for private registries; the child still inherits no ambient shell config.
    if (value && isPackageManagerNetworkOrAuthConfig(key)) {
      env[key] = value;
    }
  }
  return env;
}

interface ArchitectureOptions {
  os?: string[];
  cpu?: string[];
  libc?: string[];
}

export class Deps extends MastraBase {
  private packageManager: PackageManager;
  private rootDir: string;

  constructor(rootDir = process.cwd()) {
    super({ component: 'DEPLOYER', name: 'DEPS' });

    this.rootDir = rootDir;
    this.packageManager = this.getPackageManager();
  }

  private findLockFile(dir: string): string | null {
    const lockFiles = ['pnpm-lock.yaml', 'package-lock.json', 'yarn.lock', 'bun.lock'];
    for (const file of lockFiles) {
      if (fs.existsSync(path.join(dir, file))) {
        return file;
      }
    }
    const parentDir = path.resolve(dir, '..');
    if (parentDir !== dir) {
      return this.findLockFile(parentDir);
    }
    return null;
  }

  private getPackageManager(): PackageManager {
    const lockFile = this.findLockFile(this.rootDir);
    switch (lockFile) {
      case 'pnpm-lock.yaml':
        return 'pnpm';
      case 'package-lock.json':
        return 'npm';
      case 'yarn.lock':
        return 'yarn';
      case 'bun.lock':
        return 'bun';
      default:
        return 'npm';
    }
  }

  public getWorkspaceDependencyPath({ pkgName, version }: { pkgName: string; version: string }) {
    return `file:./workspace-module/${pkgName}-${version}.tgz`;
  }

  public async pack({ dir, destination, sanitizedName }: { dir: string; destination: string; sanitizedName: string }) {
    // Package-manager option parsers can reinterpret a relative destination
    // beginning with `-`. Resolve it against the child process cwd so it stays
    // a value while preserving the previous cwd-relative behavior.
    const resolvedDestination = path.resolve(dir, destination);
    let args = ['pack', '--pack-destination', resolvedDestination];
    if (this.packageManager === 'yarn') {
      if (
        !sanitizedName ||
        sanitizedName === '.' ||
        sanitizedName === '..' ||
        path.posix.basename(sanitizedName) !== sanitizedName ||
        path.win32.basename(sanitizedName) !== sanitizedName
      ) {
        throw new Error(`Sanitized package name must be a filename segment: ${JSON.stringify(sanitizedName)}`);
      }

      // %s includes an '@' at the start of packages names with an '@'
      // so we need to use our sanitizedName instead.
      args = ['pack', '--out', path.join(resolvedDestination, `${sanitizedName}-%v.tgz`)];
    }
    if (this.packageManager === 'bun') {
      // bun uses `pm pack` instead of `pack`
      // bun uses --destination instead of --pack-destination
      args = ['pm', 'pack', '--destination', resolvedDestination];
    }

    const cpLogger = createChildProcessLogger({
      logger: this.logger,
      root: dir,
    });

    return cpLogger({
      cmd: this.packageManager,
      args,
      env: getProcessBootstrapEnv(),
    });
  }

  private async writePnpmConfig(dir: string, options: ArchitectureOptions) {
    const workspaceYamlPath = path.join(dir, 'pnpm-workspace.yaml');

    const lines: string[] = [
      'packages:',
      "  - '.'",
      'allowBuilds:',
      '  bcrypt: true',
      '  esbuild: true',
      '  sharp: true',
      '  protobufjs: true',
      '  workerd: true',
      '  bufferutil: true',
      '  utf-8-validate: true',
      'minimumReleaseAge: 0',
    ];
    if (options.os?.length || options.cpu?.length || options.libc?.length) {
      lines.push('');
      lines.push('supportedArchitectures:');
      if (options.os?.length) {
        lines.push(`  os: ${JSON.stringify(options.os)}`);
      }
      if (options.cpu?.length) {
        lines.push(`  cpu: ${JSON.stringify(options.cpu)}`);
      }
      if (options.libc?.length) {
        lines.push(`  libc: ${JSON.stringify(options.libc)}`);
      }
    }
    lines.push('');

    await fsPromises.writeFile(workspaceYamlPath, lines.join('\n'), 'utf-8');
  }

  private async writeYarnConfig(dir: string, options: ArchitectureOptions) {
    const yarnrcPath = path.join(dir, '.yarnrc.yml');
    const config = {
      supportedArchitectures: {
        cpu: options.cpu || [],
        os: options.os || [],
        libc: options.libc || [],
      },
    };

    await fsPromises.writeFile(
      yarnrcPath,
      `supportedArchitectures:\n${Object.entries(config.supportedArchitectures)
        .map(([key, value]) => `  ${key}: ${JSON.stringify(value)}`)
        .join('\n')}`,
    );
  }

  private getNpmArgs(options: ArchitectureOptions): string[] {
    const args: string[] = [];
    if (options.cpu) args.push(`--cpu=${options.cpu.join(',')}`);
    if (options.os) args.push(`--os=${options.os.join(',')}`);
    if (options.libc) args.push(`--libc=${options.libc.join(',')}`);
    return args;
  }

  /**
   * Depending on whether we want to install or add a package, this function returns the appropriate commands.
   * All package managers support both commands (e.g. npm install has an alias on "add")
   */
  private getPackageManagerArgs(pm: PackageManager, type: 'install' | 'add'): string[] {
    const cmd = type === 'install' ? 'install' : 'add';

    switch (pm) {
      case 'npm':
        return [
          cmd,
          '--audit=false',
          '--fund=false',
          '--loglevel=error',
          '--progress=false',
          '--update-notifier=false',
        ];
      case 'yarn':
        return [cmd];
      case 'pnpm':
        return [cmd, '--loglevel=error'];
      case 'bun':
        return [cmd];
      default:
        return [cmd];
    }
  }

  public async install({
    dir = this.rootDir,
    architecture,
  }: { dir?: string; architecture?: ArchitectureOptions } = {}) {
    const pm = this.packageManager;
    const args = this.getPackageManagerArgs(pm, 'install');

    switch (pm) {
      case 'pnpm':
        if (architecture) {
          await this.writePnpmConfig(dir, architecture);
        }
        break;
      case 'yarn':
        // similar to --ignore-workspace but for yarn
        await ensureFile(path.join(dir, 'yarn.lock'));
        if (architecture) {
          await this.writeYarnConfig(dir, architecture);
        }
        break;
      case 'npm':
        if (architecture) {
          args.push(...this.getNpmArgs(architecture));
        }
        break;
      default:
      // Do nothing
    }

    const cpLogger = createChildProcessLogger({
      logger: this.logger,
      root: dir,
    });

    return cpLogger({
      cmd: pm,
      args,
      env: process.env as Record<string, string>,
    });
  }

  public async installPackages(packages: string[]) {
    const pm = this.packageManager;
    const installArgs = this.getPackageManagerArgs(pm, 'add');

    const optionLikePackage = packages.find(packageSpec => packageSpec.startsWith('-'));
    if (optionLikePackage) {
      throw new Error(`Package specs cannot start with "-": ${JSON.stringify(optionLikePackage)}`);
    }

    const cpLogger = createChildProcessLogger({
      logger: this.logger,
      root: '',
    });

    return cpLogger({
      cmd: pm,
      args: [...installArgs, ...packages],
      env: getProcessBootstrapEnv(),
    });
  }

  public async checkDependencies(dependencies: string[]): Promise<string> {
    try {
      const packageJsonPath = path.join(this.rootDir, 'package.json');

      try {
        await fsPromises.access(packageJsonPath);
      } catch {
        return 'No package.json file found in the current directory';
      }

      const packageJson = await readJSON(packageJsonPath);
      for (const dependency of dependencies) {
        if (!packageJson.dependencies || !packageJson.dependencies[dependency]) {
          return `Please install ${dependency} before running this command (${this.packageManager} install ${dependency})`;
        }
      }

      return 'ok';
    } catch (err) {
      console.error(err);
      return 'Could not check dependencies';
    }
  }

  public async getProjectName() {
    try {
      const packageJsonPath = path.join(this.rootDir, 'package.json');
      const pkg = await readJSON(packageJsonPath);
      return pkg.name;
    } catch (err) {
      throw err;
    }
  }

  public async getPackageVersion() {
    const __filename = fileURLToPath(import.meta.url);
    const __dirname = dirname(__filename);
    const pkgJsonPath = path.join(__dirname, '..', '..', 'package.json');

    const content = (await readJSON(pkgJsonPath)) as PackageJson;
    return content.version;
  }

  public async addScriptsToPackageJson(scripts: Record<string, string>) {
    const packageJson = await readJSON('package.json');
    packageJson.scripts = {
      ...packageJson.scripts,
      ...scripts,
    };
    await writeJSON('package.json', packageJson, { spaces: 2 });
  }
}

export class DepsService extends Deps {}
