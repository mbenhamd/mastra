import fs from 'node:fs';
import fsPromises from 'node:fs/promises';
import path from 'node:path';
import { execa } from 'execa';
import { getPackageManagerAddArgs } from '../utils/package-manager';
import type { PackageManager } from '../utils/package-manager';

type InstallPackagesOptions = {
  dev?: boolean;
  timeout?: number;
  cancelSignal?: AbortSignal;
};

export class DepsService {
  readonly packageManager: PackageManager;

  constructor(packageManager?: PackageManager) {
    this.packageManager = packageManager ?? this.getPackageManager();
  }

  private findLockFile(dir: string): string | null {
    const lockFiles = ['pnpm-lock.yaml', 'package-lock.json', 'yarn.lock', 'bun.lock', 'bun.lockb'];
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
    const lockFile = this.findLockFile(process.cwd());
    switch (lockFile) {
      case 'pnpm-lock.yaml':
        return 'pnpm';
      case 'package-lock.json':
        return 'npm';
      case 'yarn.lock':
        return 'yarn';
      case 'bun.lock':
      case 'bun.lockb':
        return 'bun';
      default:
        return 'npm';
    }
  }

  public async installPackages(packages: string[], options: InstallPackagesOptions = {}) {
    const pm = this.packageManager;
    const installArgs = getPackageManagerAddArgs(pm);

    const optionLikePackage = packages.find(packageSpec => packageSpec.startsWith('-'));
    if (optionLikePackage) {
      throw new Error(`Package specs cannot start with "-": ${JSON.stringify(optionLikePackage)}`);
    }

    // Keep package specs as discrete arguments so neither a shell nor the
    // package manager can reinterpret them as command-line options.
    if (options.dev) {
      installArgs.push('-D');
    }

    return execa(pm, [...installArgs, ...packages], {
      all: true,
      stdio: 'pipe',
      timeout: options.timeout,
      cancelSignal: options.cancelSignal,
      killSignal: 'SIGTERM',
      forceKillAfterDelay: 1_000,
    });
  }

  public async checkDependencies(dependencies: string[]): Promise<string> {
    try {
      const packageJsonPath = path.join(process.cwd(), 'package.json');

      try {
        await fsPromises.access(packageJsonPath);
      } catch {
        return 'No package.json file found in the current directory';
      }

      const packageJson = JSON.parse(await fsPromises.readFile(packageJsonPath, 'utf-8'));
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
      const packageJsonPath = path.join(process.cwd(), 'package.json');
      const packageJson = await fsPromises.readFile(packageJsonPath, 'utf-8');
      const pkg = JSON.parse(packageJson);
      return pkg.name;
    } catch (err) {
      throw err;
    }
  }

  public async addScriptsToPackageJson(scripts: Record<string, string>) {
    const packageJson = JSON.parse(await fsPromises.readFile('package.json', 'utf-8'));
    packageJson.scripts = {
      ...packageJson.scripts,
      ...scripts,
    };
    await fsPromises.writeFile('package.json', JSON.stringify(packageJson, null, 2));
  }
}
