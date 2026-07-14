import fs from 'node:fs';
import fsPromises from 'node:fs/promises';
import path from 'node:path';
import { execa } from 'execa';
import type { ResultPromise } from 'execa';
import { onExit } from 'signal-exit';
import { getPackageManagerAddArgs } from '../utils/package-manager';
import type { PackageManager } from '../utils/package-manager';

type InstallPackagesOptions = {
  dev?: boolean;
  timeout?: number;
  cancelSignal?: AbortSignal;
};

const FORCE_KILL_DELAY_MS = 1_000;

function getTaskkillPath(): string {
  const configuredWindowsRoot = process.env.SystemRoot ?? process.env.WINDIR;
  const windowsRoot =
    configuredWindowsRoot && path.win32.isAbsolute(configuredWindowsRoot) ? configuredWindowsRoot : 'C:\\Windows';
  return path.win32.join(windowsRoot, 'System32', 'taskkill.exe');
}

async function terminateInstallTree(subprocess: ResultPromise, signal: NodeJS.Signals): Promise<void> {
  const pid = subprocess.pid;
  if (!pid) {
    subprocess.kill(signal);
    return;
  }

  if (process.platform === 'win32') {
    try {
      const result = await execa(getTaskkillPath(), ['/T', '/F', '/PID', String(pid)], {
        reject: false,
        stdio: 'ignore',
        timeout: 5_000,
      });
      if (result.exitCode === 0) return;
    } catch {
      // Fall through when taskkill is unavailable.
    }
  } else {
    try {
      process.kill(-pid, signal);
      return;
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === 'ESRCH') return;
      // Fall through when the process group could not be reached.
    }
  }

  subprocess.kill(signal);
}

function scheduleInstallTreeTermination(subprocess: ResultPromise, timeout?: number, cancelSignal?: AbortSignal) {
  let terminationPromise: Promise<void> | undefined;
  const terminate = () => {
    terminationPromise ??= (async () => {
      if (process.platform === 'win32') {
        await terminateInstallTree(subprocess, 'SIGKILL');
        return;
      }

      await terminateInstallTree(subprocess, 'SIGTERM');
      await new Promise<void>(resolve => setTimeout(resolve, FORCE_KILL_DELAY_MS));
      await terminateInstallTree(subprocess, 'SIGKILL');
    })().catch(() => {});
    return terminationPromise;
  };

  const terminateOnExit = () => {
    const pid = subprocess.pid;
    if (process.platform !== 'win32' && pid) {
      try {
        process.kill(-pid, 'SIGKILL');
        return;
      } catch {
        // Fall through when the detached process group has already exited.
      }
    }
    try {
      subprocess.kill('SIGKILL');
    } catch {
      // Best-effort cleanup during process exit cannot be awaited or retried.
    }
  };
  const terminateOnAbort = () => void terminate();

  const timeoutTimer = timeout === undefined || timeout <= 0 ? undefined : setTimeout(() => void terminate(), timeout);
  timeoutTimer?.unref();
  if (cancelSignal?.aborted) {
    void terminate();
  } else {
    cancelSignal?.addEventListener('abort', terminateOnAbort, { once: true });
  }
  // signal-exit runs the callback exactly once for normal exits and for fatal
  // signals such as SIGINT/SIGTERM (which never emit Node's `exit` event),
  // then re-raises the signal so the CLI dies with the conventional code.
  const removeExitCleanup = onExit(terminateOnExit);

  return {
    terminate,
    clear() {
      if (timeoutTimer) clearTimeout(timeoutTimer);
      cancelSignal?.removeEventListener('abort', terminateOnAbort);
      removeExitCleanup();
    },
  };
}

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
      installArgs.push(pm === 'bun' ? '-d' : '-D');
    }

    const subprocess = execa(pm, [...installArgs, ...packages], {
      all: true,
      stdio: 'pipe',
      timeout: options.timeout,
      cancelSignal: options.cancelSignal,
      killSignal: 'SIGTERM',
      forceKillAfterDelay: FORCE_KILL_DELAY_MS,
      detached: process.platform !== 'win32',
    });
    const treeTermination = scheduleInstallTreeTermination(subprocess, options.timeout, options.cancelSignal);

    try {
      return await subprocess;
    } catch (error) {
      await treeTermination.terminate();
      throw error;
    } finally {
      treeTermination.clear();
    }
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
