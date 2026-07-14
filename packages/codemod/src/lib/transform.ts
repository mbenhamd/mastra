import fs from 'node:fs';
import { createRequire } from 'node:module';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import debug from 'debug';
import { execa } from 'execa';
import { onExit } from 'signal-exit';

interface TransformOptions {
  dry?: boolean;
  print?: boolean;
  verbose?: boolean;
  jscodeshift?: string[];
}

const log = debug('codemod:transform');
const error = debug('codemod:transform:error');
const CODEMOD_TIMEOUT_MS = 5 * 60_000;
const FORCE_KILL_DELAY_MS = 1_000;

type KillableSubprocess = {
  pid?: number;
  kill(signal?: NodeJS.Signals): boolean;
};
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

export function getJscodeshiftBin(): string {
  // Resolve the direct dependency's declared executable instead of depending
  // on a private source layout or a platform-specific .bin shim.
  const require = createRequire(import.meta.url);
  const manifestPath = require.resolve('jscodeshift/package.json');
  const manifest = JSON.parse(fs.readFileSync(manifestPath, 'utf8')) as {
    bin?: string | Record<string, string>;
  };
  const declaredBin = typeof manifest.bin === 'string' ? manifest.bin : manifest.bin?.jscodeshift;
  if (!declaredBin) {
    throw new Error('The installed jscodeshift package does not declare a jscodeshift executable');
  }
  return path.resolve(path.dirname(manifestPath), declaredBin);
}

export function buildArgs(codemodPath: string, targetPath: string, options: TransformOptions): string[] {
  // Ignoring everything under `.*/` covers `.mastra/` along with any other
  // framework build related or otherwise intended-to-be-hidden directories.
  const args = [
    '-t',
    codemodPath,
    targetPath,
    '--parser',
    'tsx',
    '--ignore-pattern=**/node_modules/**',
    '--ignore-pattern=**/.*/**',
    '--ignore-pattern=**/dist/**',
    '--ignore-pattern=**/build/**',
    '--ignore-pattern=**/*.min.js',
    '--ignore-pattern=**/*.bundle.js',
  ];

  if (options.dry) {
    args.push('--dry');
  }

  if (options.print) {
    args.push('--print');
  }

  if (options.verbose) {
    args.push('--verbose=2');
  }

  if (options.jscodeshift) {
    args.push(...options.jscodeshift);
  }

  return args;
}

export type TransformErrors = {
  transform: string;
  filename: string;
  summary: string;
}[];

function parseErrors(transform: string, output: string): TransformErrors {
  const errors: TransformErrors = [];
  const errorRegex = /ERR (.+) Transformation error/g;
  const syntaxErrorRegex = /SyntaxError: .+/g;

  let match;
  while ((match = errorRegex.exec(output)) !== null) {
    const filename = match[1]!;
    const syntaxErrorMatch = syntaxErrorRegex.exec(output);
    if (syntaxErrorMatch) {
      const summary = syntaxErrorMatch[0];
      errors.push({ transform, filename, summary });
    }
  }

  return errors;
}

function parseNotImplementedErrors(transform: string, output: string): TransformErrors {
  const notImplementedErrors: TransformErrors = [];
  const notImplementedRegex = /Not Implemented (.+): (.+)/g;

  let match;
  while ((match = notImplementedRegex.exec(output)) !== null) {
    const filename = match[1]!;
    const summary = match[2]!;
    notImplementedErrors.push({ transform, filename, summary });
  }

  return notImplementedErrors;
}

function getTaskkillPath(): string {
  const configuredWindowsRoot = process.env.SystemRoot ?? process.env.WINDIR;
  const windowsRoot =
    configuredWindowsRoot && path.win32.isAbsolute(configuredWindowsRoot) ? configuredWindowsRoot : 'C:\\Windows';
  return path.win32.join(windowsRoot, 'System32', 'taskkill.exe');
}

async function killProcessTree(subprocess: KillableSubprocess, signal: NodeJS.Signals): Promise<void> {
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
      // Fall through to direct termination when taskkill is unavailable.
    }
  } else {
    try {
      process.kill(-pid, signal);
      return;
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === 'ESRCH') {
        return;
      }
      // Fall through when the process group already exited or was not created.
    }
  }

  subprocess.kill(signal);
}

function registerProcessTreeExitCleanup(subprocess: KillableSubprocess): () => void {
  if (process.platform === 'win32') {
    return () => {};
  }

  return onExit(() => {
    const pid = subprocess.pid;
    if (!pid) return;

    try {
      process.kill(-pid, 'SIGKILL');
    } catch {
      try {
        subprocess.kill('SIGKILL');
      } catch {
        // The process already exited before the parent cleanup callback ran.
      }
    }
  });
}

function scheduleProcessTreeTimeout(subprocess: KillableSubprocess) {
  let timedOut = false;
  let terminationPromise = Promise.resolve();
  let rejectExpiration!: (error: Error) => void;
  const expiration = new Promise<never>((_resolve, reject) => {
    rejectExpiration = reject;
  });
  const timeoutTimer = setTimeout(() => {
    timedOut = true;
    const timeoutError = new Error(`Codemod process timed out after ${CODEMOD_TIMEOUT_MS}ms`);
    timeoutError.name = 'CodemodTimeoutError';
    if (process.platform === 'win32') {
      terminationPromise = killProcessTree(subprocess, 'SIGKILL').catch(() => {});
      rejectExpiration(timeoutError);
      return;
    }

    void killProcessTree(subprocess, 'SIGTERM').catch(() => {});
    terminationPromise = new Promise(resolve => {
      setTimeout(() => {
        void killProcessTree(subprocess, 'SIGKILL').then(resolve, resolve);
      }, FORCE_KILL_DELAY_MS);
    });
    rejectExpiration(timeoutError);
  }, CODEMOD_TIMEOUT_MS);
  timeoutTimer.unref();

  return {
    clear: () => clearTimeout(timeoutTimer),
    didTimeOut: () => timedOut,
    expiration,
    waitForTermination: () => terminationPromise,
  };
}

async function runJscodeshift(args: string[]): Promise<string> {
  const subprocess = execa(process.execPath, [getJscodeshiftBin(), ...args], {
    encoding: 'utf8',
    maxBuffer: 100 * 1024 * 1024,
    detached: process.platform !== 'win32',
  });
  const timeout = scheduleProcessTreeTimeout(subprocess);
  const removeExitCleanup = registerProcessTreeExitCleanup(subprocess);

  try {
    try {
      const { stdout } = await Promise.race([subprocess, timeout.expiration]);
      if (timeout.didTimeOut()) {
        await timeout.waitForTermination();
        const timeoutError = new Error(`Codemod process timed out after ${CODEMOD_TIMEOUT_MS}ms`);
        timeoutError.name = 'CodemodTimeoutError';
        throw timeoutError;
      }
      return stdout;
    } catch (cause) {
      if (cause instanceof Error && cause.name === 'CodemodTimeoutError') {
        await timeout.waitForTermination();
        throw cause;
      }
      if (timeout.didTimeOut()) {
        await timeout.waitForTermination();
        const timeoutError = new Error(`Codemod process timed out after ${CODEMOD_TIMEOUT_MS}ms`, { cause });
        timeoutError.name = 'CodemodTimeoutError';
        throw timeoutError;
      }
      throw cause;
    }
  } finally {
    timeout.clear();
    removeExitCleanup();
  }
}

export async function transform(
  codemod: string,
  source: string,
  transformOptions: TransformOptions,
  options: { logStatus: boolean } = { logStatus: true },
): Promise<{ errors: TransformErrors; notImplementedErrors: TransformErrors }> {
  if (options.logStatus) {
    log(`Applying codemod '${codemod}': ${source}`);
  }
  const codemodPath = path.resolve(__dirname, `./codemods/${codemod}.js`);
  const targetPath = path.resolve(source);
  const args = buildArgs(codemodPath, targetPath, transformOptions);
  const stdout = await runJscodeshift(args);
  const errors = parseErrors(codemod, stdout);
  const notImplementedErrors = parseNotImplementedErrors(codemod, stdout);
  if (options.logStatus) {
    if (errors.length > 0) {
      errors.forEach(({ transform, filename, summary }) => {
        error(`Error applying codemod [codemod=${transform}, path=${filename}, summary=${summary}]`);
      });
    }

    if (notImplementedErrors.length > 0) {
      log(
        `Some files require manual changes. Please search your codebase for \`FIXME(mastra): \` comments and follow the instructions to complete the upgrade.`,
      );
      notImplementedErrors.forEach(({ transform, filename, summary }) => {
        log(`Not Implemented [codemod=${transform}, path=${filename}, summary=${summary}]`);
      });
    }
  }

  return { errors, notImplementedErrors };
}
