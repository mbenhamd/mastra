import { win32 } from 'node:path';
import { Writable } from 'node:stream';
import type { IMastraLogger } from '@mastra/core/logger';
import { onExit } from 'signal-exit';

const PACKAGE_MANAGER_TIMEOUT_MS = 15 * 60_000;
const FORCE_KILL_DELAY_MS = 1_000;

type KillableSubprocess = {
  pid?: number;
  kill(signal?: NodeJS.Signals): boolean;
};

type ProcessFailure = {
  name: string;
  exitCode?: number;
  signal?: string;
  timedOut: boolean;
  isCanceled: boolean;
};

class ChildProcessError extends Error implements ProcessFailure {
  readonly exitCode?: number;
  readonly signal?: string;
  readonly timedOut: boolean;
  readonly isCanceled: boolean;
  /**
   * Captured process output. `buffer: false` keeps execa from retaining it, so
   * callers that classify package-manager failures (for example pnpm's blocked
   * build scripts) read the captured text from the thrown error instead.
   */
  readonly stdout: string;
  readonly stderr: string;

  constructor(
    { exitCode, signal, timedOut, isCanceled }: Omit<ProcessFailure, 'name'>,
    capturedOutput: { stdout: string; stderr: string } = { stdout: '', stderr: '' },
  ) {
    super('Package manager process failed');
    this.name = 'ChildProcessError';
    this.exitCode = exitCode;
    this.signal = signal;
    this.timedOut = timedOut;
    this.isCanceled = isCanceled;
    this.stdout = capturedOutput.stdout;
    this.stderr = capturedOutput.stderr;
  }
}

function getTaskkillPath(): string {
  const configuredWindowsRoot = process.env.SystemRoot ?? process.env.WINDIR;
  const windowsRoot =
    configuredWindowsRoot && win32.isAbsolute(configuredWindowsRoot) ? configuredWindowsRoot : 'C:\\Windows';
  return win32.join(windowsRoot, 'System32', 'taskkill.exe');
}

async function killProcessTree(subprocess: KillableSubprocess, signal: NodeJS.Signals): Promise<void> {
  const pid = subprocess.pid;
  if (!pid) {
    return;
  }

  if (process.platform === 'win32') {
    try {
      const { execa } = await import('execa');
      const result = await execa(getTaskkillPath(), ['/T', '/F', '/PID', String(pid)], {
        reject: false,
        stdio: 'ignore',
        timeout: 5_000,
      });
      if (result.exitCode === 0) {
        return;
      }
    } catch {
      // Fall through to a direct kill when taskkill is unavailable.
    }
  } else {
    try {
      process.kill(-pid, signal);
      return;
    } catch {
      // Fall through when the process group has already exited or was not created.
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
    if (!pid) {
      return;
    }

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

function scheduleProcessTreeTimeout(
  subprocess: KillableSubprocess,
  timeout: number,
): { clear: () => void; didTimeOut: () => boolean; waitForTermination: () => Promise<void> } {
  let timedOut = false;
  let terminationPromise = Promise.resolve();
  if (timeout <= 0) {
    return { clear: () => {}, didTimeOut: () => timedOut, waitForTermination: () => terminationPromise };
  }

  const timeoutTimer = setTimeout(() => {
    timedOut = true;
    if (process.platform === 'win32') {
      terminationPromise = killProcessTree(subprocess, 'SIGKILL').catch(() => {});
      return;
    }

    void killProcessTree(subprocess, 'SIGTERM').catch(() => {});
    terminationPromise = new Promise(resolve => {
      setTimeout(() => {
        void killProcessTree(subprocess, 'SIGKILL').then(resolve, resolve);
      }, FORCE_KILL_DELAY_MS);
    });
  }, timeout);
  timeoutTimer.unref();

  return {
    clear: () => {
      clearTimeout(timeoutTimer);
    },
    didTimeOut: () => timedOut,
    waitForTermination: () => terminationPromise,
  };
}

function toProcessFailure(error: unknown, timedOut: boolean): ProcessFailure {
  const processError = error as {
    name?: unknown;
    exitCode?: unknown;
    signal?: unknown;
    timedOut?: unknown;
    isCanceled?: unknown;
  };

  return {
    name: typeof processError?.name === 'string' ? processError.name : 'Error',
    exitCode: typeof processError?.exitCode === 'number' ? processError.exitCode : undefined,
    signal: typeof processError?.signal === 'string' ? processError.signal : undefined,
    timedOut: timedOut || processError?.timedOut === true,
    isCanceled: processError?.isCanceled === true,
  };
}

export const createPinoStream = (logger: IMastraLogger) => {
  return new Writable({
    write(chunk, _encoding, callback) {
      // Convert Buffer/string to string and trim whitespace
      const line = chunk.toString().trim();

      if (line) {
        console.info(line);
        // Log each line through Pino
        logger.info(line);
      }

      callback();
    },
  });
};

export function createChildProcessLogger({
  logger,
  root,
  timeout = PACKAGE_MANAGER_TIMEOUT_MS,
  output = 'log',
}: {
  logger: IMastraLogger;
  root: string;
  timeout?: number;
  output?: 'ignore' | 'log';
}) {
  const pinoStream = createPinoStream(logger);
  return async ({ cmd, args, env }: { cmd: string; args: string[]; env: Record<string, string> }) => {
    let processTreeTimeout = {
      clear: () => {},
      didTimeOut: () => false,
      waitForTermination: () => Promise.resolve(),
    };
    let removeExitCleanup = () => {};
    let stdout = '';
    let stderr = '';
    try {
      const { execa } = await import('execa');
      const subprocess = execa(cmd, args, {
        ...(root ? { cwd: root } : {}),
        env,
        buffer: false,
        extendEnv: false,
        shell: false,
        stdin: 'ignore',
        stdout: output === 'log' ? 'pipe' : 'ignore',
        stderr: output === 'log' ? 'pipe' : 'ignore',
        detached: process.platform !== 'win32',
      });
      processTreeTimeout = scheduleProcessTreeTimeout(subprocess, timeout);
      removeExitCleanup = registerProcessTreeExitCleanup(subprocess);

      if (output === 'log') {
        // Capture the output as well as streaming it, so a *failure* can be
        // classified by the caller off ChildProcessError.stdout/stderr (see
        // DeployerDeps' pnpm ERR_PNPM_IGNORED_BUILDS handling). The listeners
        // and the pipes are attached in the same tick, so no chunk is observed
        // by only one of them.
        subprocess.stdout?.on('data', chunk => {
          stdout += chunk.toString();
        });
        subprocess.stderr?.on('data', chunk => {
          stderr += chunk.toString();
        });

        // Pipe stdout and stderr through the logging stream.
        // { end: false } prevents the first stream to close from ending pinoStream
        // while the other may still be writing.
        subprocess.stdout?.pipe(pinoStream, { end: false });
        subprocess.stderr?.pipe(pinoStream, { end: false });
      }

      await subprocess;
      if (processTreeTimeout.didTimeOut()) {
        await processTreeTimeout.waitForTermination();
        const processFailure = toProcessFailure({ name: 'TimeoutError' }, true);
        logger.error('Process failed', { error: processFailure });
        throw new ChildProcessError(processFailure, { stdout, stderr });
      }
      // The captured text is deliberately NOT returned on success: no caller
      // reads it, and every call site awaits this for its side effects only.
      return { success: true };
    } catch (error) {
      if (error instanceof ChildProcessError) {
        throw error;
      }
      const timedOut = processTreeTimeout.didTimeOut();
      if (timedOut) {
        await processTreeTimeout.waitForTermination();
      }
      const processFailure = toProcessFailure(error, timedOut);
      logger.error('Process failed', {
        error: processFailure,
      });
      throw new ChildProcessError(processFailure, { stdout, stderr });
    } finally {
      processTreeTimeout.clear();
      removeExitCleanup();
      pinoStream.end();
    }
  };
}
