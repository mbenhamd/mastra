import { PassThrough } from 'node:stream';
import { beforeEach, describe, expect, it, vi } from 'vitest';

const mocks = vi.hoisted(() => ({
  execa: vi.fn(),
}));

vi.mock('execa', () => ({
  execa: mocks.execa,
}));

import { createChildProcessLogger } from './log.js';

function successfulSubprocess() {
  return Object.assign(Promise.resolve({ exitCode: 0 }), {
    stdout: new PassThrough(),
    stderr: new PassThrough(),
  });
}

describe('createChildProcessLogger', () => {
  const logger = {
    info: vi.fn(),
    error: vi.fn(),
  };

  beforeEach(() => {
    vi.clearAllMocks();
    mocks.execa.mockReturnValue(successfulSubprocess());
  });

  it('executes one program with discrete arguments and no shell', async () => {
    const run = createChildProcessLogger({ logger: logger as never, root: '/project path' });
    const argument = 'package; $(not-a-command) & still-data';

    await expect(
      run({
        cmd: 'pnpm',
        args: ['add', argument],
        env: { PATH: '/bin' },
      }),
    ).resolves.toEqual({ success: true });

    expect(mocks.execa).toHaveBeenCalledWith('pnpm', ['add', argument], {
      cwd: '/project path',
      env: { PATH: '/bin' },
      buffer: false,
      extendEnv: false,
      shell: false,
      stdin: 'ignore',
      stdout: 'pipe',
      stderr: 'pipe',
      detached: process.platform !== 'win32',
    });
  });

  it('logs allowlisted process status and throws a credential-safe failure', async () => {
    const failure = Object.assign(new Error('process failed with https://user:SECRET@example.invalid/pkg'), {
      command: 'npm install https://user:SECRET@example.invalid/pkg',
      escapedCommand: 'npm install https://user:SECRET@example.invalid/pkg',
      exitCode: 7,
      signal: 'SIGTERM',
      timedOut: true,
      isCanceled: false,
    });
    const subprocess = Object.assign(Promise.reject(failure), {
      stdout: new PassThrough(),
      stderr: new PassThrough(),
    });
    mocks.execa.mockReturnValue(subprocess);
    const run = createChildProcessLogger({ logger: logger as never, root: '/project' });

    const result = run({ cmd: 'npm', args: ['install'], env: { PATH: '/bin' } });

    await expect(result).rejects.toMatchObject({
      name: 'ChildProcessError',
      message: 'Package manager process failed',
      exitCode: 7,
      signal: 'SIGTERM',
      timedOut: true,
      isCanceled: false,
    });
    await expect(result).rejects.not.toBe(failure);

    expect(logger.error).toHaveBeenCalledWith('Process failed', {
      error: {
        name: 'Error',
        exitCode: 7,
        signal: 'SIGTERM',
        timedOut: true,
        isCanceled: false,
      },
    });
    expect(JSON.stringify(logger.error.mock.calls)).not.toContain('SECRET');
    await expect(
      result.catch(error => JSON.stringify(error, Object.getOwnPropertyNames(error))),
    ).resolves.not.toContain('SECRET');
  });
});
