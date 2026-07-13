import { afterEach, describe, expect, it, vi } from 'vitest';
import { createChildProcessLogger } from './log.js';

afterEach(() => {
  vi.restoreAllMocks();
});

describe('createChildProcessLogger integration', () => {
  it('passes shell-control-looking text unchanged to a real child process', async () => {
    vi.spyOn(console, 'info').mockImplementation(() => {});
    const logger = {
      info: vi.fn(),
      error: vi.fn(),
    };
    const stdoutValue = 'stdout; $(not-a-command) & | ^ %PATH%';
    const stderrValue = 'stderr; $(still-not-a-command) & | ^ %PATH%';
    const run = createChildProcessLogger({ logger: logger as never, root: process.cwd() });

    await expect(
      run({
        cmd: process.execPath,
        args: [
          '-e',
          'process.stdout.write(process.argv[1] ?? ""); process.stderr.write(process.argv[2] ?? "")',
          stdoutValue,
          stderrValue,
        ],
        env: { PATH: process.env.PATH! },
      }),
    ).resolves.toEqual({ success: true });

    expect(logger.info).toHaveBeenCalledWith(stdoutValue);
    expect(logger.info).toHaveBeenCalledWith(stderrValue);
    expect(logger.error).not.toHaveBeenCalled();
  });

  it('does not log credential-bearing argv when a real child fails', async () => {
    vi.spyOn(console, 'info').mockImplementation(() => {});
    const logger = {
      info: vi.fn(),
      error: vi.fn(),
    };
    const credential = 'https://user:DUMMY_SECRET_TOKEN_123@example.invalid/pkg';
    const run = createChildProcessLogger({ logger: logger as never, root: process.cwd() });

    await expect(
      run({
        cmd: process.execPath,
        args: ['-e', 'process.exit(7)', credential],
        env: { PATH: process.env.PATH! },
      }),
    ).rejects.toMatchObject({ exitCode: 7 });

    const serializedLog = JSON.stringify(logger.error.mock.calls);
    expect(serializedLog).not.toContain(credential);
    expect(serializedLog).not.toContain('DUMMY_SECRET_TOKEN_123');
    expect(logger.error).toHaveBeenCalledWith('Process failed', {
      error: {
        name: 'ExecaError',
        exitCode: 7,
        signal: undefined,
        timedOut: false,
        isCanceled: false,
      },
    });
  });
});
