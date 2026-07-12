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
});
