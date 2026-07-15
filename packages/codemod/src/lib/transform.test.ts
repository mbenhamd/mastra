import path from 'node:path';
import { beforeEach, describe, expect, it, vi } from 'vitest';

const mocks = vi.hoisted(() => ({
  execa: vi.fn(),
  onExit: vi.fn(),
}));

vi.mock('execa', () => ({
  execa: mocks.execa,
}));

vi.mock('signal-exit', () => ({
  onExit: mocks.onExit,
}));

import { transform } from './transform.js';

describe('transform runner', () => {
  beforeEach(() => {
    mocks.execa.mockReset();
    mocks.execa.mockResolvedValue({ stdout: '' });
    mocks.onExit.mockReset();
    mocks.onExit.mockReturnValue(vi.fn());
  });

  it('runs the declared jscodeshift bin with a shell-free argument array', async () => {
    const source = './project path; $(not-a-command)';

    await expect(
      transform(
        'v1/agent-abort-signal',
        source,
        {
          dry: true,
          print: true,
          verbose: true,
          jscodeshift: ['--extensions=ts,tsx', '--ignore-pattern', 'foo bar/**', '--run-in-band'],
        },
        { logStatus: false },
      ),
    ).resolves.toEqual({ errors: [], notImplementedErrors: [] });

    expect(mocks.execa).toHaveBeenCalledTimes(1);
    expect(mocks.execa).toHaveBeenCalledWith(
      process.execPath,
      [
        expect.stringMatching(/jscodeshift[/\\]bin[/\\]jscodeshift\.js$/),
        '-t',
        expect.stringMatching(/codemods[/\\]v1[/\\]agent-abort-signal\.js$/),
        path.resolve(source),
        '--parser',
        'tsx',
        '--ignore-pattern=**/node_modules/**',
        '--ignore-pattern=**/.*/**',
        '--ignore-pattern=**/dist/**',
        '--ignore-pattern=**/build/**',
        '--ignore-pattern=**/*.min.js',
        '--ignore-pattern=**/*.bundle.js',
        '--dry',
        '--print',
        '--verbose=2',
        '--extensions=ts,tsx',
        '--ignore-pattern',
        'foo bar/**',
        '--run-in-band',
      ],
      {
        encoding: 'utf8',
        maxBuffer: 100 * 1024 * 1024,
        detached: process.platform !== 'win32',
      },
    );
  });

  it.skipIf(process.platform === 'win32')(
    'terminates the jscodeshift process group when execution times out',
    async () => {
      vi.useFakeTimers();
      let rejectSubprocess!: (reason: Error) => void;
      const subprocess = Object.assign(
        new Promise<never>((_resolve, reject) => {
          rejectSubprocess = reject;
        }),
        { pid: 43_210, kill: vi.fn(() => true) },
      );
      mocks.execa.mockReturnValueOnce(subprocess);
      const kill = vi.spyOn(process, 'kill').mockImplementation((_pid, signal) => {
        if (signal === 'SIGTERM') {
          rejectSubprocess(new Error('terminated'));
        }
        return true;
      });

      try {
        const result = transform('v1/agent-abort-signal', '.', {}, { logStatus: false });
        const rejection = expect(result).rejects.toMatchObject({ name: 'CodemodTimeoutError' });

        await vi.advanceTimersByTimeAsync(5 * 60_000 + 1_000);
        await rejection;

        expect(kill).toHaveBeenCalledWith(-43_210, 'SIGTERM');
        expect(kill).toHaveBeenCalledWith(-43_210, 'SIGKILL');
      } finally {
        kill.mockRestore();
        vi.useRealTimers();
      }
    },
  );

  it.skipIf(process.platform === 'win32')('reports a graceful post-timeout exit as a timeout', async () => {
    vi.useFakeTimers();
    let resolveSubprocess!: (value: { stdout: string }) => void;
    const subprocess = Object.assign(
      new Promise<{ stdout: string }>(resolve => {
        resolveSubprocess = resolve;
      }),
      { pid: 43_211, kill: vi.fn(() => true) },
    );
    mocks.execa.mockReturnValueOnce(subprocess);
    const kill = vi.spyOn(process, 'kill').mockImplementation((_pid, signal) => {
      if (signal === 'SIGTERM') {
        resolveSubprocess({ stdout: 'completed after timeout' });
      }
      return true;
    });

    try {
      const result = transform('v1/agent-abort-signal', '.', {}, { logStatus: false });
      const rejection = expect(result).rejects.toMatchObject({ name: 'CodemodTimeoutError' });

      await vi.advanceTimersByTimeAsync(5 * 60_000 + 1_000);
      await rejection;

      expect(kill).toHaveBeenCalledWith(-43_211, 'SIGTERM');
      expect(kill).toHaveBeenCalledWith(-43_211, 'SIGKILL');
    } finally {
      kill.mockRestore();
      vi.useRealTimers();
    }
  });

  it.skipIf(process.platform === 'win32')('rejects after timeout even when the subprocess never settles', async () => {
    vi.useFakeTimers();
    const subprocess = Object.assign(new Promise<never>(() => {}), {
      pid: 43_213,
      kill: vi.fn(() => true),
    });
    mocks.execa.mockReturnValueOnce(subprocess);
    const kill = vi.spyOn(process, 'kill').mockReturnValue(true);

    try {
      const result = transform('v1/agent-abort-signal', '.', {}, { logStatus: false });
      const rejection = expect(result).rejects.toMatchObject({ name: 'CodemodTimeoutError' });

      await vi.advanceTimersByTimeAsync(5 * 60_000 + 1_000);
      await rejection;

      expect(kill).toHaveBeenCalledWith(-43_213, 'SIGTERM');
      expect(kill).toHaveBeenCalledWith(-43_213, 'SIGKILL');
    } finally {
      kill.mockRestore();
      vi.useRealTimers();
    }
  });

  it.skipIf(process.platform === 'win32')('terminates the process group when the parent exits', async () => {
    let resolveSubprocess!: (value: { stdout: string }) => void;
    const subprocess = Object.assign(
      new Promise<{ stdout: string }>(resolve => {
        resolveSubprocess = resolve;
      }),
      { pid: 43_212, kill: vi.fn(() => true) },
    );
    mocks.execa.mockReturnValueOnce(subprocess);
    const removeExitCleanup = vi.fn();
    let exitCleanup!: () => void;
    mocks.onExit.mockImplementationOnce(callback => {
      exitCleanup = callback;
      return removeExitCleanup;
    });
    const kill = vi.spyOn(process, 'kill').mockReturnValue(true);

    try {
      const result = transform('v1/agent-abort-signal', '.', {}, { logStatus: false });
      exitCleanup();
      resolveSubprocess({ stdout: '' });

      await expect(result).resolves.toEqual({ errors: [], notImplementedErrors: [] });
      expect(kill).toHaveBeenCalledWith(-43_212, 'SIGKILL');
      expect(removeExitCleanup).toHaveBeenCalledOnce();
    } finally {
      kill.mockRestore();
    }
  });

  it.runIf(process.platform !== 'win32')(
    'terminates the direct subprocess on a simulated Windows parent exit',
    async () => {
      const subprocess = Object.assign(Promise.resolve({ stdout: '' }), {
        pid: 43_214,
        kill: vi.fn(() => true),
      });
      mocks.execa.mockReturnValueOnce(subprocess);
      let exitCleanup!: () => void;
      mocks.onExit.mockImplementationOnce(callback => {
        exitCleanup = callback;
        return vi.fn();
      });
      const platform = vi.spyOn(process, 'platform', 'get').mockReturnValue('win32');

      try {
        const result = transform('v1/agent-abort-signal', '.', {}, { logStatus: false });
        exitCleanup();
        await result;

        expect(subprocess.kill).toHaveBeenCalledWith('SIGKILL');
      } finally {
        platform.mockRestore();
      }
    },
  );

  it('passes shell-control-looking text as inert argument data', async () => {
    await transform(
      'v1/agent-abort-signal',
      '.',
      { jscodeshift: ['--label', 'a;b|c', '--literal=$(other-command)', '--comment=#value'] },
      { logStatus: false },
    );

    const args = mocks.execa.mock.calls[0]?.[1] as string[];
    expect(args).toContain('a;b|c');
    expect(args).toContain('--literal=$(other-command)');
    expect(args).toContain('--comment=#value');
  });

  it('preserves literal dollar forms and shell metacharacters as discrete arguments', async () => {
    await transform(
      'v1/agent-abort-signal',
      '.',
      { jscodeshift: ['--pattern=${HOME};a|b', '--literal=$HOME'] },
      { logStatus: false },
    );

    const args = mocks.execa.mock.calls[0]?.[1] as string[];
    expect(args).toContain('--pattern=${HOME};a|b');
    expect(args).toContain('--literal=$HOME');
  });

  it('preserves quoted and unquoted Windows path separators', async () => {
    const jscodeshift = [
      '--ignore-pattern',
      String.raw`C:\project path\src\**`,
      '--quoted-root',
      'C:\\project path\\',
      '--root',
      String.raw`C:\project\src\**`,
      '--unc',
      String.raw`\\server\share\**`,
      '--drive-root',
      'C:\\',
    ];
    await transform('v1/agent-abort-signal', '.', { jscodeshift }, { logStatus: false });

    const args = mocks.execa.mock.calls[0]?.[1] as string[];
    expect(args).toContain(String.raw`C:\project path\src\**`);
    expect(args).toContain('C:\\project path\\');
    expect(args).toContain(String.raw`C:\project\src\**`);
    expect(args).toContain('C:\\');
    expect(args).toContain(String.raw`\\server\share\**`);
  });
});
