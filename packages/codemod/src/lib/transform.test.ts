import path from 'node:path';
import { beforeEach, describe, expect, it, vi } from 'vitest';

const mocks = vi.hoisted(() => ({
  exec: vi.fn(),
  execFile: vi.fn(),
}));

vi.mock('node:child_process', () => ({
  default: {
    exec: mocks.exec,
    execFile: mocks.execFile,
  },
}));

vi.mock('node:util', () => ({
  default: {
    promisify: (fn: unknown) => fn,
  },
}));

import { transform } from './transform.js';

describe('transform runner', () => {
  beforeEach(() => {
    mocks.exec.mockReset();
    mocks.execFile.mockReset();
    mocks.exec.mockResolvedValue({ stdout: '' });
    mocks.execFile.mockResolvedValue({ stdout: '' });
  });

  it('runs jscodeshift with a shell-free argument array and preserves quoted custom values', async () => {
    const source = './project path; $(not-a-command)';

    await expect(
      transform(
        'v1/agent-abort-signal',
        source,
        {
          dry: true,
          print: true,
          verbose: true,
          jscodeshift: '--extensions=ts,tsx --ignore-pattern "foo bar/**" --run-in-band',
        },
        { logStatus: false },
      ),
    ).resolves.toEqual({ errors: [], notImplementedErrors: [] });

    expect(mocks.exec).not.toHaveBeenCalled();
    expect(mocks.execFile).toHaveBeenCalledTimes(1);
    expect(mocks.execFile).toHaveBeenCalledWith(
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
        '--verbose',
        '--extensions=ts,tsx',
        '--ignore-pattern',
        'foo bar/**',
        '--run-in-band',
      ],
      { encoding: 'utf8' },
    );
  });

  it('passes shell-control-looking text as inert argument data', async () => {
    await transform(
      'v1/agent-abort-signal',
      '.',
      { jscodeshift: '--label "a;b|c" --literal=$(other-command) --comment=#value' },
      { logStatus: false },
    );

    const args = mocks.execFile.mock.calls[0]?.[1] as string[];
    expect(args).toContain('a;b|c');
    expect(args).toContain('--literal=$(other-command)');
    expect(args).toContain('--comment=#value');
    expect(mocks.exec).not.toHaveBeenCalled();
  });

  it('rejects an unterminated custom option quote', async () => {
    await expect(
      transform('v1/agent-abort-signal', '.', { jscodeshift: '--ignore-pattern "unterminated' }, { logStatus: false }),
    ).rejects.toThrow('unterminated quote');
    expect(mocks.execFile).not.toHaveBeenCalled();
  });

  it('preserves literal dollar forms and quoted shell metacharacters', async () => {
    await transform(
      'v1/agent-abort-signal',
      '.',
      { jscodeshift: '--pattern="${HOME};a|b" --literal=$HOME' },
      { logStatus: false },
    );

    const args = mocks.execFile.mock.calls[0]?.[1] as string[];
    expect(args).toContain('--pattern=${HOME};a|b');
    expect(args).toContain('--literal=$HOME');
  });

  it('preserves quoted and unquoted Windows path separators', async () => {
    const jscodeshift =
      String.raw`--ignore-pattern "C:\project path\src\**" --quoted-root "C:\project path\\" --root C:\project\src\** --unc \\server\share\**` +
      ' --drive-root C:\\';
    await transform('v1/agent-abort-signal', '.', { jscodeshift }, { logStatus: false });

    const args = mocks.execFile.mock.calls[0]?.[1] as string[];
    expect(args).toContain(String.raw`C:\project path\src\**`);
    expect(args).toContain('C:\\project path\\');
    expect(args).toContain(String.raw`C:\project\src\**`);
    expect(args).toContain('C:\\');
    expect(args).toContain(String.raw`\\server\share\**`);
  });
});
