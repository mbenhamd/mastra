import path from 'node:path';
import { beforeEach, describe, expect, it, vi } from 'vitest';

const mocks = vi.hoisted(() => ({
  execa: vi.fn(),
}));

vi.mock('execa', () => ({
  execa: mocks.execa,
}));

import { transform } from './transform.js';

describe('transform runner', () => {
  beforeEach(() => {
    mocks.execa.mockReset();
    mocks.execa.mockResolvedValue({ stdout: '' });
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
      { encoding: 'utf8', maxBuffer: 100 * 1024 * 1024 },
    );
  });

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
