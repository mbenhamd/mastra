import { execFile } from 'node:child_process';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { promisify } from 'node:util';
import { afterEach, describe, expect, it } from 'vitest';
import { buildArgs, getJscodeshiftBin } from './transform.js';

const run = promisify(execFile);
const fixtures: string[] = [];

afterEach(async () => {
  await Promise.all(fixtures.splice(0).map(fixture => rm(fixture, { recursive: true, force: true })));
});

describe('jscodeshift process integration', () => {
  it('accepts verbose level 2 through the installed CLI parser', async () => {
    const fixture = await mkdtemp(path.join(tmpdir(), 'mastra-codemod-runner-'));
    fixtures.push(fixture);
    const transformPath = path.join(fixture, 'transform.cjs');
    const sourcePath = path.join(fixture, 'source.ts');
    await writeFile(transformPath, 'module.exports = file => file.source;');
    await writeFile(sourcePath, 'export const value: number = 1;');

    const args = buildArgs(transformPath, sourcePath, {
      dry: true,
      verbose: true,
      jscodeshift: ['--run-in-band'],
    });
    const { stdout } = await run(process.execPath, [getJscodeshiftBin(), ...args], { encoding: 'utf8' });

    expect(stdout).toContain('All done.');
    expect(stdout).toContain('1 unmodified');
  });

  it('places explicit custom options after defaults so the caller has final precedence', () => {
    const args = buildArgs('/tmp/transform.cjs', '/tmp/source.ts', {
      jscodeshift: ['--parser=ts', '--ignore-pattern=custom/**'],
    });

    expect(args.lastIndexOf('--parser=ts')).toBeGreaterThan(args.lastIndexOf('tsx'));
    expect(args.at(-1)).toBe('--ignore-pattern=custom/**');
  });
});
