import { execFile } from 'node:child_process';
import { access, link, mkdir, mkdtemp, readFile, rm, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join, sep } from 'node:path';
import { fileURLToPath } from 'node:url';
import { promisify } from 'node:util';
import { afterEach, describe, expect, it } from 'vitest';

const run = promisify(execFile);
const fixerPath = fileURLToPath(new URL('./commonjs-tsc-fixer.js', import.meta.url));
const fixtures: string[] = [];
const directoryLinkType = process.platform === 'win32' ? 'junction' : 'dir';

async function createFixture(exports: Record<string, unknown>): Promise<string> {
  const fixtureRoot = await mkdtemp(join(tmpdir(), 'commonjs-tsc-fixer-'));
  const root = join(fixtureRoot, 'package');
  fixtures.push(fixtureRoot);
  await mkdir(root);
  await writeFile(join(root, 'package.json'), JSON.stringify({ exports }));
  return root;
}

afterEach(async () => {
  await Promise.all(fixtures.splice(0).map(root => rm(root, { recursive: true, force: true })));
});

describe('commonjs-tsc-fixer', () => {
  it('uses replacement tokens and repeated wildcards literally', async () => {
    const root = await createFixture({
      './*-mirror-*': { require: { types: './dist/*/index.d.ts' } },
    });
    const directories = ['$&', "$'", '$`'];
    for (const directory of directories) {
      await mkdir(join(root, 'dist', directory), { recursive: true });
      await writeFile(join(root, 'dist', directory, 'index.d.ts'), 'export type Example = true;');
    }

    await run(process.execPath, [fixerPath], { cwd: root });

    for (const directory of directories) {
      await expect(readFile(join(root, `${directory}-mirror-${directory}.d.ts`), 'utf8')).resolves.toBe(
        `export * from ${JSON.stringify(`./dist/${directory}`)};`,
      );
    }
  });

  it.runIf(process.platform !== 'win32')('escapes quotes and control characters in module specifiers', async () => {
    const root = await createFixture({
      './*': { require: { types: './dist/*/index.d.ts' } },
    });
    const directory = 'quote"line\nbreak';
    await mkdir(join(root, 'dist', directory), { recursive: true });
    await writeFile(join(root, 'dist', directory, 'index.d.ts'), 'export type Example = true;');

    await run(process.execPath, [fixerPath], { cwd: root });

    await expect(readFile(join(root, `${directory}.d.ts`), 'utf8')).resolves.toBe(
      `export * from ${JSON.stringify(`./dist/${directory}`)};`,
    );
  });

  it('rejects generated declaration paths outside the package root', async () => {
    const root = await createFixture({
      './../escaped-*': { require: { types: './dist/*/index.d.ts' } },
    });
    await mkdir(join(root, 'dist', 'example'), { recursive: true });
    await writeFile(join(root, 'dist', 'example', 'index.d.ts'), 'export type Example = true;');

    await expect(run(process.execPath, [fixerPath], { cwd: root })).rejects.toThrow(/outside the package root/);
    await expect(access(join(dirname(root), 'escaped-example.d.ts'))).rejects.toThrow();
  });

  it('rejects a symlinked output directory that escapes the package', async () => {
    const root = await createFixture({
      './nested/deeper/*': { require: { types: './dist/*/index.d.ts' } },
    });
    const outside = join(dirname(root), 'outside');
    await mkdir(join(root, 'dist', 'example'), { recursive: true });
    await writeFile(join(root, 'dist', 'example', 'index.d.ts'), 'export type Example = true;');
    await mkdir(outside);
    await symlink(outside, join(root, 'nested'), directoryLinkType);

    await expect(run(process.execPath, [fixerPath], { cwd: root })).rejects.toThrow(
      /Generated declaration directory cannot contain symbolic links/,
    );
    await expect(access(join(outside, 'deeper'))).rejects.toThrow();
  });

  it('does not follow declaration-source directory symlinks outside dist', async () => {
    const root = await createFixture({
      './*': { require: { types: './dist/*/index.d.ts' } },
    });
    const outside = join(dirname(root), 'outside');
    await mkdir(outside);
    await writeFile(join(outside, 'index.d.ts'), 'export type Outside = true;');
    await mkdir(join(root, 'dist'));
    await symlink(outside, join(root, 'dist', 'linked'), directoryLinkType);

    await run(process.execPath, [fixerPath], { cwd: root });

    await expect(access(join(root, 'linked.d.ts'))).rejects.toThrow();
  });

  it('rejects a symlinked dist root', async () => {
    const root = await createFixture({
      './*': { require: { types: './dist/*/index.d.ts' } },
    });
    const outside = join(dirname(root), 'outside');
    await mkdir(join(outside, 'example'), { recursive: true });
    await writeFile(join(outside, 'example', 'index.d.ts'), 'export type Outside = true;');
    await symlink(outside, join(root, 'dist'), directoryLinkType);

    await expect(run(process.execPath, [fixerPath], { cwd: root })).rejects.toThrow(
      /Dist directory cannot be a symbolic link/,
    );
    await expect(access(join(root, 'example.d.ts'))).rejects.toThrow();
  });

  it('rejects declaration sources outside dist', async () => {
    const root = await createFixture({
      './*': { require: { types: './outside/*/index.d.ts' } },
    });
    await mkdir(join(root, 'dist'));
    await mkdir(join(root, 'outside', 'example'), { recursive: true });
    await writeFile(join(root, 'outside', 'example', 'index.d.ts'), 'export type Outside = true;');

    await expect(run(process.execPath, [fixerPath], { cwd: root })).rejects.toThrow(
      /Declaration source resolves outside the package root/,
    );
    await expect(access(join(root, 'example.d.ts'))).rejects.toThrow();
  });

  it.runIf(sep === '/')('rejects POSIX declaration paths containing literal backslashes', async () => {
    const root = await createFixture({
      './*': { require: { types: './dist/*/index.d.ts' } },
    });
    await mkdir(join(root, 'dist', 'back\\slash'), { recursive: true });
    await writeFile(join(root, 'dist', 'back\\slash', 'index.d.ts'), 'export type Example = true;');

    await expect(run(process.execPath, [fixerPath], { cwd: root })).rejects.toThrow(
      /Declaration paths cannot contain backslashes on POSIX/,
    );
  });

  it.runIf(process.platform !== 'win32')('rejects an existing symlink declaration target', async () => {
    const root = await createFixture({
      './nested/*': { require: { types: './dist/*/index.d.ts' } },
    });
    const outside = join(dirname(root), 'outside.d.ts');
    await writeFile(outside, 'do not overwrite');
    await mkdir(join(root, 'dist', 'example'), { recursive: true });
    await writeFile(join(root, 'dist', 'example', 'index.d.ts'), 'export type Example = true;');
    await mkdir(join(root, 'nested'));
    await symlink(outside, join(root, 'nested', 'example.d.ts'), 'file');

    await expect(run(process.execPath, [fixerPath], { cwd: root })).rejects.toThrow(
      /Generated declaration target cannot be a symbolic link/,
    );
    await expect(readFile(outside, 'utf8')).resolves.toBe('do not overwrite');
  });

  it('atomically replaces a hard-linked declaration without mutating the other link', async () => {
    const root = await createFixture({
      './nested/*': { require: { types: './dist/*/index.d.ts' } },
    });
    const outside = join(dirname(root), 'outside.d.ts');
    const target = join(root, 'nested', 'example.d.ts');
    await writeFile(outside, 'do not overwrite');
    await mkdir(join(root, 'dist', 'example'), { recursive: true });
    await writeFile(join(root, 'dist', 'example', 'index.d.ts'), 'export type Example = true;');
    await mkdir(join(root, 'nested'));
    await link(outside, target);

    await run(process.execPath, [fixerPath], { cwd: root });

    await expect(readFile(outside, 'utf8')).resolves.toBe('do not overwrite');
    await expect(readFile(target, 'utf8')).resolves.toBe('export * from "./../dist/example";');
  });
});
