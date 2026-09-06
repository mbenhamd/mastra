import { execFile } from 'node:child_process';
import { mkdir, mkdtemp, readFile, rm, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { promisify } from 'node:util';
import { afterEach, describe, expect, it } from 'vitest';
import { generateTypes } from '../packages/_types-builder/src/index.js';

const run = promisify(execFile);
const fixtures: string[] = [];
const typesBuilderRoot = fileURLToPath(new URL('../packages/_types-builder/', import.meta.url));

async function writeFixtureFile(root: string, file: string, contents: string) {
  const filePath = join(root, file);
  await mkdir(dirname(filePath), { recursive: true });
  await writeFile(filePath, contents);
}

async function createFixture() {
  const root = await mkdtemp(join(tmpdir(), 'types-builder-'));
  fixtures.push(root);

  await writeFixtureFile(
    root,
    'package.json',
    JSON.stringify({ name: '@fixture/consumer', type: 'module', dependencies: { '@fixture/bundle': '1.0.0' } }),
  );
  await writeFixtureFile(
    root,
    'tsconfig.build.json',
    JSON.stringify({
      compilerOptions: {
        declaration: true,
        emitDeclarationOnly: true,
        outDir: 'dist',
        rootDir: 'src',
        module: 'ESNext',
        moduleResolution: 'Bundler',
        target: 'ES2022',
        strict: true,
      },
      include: ['src/**/*.ts'],
    }),
  );
  await writeFixtureFile(
    root,
    'src/index.ts',
    [
      "import type { FileSentinel } from './collision';",
      "import type { FileSentinel as ExplicitFileSentinel } from './collision.js';",
      "import type { ESM_SENTINEL } from '@fixture/bundle/esm';",
      "import type { CJS_SENTINEL } from '@fixture/bundle/cjs';",
      'export type Result = FileSentinel & ExplicitFileSentinel & ESM_SENTINEL & CJS_SENTINEL;',
    ].join('\n'),
  );
  await writeFixtureFile(root, 'src/collision.ts', 'export type FileSentinel = { source: "file" };\n');
  await writeFixtureFile(root, 'src/collision/index.ts', 'export type FileSentinel = { source: "directory" };\n');

  const bundleRoot = join(root, 'node_modules/@fixture/bundle');
  await writeFixtureFile(
    bundleRoot,
    'package.json',
    JSON.stringify({
      name: '@fixture/bundle',
      version: '1.0.0',
      type: 'module',
      exports: {
        '.': { types: './dist/esm.mjs', import: './dist/esm.mjs', require: './dist/cjs.cjs' },
        './esm': { types: './dist/esm.mjs', import: './dist/esm.mjs', require: './dist/esm.cjs' },
        './cjs': { types: './dist/cjs.cjs', import: './dist/cjs.mjs', require: './dist/cjs.cjs' },
      },
    }),
  );
  await writeFixtureFile(bundleRoot, 'dist/esm.mjs', '');
  await writeFixtureFile(bundleRoot, 'dist/cjs.cjs', '');
  await writeFixtureFile(
    bundleRoot,
    'dist/esm.d.mts',
    'import type { ESM_LEAF } from "./leaf.mjs";\nexport type ESM_SENTINEL = { esm: ESM_LEAF["value"] };\n',
  );
  await writeFixtureFile(bundleRoot, 'dist/esm.d.ts', 'export type ESM_SENTINEL = { esm: "wrong-ts" };\n');
  await writeFixtureFile(
    bundleRoot,
    'dist/cjs.d.cts',
    'import type { CJS_LEAF } from "./leaf.cjs";\nexport type CJS_SENTINEL = { cjs: CJS_LEAF["value"] };\n',
  );
  await writeFixtureFile(bundleRoot, 'dist/cjs.d.ts', 'export type CJS_SENTINEL = { cjs: "wrong-ts" };\n');
  await writeFixtureFile(bundleRoot, 'dist/leaf.mjs', '');
  await writeFixtureFile(bundleRoot, 'dist/leaf.cjs', '');
  await writeFixtureFile(bundleRoot, 'dist/leaf.d.mts', 'export type ESM_LEAF = { value: "mts" };\n');
  await writeFixtureFile(bundleRoot, 'dist/leaf.d.cts', 'export type CJS_LEAF = { value: "cts" };\n');
  await writeFixtureFile(bundleRoot, 'dist/leaf.d.ts', 'export type ESM_LEAF = { value: "wrong-ts" };\n');

  const binDir = join(root, 'node_modules/.bin');
  await mkdir(binDir, { recursive: true });
  await symlink(join(process.cwd(), 'node_modules/.bin/tsc'), join(binDir, 'tsc'));
  return root;
}

afterEach(async () => {
  await Promise.all(fixtures.splice(0).map(fixture => rm(fixture, { recursive: true, force: true })));
});

describe('types-builder declaration resolution', () => {
  it('generates file-first imports and preserves bundled ESM/CJS declaration identity', async () => {
    const root = await createFixture();

    await generateTypes(root, new Set(['@fixture/bundle']));

    const generated = await readFile(join(root, 'dist/index.d.ts'), 'utf8');
    expect(generated).toContain("from './collision.js'");
    expect(generated.match(/from '\.\/collision\.js'/g)).toHaveLength(2);
    expect(generated).not.toContain("from './collision/index.js'");
    expect(generated).toContain('_types/@fixture_bundle/dist/esm.d.mts');
    expect(generated).toContain('_types/@fixture_bundle/dist/cjs.d.cts');
    await expect(readFile(join(root, 'dist/_types/@fixture_bundle/dist/esm.d.mts'), 'utf8')).resolves.toContain(
      './leaf.mjs',
    );
    await expect(readFile(join(root, 'dist/_types/@fixture_bundle/dist/cjs.d.cts'), 'utf8')).resolves.toContain(
      './leaf.cjs',
    );
    await rm(join(root, 'node_modules/@fixture/bundle'), { recursive: true, force: true });

    await writeFixtureFile(
      root,
      'consumer.ts',
      [
        "import type { Result } from './dist/index.js';",
        'const result: Result = { source: "file", esm: "mts", cjs: "cts" };',
        'void result;',
      ].join('\n'),
    );
    await writeFixtureFile(
      root,
      'tsconfig.consumer.json',
      JSON.stringify({
        compilerOptions: { module: 'NodeNext', moduleResolution: 'NodeNext', target: 'ES2022', strict: true },
        include: ['consumer.ts'],
      }),
    );
    await run(join(process.cwd(), 'node_modules/.bin/tsc'), ['-p', 'tsconfig.consumer.json', '--noEmit'], {
      cwd: root,
    });
  });
});
