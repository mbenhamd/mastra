import { execFile } from 'node:child_process';
import { mkdir, mkdtemp, readFile, rm, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { promisify } from 'node:util';
import { expect, it } from 'vitest';

const run = promisify(execFile);
const generatorUrl = new URL('../packages/_types-builder/src/index.js', import.meta.url);
const nodeModules = fileURLToPath(new URL('../node_modules', import.meta.url));

it('preserves native module resolution in generated declarations', async () => {
  const root = await mkdtemp(join(tmpdir(), 'types-builder-resolution-'));
  try {
    const fixtureNodeModules = join(root, 'node_modules');
    await mkdir(fixtureNodeModules);
    for (const name of ['.bin', 'typescript']) {
      await symlink(
        join(nodeModules, name),
        join(fixtureNodeModules, name),
        process.platform === 'win32' ? 'junction' : 'dir',
      );
    }
    const dependency = join(fixtureNodeModules, 'types-builder-dependency');
    await mkdir(dependency);
    await writeFile(
      join(dependency, 'package.json'),
      JSON.stringify({ name: 'types-builder-dependency', version: '1.0.0', exports: './index.d.ts' }),
    );
    await writeFile(
      join(dependency, 'index.d.ts'),
      "export type { CommonJs } from './common.cjs';\nexport type { ModuleJs } from './module.mjs';\nexport type { ScriptJs } from './script.js';\n",
    );
    for (const [file, declaration] of [
      ['common.d.cts', "export type CommonJs = 'commonjs';\n"],
      ['common.d.ts', "export type CommonJs = 'wrong';\n"],
      ['module.d.mts', "export type ModuleJs = 'module';\n"],
      ['module.d.ts', "export type ModuleJs = 'wrong';\n"],
      ['script.d.ts', "export type ScriptJs = 'script';\n"],
      ['script.d.cts', "export type ScriptJs = 'wrong';\n"],
    ] as const) {
      await writeFile(join(dependency, file), declaration);
    }
    await mkdir(join(root, 'src', 'utils'), { recursive: true });
    await mkdir(join(root, 'src', 'directory'), { recursive: true });
    await writeFile(join(root, 'package.json'), JSON.stringify({ name: 'types-builder-fixture', type: 'module' }));
    await writeFile(
      join(root, 'tsconfig.build.json'),
      JSON.stringify({
        compilerOptions: {
          target: 'ES2022',
          module: 'ESNext',
          moduleResolution: 'Bundler',
          strict: true,
          types: [],
          declaration: true,
          emitDeclarationOnly: true,
          noEmitOnError: true,
          rootDir: 'src',
          outDir: 'dist',
        },
        include: ['src'],
      }),
    );
    await writeFile(
      join(root, 'src', 'index.ts'),
      "export type { FileValue } from './utils';\nexport type { DirectoryValue } from './directory';\nexport type { CommonJs, ModuleJs, ScriptJs } from 'types-builder-dependency';\n",
    );
    await writeFile(join(root, 'src', 'utils.ts'), "export type FileValue = 'file';\n");
    await writeFile(join(root, 'src', 'utils', 'index.ts'), "export type FileValue = 'wrong-directory';\n");
    await writeFile(join(root, 'src', 'directory', 'index.ts'), "export type DirectoryValue = 'directory';\n");

    await run(
      process.execPath,
      [
        '--input-type=module',
        '--eval',
        `import { generateTypes } from ${JSON.stringify(generatorUrl.href)}; await generateTypes(${JSON.stringify(root)}, new Set(['types-builder-dependency']));`,
      ],
      { cwd: root, env: { ...process.env, npm_config_offline: 'true' } },
    );

    const declarations = await readFile(join(root, 'dist', 'index.d.ts'), 'utf8');
    expect(declarations).toContain("from './utils.js'");
    expect(declarations).toContain("from './directory/index.js'");
    const embedded = join(root, 'dist', '_types', 'types-builder-dependency');
    expect(await readFile(join(embedded, 'common.d.cts'), 'utf8')).toContain("CommonJs = 'commonjs'");
    expect(await readFile(join(embedded, 'module.d.mts'), 'utf8')).toContain("ModuleJs = 'module'");
    expect(await readFile(join(embedded, 'script.d.ts'), 'utf8')).toContain("ScriptJs = 'script'");
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}, 30_000);
