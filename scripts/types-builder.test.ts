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

it('preserves file-before-directory resolution in generated declarations', async () => {
  const root = await mkdtemp(join(tmpdir(), 'types-builder-resolution-'));
  try {
    await symlink(nodeModules, join(root, 'node_modules'), process.platform === 'win32' ? 'junction' : 'dir');
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
      "export type { FileValue } from './utils';\nexport type { DirectoryValue } from './directory';\n",
    );
    await writeFile(join(root, 'src', 'utils.ts'), "export type FileValue = 'file';\n");
    await writeFile(join(root, 'src', 'utils', 'index.ts'), "export type FileValue = 'wrong-directory';\n");
    await writeFile(join(root, 'src', 'directory', 'index.ts'), "export type DirectoryValue = 'directory';\n");

    await run(
      process.execPath,
      [
        '--input-type=module',
        '--eval',
        `import { generateTypes } from ${JSON.stringify(generatorUrl.href)}; await generateTypes(${JSON.stringify(root)});`,
      ],
      { cwd: root, env: { ...process.env, npm_config_offline: 'true' } },
    );

    const declarations = await readFile(join(root, 'dist', 'index.d.ts'), 'utf8');
    expect(declarations).toContain("from './utils.js'");
    expect(declarations).toContain("from './directory/index.js'");
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}, 30_000);
