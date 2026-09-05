import { randomUUID } from 'node:crypto';
import { lstat, mkdir, open, readFile, realpath, rename, rm } from 'node:fs/promises';
import { basename, dirname, isAbsolute, join, relative, resolve, sep } from 'node:path';
import { glob as globby } from 'tinyglobby';

/** Convert Windows backslashes to posix forward slashes */
function slash(p) {
  return p.replaceAll('\\', '/');
}

function assertContained(root, candidate, description) {
  const relativePath = relative(root, candidate);
  if (relativePath === '..' || relativePath.startsWith(`..${sep}`) || isAbsolute(relativePath)) {
    throw new Error(`${description} resolves outside the package root: ${JSON.stringify(candidate)}`);
  }
}

async function assertSafeSource(rootPath, distRoot, file) {
  const [distStats, fileStats] = await Promise.all([lstat(distRoot), lstat(file)]);
  if (distStats.isSymbolicLink()) {
    throw new Error(`Dist directory cannot be a symbolic link: ${JSON.stringify(distRoot)}`);
  }
  if (fileStats.isSymbolicLink()) {
    throw new Error(`Declaration source cannot be a symbolic link: ${JSON.stringify(file)}`);
  }
  const [canonicalDistRoot, canonicalFile] = await Promise.all([realpath(distRoot), realpath(file)]);
  assertContained(rootPath, canonicalDistRoot, 'Dist directory');
  assertContained(canonicalDistRoot, canonicalFile, 'Declaration source');
}

async function prepareSafeTarget(rootPath, targetPath) {
  assertContained(rootPath, targetPath, 'Generated declaration path');
  let targetDirectory = rootPath;
  for (const component of relative(rootPath, dirname(targetPath)).split(sep).filter(Boolean)) {
    targetDirectory = join(targetDirectory, component);
    let componentStats;
    try {
      componentStats = await lstat(targetDirectory);
    } catch (error) {
      if (error?.code !== 'ENOENT') {
        throw error;
      }
      await mkdir(targetDirectory);
      continue;
    }
    if (componentStats.isSymbolicLink()) {
      throw new Error(
        `Generated declaration directory cannot contain symbolic links: ${JSON.stringify(targetDirectory)}`,
      );
    }
    if (!componentStats.isDirectory()) {
      throw new Error(
        `Generated declaration directory component is not a directory: ${JSON.stringify(targetDirectory)}`,
      );
    }
  }

  const canonicalDirectory = await realpath(targetDirectory);
  assertContained(rootPath, canonicalDirectory, 'Generated declaration directory');
  const safeTargetPath = join(canonicalDirectory, basename(targetPath));
  let targetStats;
  try {
    targetStats = await lstat(safeTargetPath);
  } catch (error) {
    if (error?.code !== 'ENOENT') {
      throw error;
    }
  }
  if (targetStats?.isSymbolicLink()) {
    throw new Error(`Generated declaration target cannot be a symbolic link: ${JSON.stringify(safeTargetPath)}`);
  }

  // These checks reject pre-existing symlink escapes. Concurrent directory
  // swaps remain an OS-level race without portable openat-style APIs.
  return safeTargetPath;
}

async function replaceFileAtomically(targetPath, contents) {
  const temporaryPath = join(dirname(targetPath), `.${basename(targetPath)}.${randomUUID()}.tmp`);
  const temporaryFile = await open(temporaryPath, 'wx');
  try {
    try {
      await temporaryFile.writeFile(contents);
    } finally {
      await temporaryFile.close();
    }
    await rename(temporaryPath, targetPath);
  } finally {
    await rm(temporaryPath, { force: true });
  }
}

function declarationExport(targetPath, sourcePath) {
  const nativeRelativePath = relative(dirname(targetPath), sourcePath);
  if (sep === '/' && nativeRelativePath.includes('\\')) {
    throw new Error(`Declaration paths cannot contain backslashes on POSIX: ${JSON.stringify(sourcePath)}`);
  }
  const relativePath = slash(nativeRelativePath).replace(/\/index\.d\.ts$/, '');
  return `export * from ${JSON.stringify(`./${relativePath}`)};`;
}

async function cleanupDtsFiles() {
  const rootPath = process.cwd();
  const files = await globby('./*.d.ts', { cwd: rootPath });

  for (const file of files) {
    await rm(join(rootPath, file), { force: true });
  }
}

async function writeDtsFiles() {
  const rootPath = await realpath(process.cwd());
  const distRoot = join(rootPath, 'dist');
  const packageJson = JSON.parse(await readFile(join(rootPath, 'package.json')));

  const exports = packageJson.exports;

  // Handle specific path exports
  for (const [key, value] of Object.entries(exports)) {
    if (key !== '.' && value?.require?.types) {
      const pattern = value.require.types;
      const matches = await globby(pattern, {
        cwd: rootPath,
        absolute: true,
        followSymbolicLinks: false,
      });

      for (const file of matches) {
        await assertSafeSource(rootPath, distRoot, file);
        let filename = key;
        if (key.endsWith('*')) {
          // For wildcard patterns, derive the subpath relative to dist/
          const dir = dirname(file);
          const subPath = slash(relative(distRoot, dir));
          // split/join replaces every '*' and doesn't interpret '$' patterns
          // in the replacement (CodeQL js/incomplete-sanitization)
          filename = key.split('*').join(subPath);
        }

        const targetPath = resolve(rootPath, `${filename}.d.ts`);
        const safeTargetPath = await prepareSafeTarget(rootPath, targetPath);
        await replaceFileAtomically(safeTargetPath, declarationExport(safeTargetPath, file));
      }
    }
  }
}

await cleanupDtsFiles();
await writeDtsFiles();
