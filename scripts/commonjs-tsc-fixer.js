import { readFile, writeFile, rm, mkdir, lstat, realpath } from 'node:fs/promises';
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
  const [canonicalRoot, canonicalDistRoot, canonicalFile, distStats, fileStats] = await Promise.all([
    realpath(rootPath),
    realpath(distRoot),
    realpath(file),
    lstat(distRoot),
    lstat(file),
  ]);
  if (distStats.isSymbolicLink()) {
    throw new Error(`Dist directory cannot be a symbolic link: ${JSON.stringify(distRoot)}`);
  }
  if (fileStats.isSymbolicLink()) {
    throw new Error(`Declaration source cannot be a symbolic link: ${JSON.stringify(file)}`);
  }
  assertContained(canonicalRoot, canonicalDistRoot, 'Dist directory');
  assertContained(canonicalDistRoot, canonicalFile, 'Declaration source');
}

async function prepareSafeTarget(rootPath, targetPath) {
  const canonicalRoot = await realpath(rootPath);
  const relativeTargetDirectory = relative(rootPath, dirname(targetPath));
  let canonicalTargetDirectory = canonicalRoot;

  for (const component of relativeTargetDirectory.split(sep).filter(Boolean)) {
    canonicalTargetDirectory = join(canonicalTargetDirectory, component);
    try {
      const componentStats = await lstat(canonicalTargetDirectory);
      if (componentStats.isSymbolicLink()) {
        throw new Error(`Generated declaration directory cannot contain symbolic links: ${canonicalTargetDirectory}`);
      }
      if (!componentStats.isDirectory()) {
        throw new Error(`Generated declaration directory component is not a directory: ${canonicalTargetDirectory}`);
      }
    } catch (error) {
      if (error?.code !== 'ENOENT') {
        throw error;
      }
      await mkdir(canonicalTargetDirectory);
    }
  }

  canonicalTargetDirectory = await realpath(canonicalTargetDirectory);
  assertContained(canonicalRoot, canonicalTargetDirectory, 'Generated declaration directory');
  const safeTargetPath = resolve(canonicalTargetDirectory, basename(targetPath));

  try {
    const targetStats = await lstat(safeTargetPath);
    if (targetStats.isSymbolicLink()) {
      throw new Error(`Generated declaration target cannot be a symbolic link: ${JSON.stringify(safeTargetPath)}`);
    }
  } catch (error) {
    if (error?.code !== 'ENOENT') {
      throw error;
    }
  }

  // These checks reject pre-existing symlink escapes. A concurrent same-user
  // path swap remains an OS-level race without portable openat-style APIs.
  return safeTargetPath;
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
  const rootPath = process.cwd();
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
        const distRoot = resolve(rootPath, 'dist');
        await assertSafeSource(rootPath, distRoot, file);

        if (key.endsWith('*')) {
          // For wildcard patterns, derive the subpath relative to dist/
          const dir = dirname(file);
          const subPath = slash(relative(distRoot, dir));
          // Replace wildcard text literally. String.replace would interpret
          // `$&`, `$`` and `$'` sequences in matched directory names.
          const filename = key.split('*').join(subPath);

          const targetPath = resolve(rootPath, `${filename}.d.ts`);
          assertContained(rootPath, targetPath, 'Generated declaration path');
          const safeTargetPath = await prepareSafeTarget(rootPath, targetPath);

          await writeFile(safeTargetPath, declarationExport(safeTargetPath, file));
        } else {
          const targetPath = resolve(rootPath, `${key}.d.ts`);
          assertContained(rootPath, targetPath, 'Generated declaration path');
          const safeTargetPath = await prepareSafeTarget(rootPath, targetPath);

          await writeFile(safeTargetPath, declarationExport(safeTargetPath, file));
        }
      }
    }
  }
}

await cleanupDtsFiles();
await writeDtsFiles();
