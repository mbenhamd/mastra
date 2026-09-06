#!/usr/bin/env node

import { createRequire } from 'node:module';
import { promises as fs } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { execFileSync } from 'node:child_process';

const require = createRequire(path.join(process.cwd(), 'package.json'));
const ts = (() => {
  try {
    return require('typescript-classic');
  } catch {
    return require('typescript');
  }
})();
const npm = process.platform === 'win32' ? 'npm.cmd' : 'npm';

function command(args, cwd, timeout = 60_000) {
  try {
    return {
      ok: true,
      output: execFileSync(args[0], args.slice(1), {
        cwd,
        encoding: 'utf8',
        timeout,
        stdio: ['ignore', 'pipe', 'pipe'],
      }),
    };
  } catch (error) {
    return { ok: false, output: `${error.stdout ?? ''}${error.stderr ?? ''}` };
  }
}

async function pack(packageDir, destination) {
  const result = command(
    [npm, 'pack', '--json', '--ignore-scripts', '--offline', '--pack-destination', destination],
    packageDir,
  );
  if (!result.ok) throw new Error(`npm pack failed:\n${result.output}`);
  const entries = JSON.parse(result.output);
  const metadata = Array.isArray(entries) ? entries[0] : Object.values(entries)[0];
  if (!metadata?.filename) throw new Error(`npm pack returned no tarball filename:\n${result.output}`);
  return path.join(destination, metadata.filename);
}

function exportTargets(value, targets = []) {
  if (typeof value === 'string') targets.push(value);
  else if (Array.isArray(value)) value.forEach(item => exportTargets(item, targets));
  else if (value && typeof value === 'object') Object.values(value).forEach(item => exportTargets(item, targets));
  return targets;
}

function declarationEdges(file, source, root) {
  const result = [];
  const add = node => {
    if (!node) return;
    const specifier = ts.isLiteralTypeNode(node) ? node.literal.text : node?.text;
    if (specifier?.startsWith('.')) result.push({ file, specifier, source, root });
  };
  const visit = node => {
    if (ts.isImportDeclaration(node) || ts.isExportDeclaration(node)) add(node.moduleSpecifier);
    else if (ts.isImportEqualsDeclaration(node) && ts.isExternalModuleReference(node.moduleReference))
      add(node.moduleReference.expression);
    else if (ts.isImportTypeNode(node)) add(node.argument);
    ts.forEachChild(node, visit);
  };
  visit(source);
  return result;
}

/** Validate the files in an npm packed archive. This is also used by the real package lane. */
export async function validatePackedArchive(archive) {
  const temp = await fs.mkdtemp(path.join(os.tmpdir(), 'mastra-packed-check-'));
  try {
    const unpack = command(['tar', '-xzf', archive, '-C', temp], process.cwd());
    if (!unpack.ok) return ['archive could not be unpacked'];
    const root = path.join(temp, 'package');
    const packageJson = JSON.parse(await fs.readFile(path.join(root, 'package.json'), 'utf8'));
    const files = [];
    const walk = async dir => {
      for (const entry of await fs.readdir(dir, { withFileTypes: true })) {
        const full = path.join(dir, entry.name);
        if (entry.isDirectory()) await walk(full);
        else files.push(path.relative(root, full).replaceAll(path.sep, '/'));
      }
    };
    await walk(root);
    const present = new Set(files);
    const errors = [];
    for (const target of exportTargets(packageJson.exports)) {
      if (!target.startsWith('./') || target.includes('*')) continue;
      const relative = target.slice(2);
      if (!present.has(relative)) errors.push(`missing export target ${target}`);
    }
    const declarationFiles = files.filter(file => /\.d\.(?:ts|mts|cts)$/.test(file));
    const options = {
      module: ts.ModuleKind.NodeNext,
      moduleResolution: ts.ModuleResolutionKind.NodeNext,
      target: ts.ScriptTarget.ES2022,
      strict: true,
    };
    const host = {
      fileExists: ts.sys.fileExists,
      readFile: ts.sys.readFile,
      realpath: ts.sys.realpath,
      directoryExists: ts.sys.directoryExists,
      getCurrentDirectory: ts.sys.getCurrentDirectory,
    };
    for (const relative of declarationFiles) {
      const file = path.join(root, relative);
      const source = ts.createSourceFile(file, await fs.readFile(file, 'utf8'), ts.ScriptTarget.Latest, true);
      for (const reference of source.referencedFiles ?? []) {
        const referenced = path.resolve(path.dirname(file), reference.fileName);
        if (
          !referenced.startsWith(root + path.sep) ||
          !/\.d\.(?:ts|mts|cts)$/.test(referenced) ||
          !present.has(path.relative(root, referenced).replaceAll(path.sep, '/'))
        )
          errors.push(`unresolved declaration reference ${relative} -> ${reference.fileName}`);
      }
      for (const edge of declarationEdges(file, source, root)) {
        const resolved = ts.resolveModuleName(edge.specifier, file, options, host).resolvedModule;
        if (
          !resolved ||
          !resolved.resolvedFileName.startsWith(root + path.sep) ||
          !/\.d\.(?:ts|mts|cts)$/.test(resolved.resolvedFileName)
        )
          errors.push(`unresolved declaration reference ${relative} -> ${edge.specifier}`);
      }
    }
    return errors;
  } finally {
    await fs.rm(temp, { recursive: true, force: true });
  }
}

async function install(archive, dir) {
  await fs.mkdir(path.join(dir, 'node_modules'), { recursive: true });
  await fs.writeFile(
    path.join(dir, 'package.json'),
    JSON.stringify({ name: 'packed-consumer', private: true, type: 'module' }),
  );
  const result = command(
    [
      npm,
      'install',
      '--ignore-scripts',
      '--offline',
      '--no-audit',
      '--no-fund',
      '--no-package-lock',
      '--prefix',
      dir,
      archive,
    ],
    process.cwd(),
  );
  if (!result.ok) throw new Error(`npm install failed:\n${result.output}`);
}

async function checkConsumer(archive, kind = 'good') {
  const temp = await fs.mkdtemp(path.join(os.tmpdir(), 'mastra-consumer-'));
  try {
    await install(archive, temp);
    if (kind === 'missing-export') return;
    const importPath = 'packed-fixture';
    await fs.writeFile(
      path.join(temp, 'consumer.mts'),
      `import { answer } from "${importPath}"; const esm: number = answer; console.log(esm);\n`,
    );
    await fs.writeFile(
      path.join(temp, 'consumer.cts'),
      'import fixture = require("packed-fixture"); const cjs: number = fixture.answer; console.log(cjs);\n',
    );
    await fs.writeFile(
      path.join(temp, 'tsconfig.json'),
      JSON.stringify({
        compilerOptions: {
          strict: true,
          skipLibCheck: false,
          noEmit: true,
          module: 'NodeNext',
          moduleResolution: 'NodeNext',
          target: 'ES2022',
          types: [],
        },
        files: ['consumer.mts', 'consumer.cts'],
      }),
    );
    const config = ts.readConfigFile(path.join(temp, 'tsconfig.json'), ts.sys.readFile).config;
    const parsed = ts.parseJsonConfigFileContent(config, ts.sys, temp);
    const diagnostics = ts.getPreEmitDiagnostics(ts.createProgram(parsed.fileNames, parsed.options));
    const text = diagnostics.map(d => ts.flattenDiagnosticMessageText(d.messageText, '\n')).join('\n');
    const expectFailure = kind !== 'good';
    if (expectFailure ? !diagnostics.length : diagnostics.length)
      throw new Error(
        expectFailure ? `${kind}: broken archive unexpectedly typechecked` : `consumer typecheck failed:\n${text}`,
      );
    if (expectFailure) {
      const expectedCode = kind === 'precedence' ? 2322 : 2307;
      if (!diagnostics.some(diagnostic => diagnostic.code === expectedCode))
        throw new Error(`${kind}: missing expected TS${expectedCode} diagnostic:\n${text}`);
      return;
    }
    const imported = command(
      [
        process.execPath,
        '--input-type=module',
        '-e',
        `import { answer } from '${importPath}'; if (answer !== 42) process.exit(1)`,
      ],
      temp,
    );
    if (!imported.ok) throw new Error(`runtime import failed:\n${imported.output}`);
    const required = command(
      [process.execPath, '-e', "const { answer } = require('packed-fixture'); if (answer !== 42) process.exit(1)"],
      temp,
    );
    if (!required.ok) throw new Error(`runtime require failed:\n${required.output}`);
  } finally {
    await fs.rm(temp, { recursive: true, force: true });
  }
}

async function fixture(dir, kind) {
  const pkg = {
    name: 'packed-fixture',
    version: '1.0.0',
    type: 'module',
    files: ['dist'],
    exports: {
      '.': {
        import: { types: './dist/index.d.mts', default: './dist/index.mjs' },
        require: { types: './dist/index.d.cts', default: './dist/index.cjs' },
      },
    },
  };
  await fs.mkdir(path.join(dir, 'dist'), { recursive: true });
  await fs.writeFile(path.join(dir, 'package.json'), JSON.stringify(pkg, null, 2));
  await fs.writeFile(path.join(dir, 'dist/index.mjs'), 'export const answer = 42;\n');
  await fs.writeFile(path.join(dir, 'dist/index.cjs'), 'exports.answer = 42;\n');
  await fs.writeFile(
    path.join(dir, 'dist/index.d.mts'),
    kind === 'missing-export'
      ? 'export declare const answer: number;\n'
      : kind === 'broken-reference'
        ? 'export { answer } from "./missing.mjs";\n'
        : 'declare const answer: number; export { answer };\n',
  );
  await fs.writeFile(
    path.join(dir, 'dist/index.d.cts'),
    kind === 'cjs-graph'
      ? 'import type { Missing } from "./internal.js"; declare const value: Missing; export = value;\n'
      : 'declare const value: { answer: number }; export = value;\n',
  );
  if (kind === 'missing-export') pkg.exports['.'].import.default = './dist/missing.mjs';
  if (kind === 'precedence') {
    await fs.writeFile(path.join(dir, 'dist/index.d.mts'), 'export { answer } from "./feature.js";\n');
    await fs.writeFile(path.join(dir, 'dist/feature.d.ts'), 'export declare const answer: string;\n');
    await fs.mkdir(path.join(dir, 'dist/feature'));
    await fs.writeFile(path.join(dir, 'dist/feature/index.d.ts'), 'export declare const answer: number;\n');
  }
  await fs.writeFile(path.join(dir, 'package.json'), JSON.stringify(pkg, null, 2));
}

async function main() {
  const packageIndex = process.argv.indexOf('--package');
  if (packageIndex !== -1) {
    const packageDir = path.resolve(process.argv[packageIndex + 1]);
    const temp = await fs.mkdtemp(path.join(os.tmpdir(), 'mastra-pack-'));
    try {
      const archive = await pack(packageDir, temp);
      const errors = await validatePackedArchive(archive);
      if (errors.length) throw new Error(errors.join('\n'));
      console.log(`✅ ${archive}`);
    } finally {
      await fs.rm(temp, { recursive: true, force: true });
    }
    return;
  }
  const temp = await fs.mkdtemp(path.join(os.tmpdir(), 'mastra-fixtures-'));
  try {
    for (const kind of ['good', 'missing-export', 'broken-reference', 'precedence', 'cjs-graph']) {
      const dir = path.join(temp, kind);
      await fixture(dir, kind);
      const archive = await pack(dir, temp);
      const errors = await validatePackedArchive(archive);
      if (kind === 'good' && errors.length) throw new Error(`${kind}: ${errors.join('; ')}`);
      const expectedError = {
        'missing-export': 'missing export target ./dist/missing.mjs',
        'broken-reference': 'unresolved declaration reference dist/index.d.mts -> ./missing.mjs',
        'cjs-graph': 'unresolved declaration reference dist/index.d.cts -> ./internal.js',
      }[kind];
      if (expectedError && !errors.includes(expectedError))
        throw new Error(`${kind}: archive validator did not report ${expectedError}: ${errors.join('; ')}`);
      await checkConsumer(archive, kind);
      console.log(`✅ ${kind}`);
    }
  } finally {
    await fs.rm(temp, { recursive: true, force: true });
  }
}

main().catch(error => {
  console.error(`❌ ${error.message}`);
  process.exitCode = 1;
});
