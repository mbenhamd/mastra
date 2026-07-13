import fs from 'node:fs';
import { createRequire } from 'node:module';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import debug from 'debug';
import { execa } from 'execa';

interface TransformOptions {
  dry?: boolean;
  print?: boolean;
  verbose?: boolean;
  jscodeshift?: string[];
}

const log = debug('codemod:transform');
const error = debug('codemod:transform:error');
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

export function getJscodeshiftBin(): string {
  // Resolve the direct dependency's declared executable instead of depending
  // on a private source layout or a platform-specific .bin shim.
  const require = createRequire(import.meta.url);
  const manifestPath = require.resolve('jscodeshift/package.json');
  const manifest = JSON.parse(fs.readFileSync(manifestPath, 'utf8')) as {
    bin?: string | Record<string, string>;
  };
  const declaredBin = typeof manifest.bin === 'string' ? manifest.bin : manifest.bin?.jscodeshift;
  if (!declaredBin) {
    throw new Error('The installed jscodeshift package does not declare a jscodeshift executable');
  }
  return path.resolve(path.dirname(manifestPath), declaredBin);
}

export function buildArgs(codemodPath: string, targetPath: string, options: TransformOptions): string[] {
  // Ignoring everything under `.*/` covers `.mastra/` along with any other
  // framework build related or otherwise intended-to-be-hidden directories.
  const args = [
    '-t',
    codemodPath,
    targetPath,
    '--parser',
    'tsx',
    '--ignore-pattern=**/node_modules/**',
    '--ignore-pattern=**/.*/**',
    '--ignore-pattern=**/dist/**',
    '--ignore-pattern=**/build/**',
    '--ignore-pattern=**/*.min.js',
    '--ignore-pattern=**/*.bundle.js',
  ];

  if (options.dry) {
    args.push('--dry');
  }

  if (options.print) {
    args.push('--print');
  }

  if (options.verbose) {
    args.push('--verbose=2');
  }

  if (options.jscodeshift) {
    args.push(...options.jscodeshift);
  }

  return args;
}

export type TransformErrors = {
  transform: string;
  filename: string;
  summary: string;
}[];

function parseErrors(transform: string, output: string): TransformErrors {
  const errors: TransformErrors = [];
  const errorRegex = /ERR (.+) Transformation error/g;
  const syntaxErrorRegex = /SyntaxError: .+/g;

  let match;
  while ((match = errorRegex.exec(output)) !== null) {
    const filename = match[1]!;
    const syntaxErrorMatch = syntaxErrorRegex.exec(output);
    if (syntaxErrorMatch) {
      const summary = syntaxErrorMatch[0];
      errors.push({ transform, filename, summary });
    }
  }

  return errors;
}

function parseNotImplementedErrors(transform: string, output: string): TransformErrors {
  const notImplementedErrors: TransformErrors = [];
  const notImplementedRegex = /Not Implemented (.+): (.+)/g;

  let match;
  while ((match = notImplementedRegex.exec(output)) !== null) {
    const filename = match[1]!;
    const summary = match[2]!;
    notImplementedErrors.push({ transform, filename, summary });
  }

  return notImplementedErrors;
}

export async function transform(
  codemod: string,
  source: string,
  transformOptions: TransformOptions,
  options: { logStatus: boolean } = { logStatus: true },
): Promise<{ errors: TransformErrors; notImplementedErrors: TransformErrors }> {
  if (options.logStatus) {
    log(`Applying codemod '${codemod}': ${source}`);
  }
  const codemodPath = path.resolve(__dirname, `./codemods/${codemod}.js`);
  const targetPath = path.resolve(source);
  const args = buildArgs(codemodPath, targetPath, transformOptions);
  const { stdout } = await execa(process.execPath, [getJscodeshiftBin(), ...args], {
    encoding: 'utf8',
    maxBuffer: 100 * 1024 * 1024,
  });
  const errors = parseErrors(codemod, stdout);
  const notImplementedErrors = parseNotImplementedErrors(codemod, stdout);
  if (options.logStatus) {
    if (errors.length > 0) {
      errors.forEach(({ transform, filename, summary }) => {
        error(`Error applying codemod [codemod=${transform}, path=${filename}, summary=${summary}]`);
      });
    }

    if (notImplementedErrors.length > 0) {
      log(
        `Some files require manual changes. Please search your codebase for \`FIXME(mastra): \` comments and follow the instructions to complete the upgrade.`,
      );
      notImplementedErrors.forEach(({ transform, filename, summary }) => {
        log(`Not Implemented [codemod=${transform}, path=${filename}, summary=${summary}]`);
      });
    }
  }

  return { errors, notImplementedErrors };
}
