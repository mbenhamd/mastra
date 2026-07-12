import child_process from 'node:child_process';
import { createRequire } from 'node:module';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import util from 'node:util';
import debug from 'debug';

const execFile = util.promisify(child_process.execFile);

interface TransformOptions {
  dry?: boolean;
  print?: boolean;
  verbose?: boolean;
  jscodeshift?: string;
}

const log = debug('codemod:transform');
const error = debug('codemod:transform:error');
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

function getJscodeshiftBin(): string {
  // jscodeshift is a direct dependency, so run its JavaScript entrypoint with
  // the current Node executable instead of relying on a shell or .bin shim.
  const require = createRequire(import.meta.url);
  return require.resolve('jscodeshift/bin/jscodeshift.js');
}

/**
 * Split the CLI's single custom-options value without applying shell syntax.
 * Quotes group whitespace; backslashes remain path separators unless they
 * explicitly escape whitespace or a quote. Shell-looking characters are data.
 */
function parseJscodeshiftArgs(value: string): string[] {
  const args: string[] = [];
  let current = '';
  let quote: "'" | '"' | undefined;
  let tokenStarted = false;

  for (let index = 0; index < value.length; index++) {
    const character = value[index]!;

    if (quote) {
      if (character === quote) {
        quote = undefined;
      } else if (character === '\\' && quote === '"') {
        let backslashCount = 1;
        while (value[index + backslashCount] === '\\') {
          backslashCount++;
        }

        if (value[index + backslashCount] === '"') {
          current += '\\'.repeat(Math.floor(backslashCount / 2));
          if (backslashCount % 2 === 0) {
            quote = undefined;
          } else {
            current += '"';
          }
          index += backslashCount;
        } else {
          current += '\\'.repeat(backslashCount);
          index += backslashCount - 1;
        }
      } else {
        current += character;
      }
      continue;
    }

    if (/\s/.test(character)) {
      if (tokenStarted) {
        args.push(current);
        current = '';
        tokenStarted = false;
      }
      continue;
    }

    if (character === "'" || character === '"') {
      quote = character;
      tokenStarted = true;
      continue;
    }

    if (character === '\\') {
      const next = value[index + 1];
      if (next && (/\s/.test(next) || next === "'" || next === '"')) {
        current += next;
        index++;
      } else {
        // Preserve ordinary and trailing backslashes for Windows paths.
        current += character;
      }
      tokenStarted = true;
      continue;
    }

    current += character;
    tokenStarted = true;
  }

  if (quote) {
    throw new Error('Custom jscodeshift options contain an unterminated quote');
  }
  if (tokenStarted) {
    args.push(current);
  }

  return args;
}

function buildArgs(codemodPath: string, targetPath: string, options: TransformOptions): string[] {
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
    args.push('--verbose');
  }

  if (options.jscodeshift) {
    args.push(...parseJscodeshiftArgs(options.jscodeshift));
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
  const { stdout } = await execFile(process.execPath, [getJscodeshiftBin(), ...args], { encoding: 'utf8' });
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
