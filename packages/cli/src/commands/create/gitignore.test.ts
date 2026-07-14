import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';
import { MASTRA_GITIGNORE_ENTRIES, ensureGitignoreEntries } from './utils.js';

const fixtures: string[] = [];

afterEach(async () => {
  await Promise.all(fixtures.splice(0).map(fixture => rm(fixture, { recursive: true, force: true })));
});

async function gitignoreFixture(): Promise<string> {
  const fixture = await mkdtemp(path.join(tmpdir(), 'mastra-create-gitignore-'));
  fixtures.push(fixture);
  return path.join(fixture, '.gitignore');
}

// Entries `bun init -y` generates before the Mastra step runs.
const BUN_GITIGNORE = `# dependencies (bun install)
node_modules

# output
out
dist
*.tgz

# code coverage
coverage
*.lcov

# logs
logs
_.log
report.[0-9]_.[0-9]_.[0-9]_.[0-9]_.json

# dotenv environment variable files
.env.local
.env.development.local
.env.test.local
.env.production.local

# caches
.eslintcache
.cache
*.tsbuildinfo

# IntelliJ based IDEs
.idea

# Finder (MacOS) folder config
.DS_Store
`;

describe('ensureGitignoreEntries', () => {
  it('creates the file with the Mastra entries when none exists', async () => {
    const gitignorePath = await gitignoreFixture();

    await ensureGitignoreEntries(gitignorePath);

    await expect(readFile(gitignorePath, 'utf-8')).resolves.toBe(`${MASTRA_GITIGNORE_ENTRIES.join('\n')}\n`);
  });

  it('preserves Bun-generated entries and appends only the missing Mastra entries', async () => {
    const gitignorePath = await gitignoreFixture();
    await writeFile(gitignorePath, BUN_GITIGNORE);

    await ensureGitignoreEntries(gitignorePath);

    const contents = await readFile(gitignorePath, 'utf-8');
    expect(contents.startsWith(BUN_GITIGNORE)).toBe(true);
    for (const bunEntry of ['out', 'coverage', '.env.local', '.eslintcache', '.idea', '.DS_Store']) {
      expect(contents).toContain(`\n${bunEntry}\n`);
    }
    for (const mastraEntry of MASTRA_GITIGNORE_ENTRIES) {
      expect(contents.split(/\r?\n/)).toContain(mastraEntry);
    }
    // `node_modules` and `dist` already exist in the Bun file and must not be duplicated.
    expect(contents.split(/\r?\n/).filter(line => line.trim() === 'node_modules')).toHaveLength(1);
    expect(contents.split(/\r?\n/).filter(line => line.trim() === 'dist')).toHaveLength(1);
  });

  it('is idempotent across repeated runs', async () => {
    const gitignorePath = await gitignoreFixture();

    await ensureGitignoreEntries(gitignorePath);
    const firstPass = await readFile(gitignorePath, 'utf-8');
    await ensureGitignoreEntries(gitignorePath);

    await expect(readFile(gitignorePath, 'utf-8')).resolves.toBe(firstPass);
  });

  it('appends after a final line that has no trailing newline', async () => {
    const gitignorePath = await gitignoreFixture();
    await writeFile(gitignorePath, 'coverage');

    await ensureGitignoreEntries(gitignorePath);

    const lines = (await readFile(gitignorePath, 'utf-8')).split('\n');
    expect(lines[0]).toBe('coverage');
    expect(lines[1]).toBe(MASTRA_GITIGNORE_ENTRIES[0]);
  });
});
