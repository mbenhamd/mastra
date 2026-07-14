import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it, vi } from 'vitest';

const mocks = vi.hoisted(() => ({
  createChildProcessLogger: vi.fn(),
  runProcess: vi.fn(),
}));

vi.mock('../deploy/log.js', () => ({
  createChildProcessLogger: mocks.createChildProcessLogger,
}));

import { Bundler } from './index.js';

describe('Bundler package-lock generation', () => {
  let directory: string | undefined;

  afterEach(async () => {
    vi.clearAllMocks();
    if (directory) {
      await rm(directory, { recursive: true, force: true });
      directory = undefined;
    }
  });

  it('runs npm with structured arguments and restores node_modules', async () => {
    directory = await mkdtemp(join(tmpdir(), 'mastra-deployer-lockfile-'));
    const nodeModules = join(directory, 'node_modules');
    await mkdir(nodeModules);
    await writeFile(join(nodeModules, 'marker'), 'preserved');
    mocks.runProcess.mockResolvedValue({ success: true });
    mocks.createChildProcessLogger.mockReturnValue(mocks.runProcess);
    const bundler = new (Bundler as unknown as new (name: string) => Bundler)('test');

    await (bundler as unknown as { generateNpmLockfile(outputDir: string): Promise<void> }).generateNpmLockfile(
      directory,
    );

    expect(mocks.createChildProcessLogger).toHaveBeenCalledWith({
      logger: expect.anything(),
      root: directory,
      timeout: 60_000,
      output: 'ignore',
    });
    expect(mocks.runProcess).toHaveBeenCalledWith({
      cmd: 'npm',
      args: ['install', '--package-lock-only', '--force'],
      env: process.env,
    });
    await expect(readFile(join(nodeModules, 'marker'), 'utf8')).resolves.toBe('preserved');
  });
});
