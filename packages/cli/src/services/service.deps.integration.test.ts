import { spawn } from 'node:child_process';
import { chmod, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { afterEach, describe, expect, it } from 'vitest';
import { DepsService } from './service.deps.js';

const fixtures: string[] = [];

afterEach(async () => {
  await Promise.all(fixtures.splice(0).map(fixture => rm(fixture, { recursive: true, force: true })));
});

function isProcessAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

async function waitFor<T>(probe: () => Promise<T | undefined> | T | undefined, timeoutMs = 10_000): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const value = await probe();
    if (value !== undefined) return value;
    if (Date.now() > deadline) throw new Error(`Timed out after ${timeoutMs}ms waiting for condition`);
    await new Promise(resolve => setTimeout(resolve, 50));
  }
}

describe('DepsService subprocess lifecycle', () => {
  it.runIf(process.platform !== 'win32')(
    'does not reject a timed-out install until the child is terminated',
    async () => {
      const fixture = await mkdtemp(path.join(tmpdir(), 'mastra-deps-service-'));
      fixtures.push(fixture);
      const executable = path.join(fixture, 'npm');
      const pidFile = path.join(fixture, 'child.pid');
      await writeFile(
        executable,
        `#!/usr/bin/env node
const { spawn } = require('node:child_process');
const fs = require('node:fs');
const child = spawn(process.execPath, ['-e', 'process.on("SIGTERM", () => {}); setInterval(() => {}, 1000)'], {
  stdio: 'ignore',
});
fs.writeFileSync(process.argv.at(-1), String(child.pid));
process.on('SIGTERM', () => {});
setInterval(() => {}, 1_000);
`,
      );
      await chmod(executable, 0o755);
      const previousPath = process.env.PATH;
      process.env.PATH = `${fixture}${path.delimiter}${previousPath ?? ''}`;
      let childPid: number | undefined;

      try {
        await expect(new DepsService('npm').installPackages([pidFile], { timeout: 500 })).rejects.toMatchObject({
          timedOut: true,
        });
        childPid = Number(await readFile(pidFile, 'utf8'));
        expect(() => process.kill(childPid, 0)).toThrow();
      } finally {
        process.env.PATH = previousPath;
        const pid = childPid ?? Number(await readFile(pidFile, 'utf8').catch(() => ''));
        if (Number.isSafeInteger(pid) && pid > 0) {
          try {
            process.kill(pid, 'SIGKILL');
          } catch {
            // The child already exited.
          }
        }
      }
    },
  );

  it.runIf(process.platform !== 'win32')(
    'kills the detached install tree when the CLI is interrupted by SIGINT',
    async () => {
      const fixture = await mkdtemp(path.join(tmpdir(), 'mastra-deps-service-signal-'));
      fixtures.push(fixture);
      const executable = path.join(fixture, 'npm');
      const pidFile = path.join(fixture, 'install.pid');
      const runnerFile = path.join(fixture, 'runner.mts');
      // A SIGTERM-resistant stand-in for a package manager whose descendants
      // must not survive the CLI being interrupted.
      await writeFile(
        executable,
        `#!/usr/bin/env node
const fs = require('node:fs');
process.on('SIGTERM', () => {});
fs.writeFileSync(process.argv.at(-1), String(process.pid));
setInterval(() => {}, 1_000);
`,
      );
      await chmod(executable, 0o755);
      await writeFile(
        runnerFile,
        `import { DepsService } from ${JSON.stringify(new URL('./service.deps.ts', import.meta.url).href)};
await new DepsService('npm').installPackages([${JSON.stringify(pidFile)}]).catch(() => {});
`,
      );

      // Run the install in a real child process so a real SIGINT exercises the
      // signal-exit cleanup path; Node never emits `exit` for fatal signals.
      const runner = spawn(process.execPath, ['--import', 'tsx', runnerFile], {
        cwd: fileURLToPath(new URL('.', import.meta.url)),
        env: { ...process.env, PATH: `${fixture}${path.delimiter}${process.env.PATH ?? ''}` },
        stdio: 'ignore',
      });
      const runnerExit = new Promise<{ code: number | null; signal: NodeJS.Signals | null }>((resolve, reject) => {
        runner.once('error', reject);
        runner.once('exit', (code, signal) => resolve({ code, signal }));
      });

      const installPid = await waitFor(async () => {
        const contents = await readFile(pidFile, 'utf8').catch(() => '');
        const pid = Number(contents);
        return Number.isSafeInteger(pid) && pid > 0 ? pid : undefined;
      });

      try {
        expect(isProcessAlive(installPid)).toBe(true);
        expect(runner.kill('SIGINT')).toBe(true);

        const { signal } = await runnerExit;
        expect(signal).toBe('SIGINT');
        await waitFor(() => (isProcessAlive(installPid) ? undefined : true), 5_000);
        expect(isProcessAlive(installPid)).toBe(false);
      } finally {
        if (isProcessAlive(installPid)) {
          try {
            process.kill(installPid, 'SIGKILL');
          } catch {
            // The install process already exited.
          }
        }
        runner.kill('SIGKILL');
      }
    },
    30_000,
  );
});
