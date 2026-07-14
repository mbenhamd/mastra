import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { createChildProcessLogger } from './log.js';

const fixtures: string[] = [];

afterEach(async () => {
  vi.restoreAllMocks();
  await Promise.all(fixtures.splice(0).map(fixture => rm(fixture, { recursive: true, force: true })));
});

describe('createChildProcessLogger integration', () => {
  it('passes shell-control-looking text unchanged to a real child process', async () => {
    vi.spyOn(console, 'info').mockImplementation(() => {});
    const logger = {
      info: vi.fn(),
      error: vi.fn(),
    };
    const stdoutValue = 'stdout; $(not-a-command) & | ^ %PATH%';
    const stderrValue = 'stderr; $(still-not-a-command) & | ^ %PATH%';
    const run = createChildProcessLogger({ logger: logger as never, root: process.cwd() });

    await expect(
      run({
        cmd: process.execPath,
        args: [
          '-e',
          'process.stdout.write(process.argv[1] ?? ""); process.stderr.write(process.argv[2] ?? "")',
          stdoutValue,
          stderrValue,
        ],
        env: { PATH: process.env.PATH! },
      }),
    ).resolves.toEqual({ success: true });

    expect(logger.info).toHaveBeenCalledWith(stdoutValue);
    expect(logger.info).toHaveBeenCalledWith(stderrValue);
    expect(logger.error).not.toHaveBeenCalled();
  });

  it('does not log credential-bearing argv when a real child fails', async () => {
    vi.spyOn(console, 'info').mockImplementation(() => {});
    const logger = {
      info: vi.fn(),
      error: vi.fn(),
    };
    const credential = 'https://user:DUMMY_SECRET_TOKEN_123@example.invalid/pkg';
    const run = createChildProcessLogger({ logger: logger as never, root: process.cwd() });

    const failure = await run({
      cmd: process.execPath,
      args: ['-e', 'process.exit(7)', credential],
      env: { PATH: process.env.PATH! },
    }).catch(error => error);

    expect(failure).toMatchObject({
      name: 'ChildProcessError',
      message: 'Package manager process failed',
      exitCode: 7,
    });
    expect(JSON.stringify(failure, Object.getOwnPropertyNames(failure))).not.toContain(credential);
    expect(JSON.stringify(failure, Object.getOwnPropertyNames(failure))).not.toContain('DUMMY_SECRET_TOKEN_123');

    const serializedLog = JSON.stringify(logger.error.mock.calls);
    expect(serializedLog).not.toContain(credential);
    expect(serializedLog).not.toContain('DUMMY_SECRET_TOKEN_123');
    expect(logger.error).toHaveBeenCalledWith('Process failed', {
      error: {
        name: 'ExecaError',
        exitCode: 7,
        signal: undefined,
        timedOut: false,
        isCanceled: false,
      },
    });
  });

  it('terminates descendants that keep output pipes open after the package manager times out', async () => {
    vi.spyOn(console, 'info').mockImplementation(() => {});
    const logger = {
      info: vi.fn(),
      error: vi.fn(),
    };
    const fixture = await mkdtemp(join(tmpdir(), 'mastra-deployer-process-tree-'));
    fixtures.push(fixture);
    const childPidPath = join(fixture, 'child.pid');
    const run = createChildProcessLogger({ logger: logger as never, root: process.cwd(), timeout: 500 });
    let childPid: number | undefined;

    try {
      await expect(
        run({
          cmd: process.execPath,
          args: [
            '-e',
            `const { spawn } = require('node:child_process');
const { writeFileSync } = require('node:fs');
const child = spawn(process.execPath, ['-e', "process.on('SIGTERM', () => {}); setInterval(() => {}, 1000)"], { stdio: 'inherit' });
writeFileSync(process.argv[1], String(child.pid));
process.on('SIGTERM', () => {});
setInterval(() => {}, 1000);`,
            childPidPath,
          ],
          env: process.env as Record<string, string>,
        }),
      ).rejects.toMatchObject({ timedOut: true });

      childPid = Number(await readFile(childPidPath, 'utf8'));
      expect(() => process.kill(childPid!, 0)).toThrow();
    } finally {
      const pid = childPid ?? Number(await readFile(childPidPath, 'utf8').catch(() => ''));
      if (Number.isSafeInteger(pid) && pid > 0) {
        try {
          process.kill(pid, 'SIGKILL');
        } catch {
          // The process-tree timeout already terminated the child.
        }
      }
    }
  });

  it('preserves force-kill escalation after the package manager leader exits', async () => {
    vi.spyOn(console, 'info').mockImplementation(() => {});
    const logger = {
      info: vi.fn(),
      error: vi.fn(),
    };
    const fixture = await mkdtemp(join(tmpdir(), 'mastra-deployer-process-escalation-'));
    fixtures.push(fixture);
    const childPidPath = join(fixture, 'child.pid');
    const heartbeatPath = join(fixture, 'heartbeat');
    const run = createChildProcessLogger({ logger: logger as never, root: process.cwd(), timeout: 500 });
    let childPid: number | undefined;

    try {
      await expect(
        run({
          cmd: process.execPath,
          args: [
            '-e',
            `const { spawn } = require('node:child_process');
const { writeFileSync } = require('node:fs');
const child = spawn(process.execPath, ['-e', "const { writeFileSync } = require('node:fs'); const path = process.argv[1]; process.on('SIGTERM', () => {}); setInterval(() => writeFileSync(path, String(Date.now())), 20)", process.argv[2]], { stdio: 'ignore' });
writeFileSync(process.argv[1], String(child.pid));
setInterval(() => {}, 1000);`,
            childPidPath,
            heartbeatPath,
          ],
          env: process.env as Record<string, string>,
        }),
      ).rejects.toMatchObject({ timedOut: true });

      childPid = Number(await readFile(childPidPath, 'utf8'));
      const heartbeatAfterEscalation = await readFile(heartbeatPath, 'utf8');
      await new Promise(resolve => setTimeout(resolve, 150));
      expect(await readFile(heartbeatPath, 'utf8')).toBe(heartbeatAfterEscalation);
    } finally {
      const pid = childPid ?? Number(await readFile(childPidPath, 'utf8').catch(() => ''));
      if (Number.isSafeInteger(pid) && pid > 0) {
        try {
          process.kill(pid, 'SIGKILL');
        } catch {
          // The process-tree timeout already terminated the child.
        }
      }
    }
  });

  it('reports a timeout when the child handles SIGTERM and exits with code zero', async () => {
    vi.spyOn(console, 'info').mockImplementation(() => {});
    const logger = {
      info: vi.fn(),
      error: vi.fn(),
    };
    const run = createChildProcessLogger({
      logger: logger as never,
      root: process.cwd(),
      timeout: 500,
      output: 'ignore',
    });

    await expect(
      run({
        cmd: process.execPath,
        args: ['-e', "process.on('SIGTERM', () => process.exit(0)); setInterval(() => {}, 1000)"],
        env: process.env as Record<string, string>,
      }),
    ).rejects.toMatchObject({
      name: 'ChildProcessError',
      timedOut: true,
    });

    expect(logger.error).toHaveBeenCalledOnce();
  });
});
