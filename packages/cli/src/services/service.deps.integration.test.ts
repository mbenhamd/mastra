import { chmod, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';
import { DepsService } from './service.deps.js';

const fixtures: string[] = [];

afterEach(async () => {
  await Promise.all(fixtures.splice(0).map(fixture => rm(fixture, { recursive: true, force: true })));
});

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
const fs = require('node:fs');
fs.writeFileSync(process.argv.at(-1), String(process.pid));
process.on('SIGTERM', () => {});
setInterval(() => {}, 1_000);
`,
      );
      await chmod(executable, 0o755);
      const previousPath = process.env.PATH;
      process.env.PATH = `${fixture}${path.delimiter}${previousPath ?? ''}`;

      try {
        await expect(new DepsService('npm').installPackages([pidFile], { timeout: 500 })).rejects.toMatchObject({
          timedOut: true,
        });
        const childPid = Number(await readFile(pidFile, 'utf8'));
        expect(() => process.kill(childPid, 0)).toThrow();
      } finally {
        process.env.PATH = previousPath;
      }
    },
  );
});
