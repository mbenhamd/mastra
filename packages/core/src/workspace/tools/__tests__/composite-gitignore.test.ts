import * as fs from 'node:fs/promises';
import * as os from 'node:os';
import * as path from 'node:path';
import { describe, it, expect, beforeEach, afterEach } from 'vitest';

import { WORKSPACE_TOOLS } from '../../constants';
import { CompositeFilesystem, LocalFilesystem } from '../../filesystem';
import { Workspace } from '../../workspace';
import { createWorkspaceTools } from '../tools';

// A composite has no filesystem at its root, so its root `.gitignore` does not
// exist. list_files and grep load that file before walking; the missing mount
// must read as an absent `.gitignore`, not as a failed read.
describe('workspace tools on a composite filesystem', () => {
  let scratchDir: string;
  let projectDir: string;

  beforeEach(async () => {
    scratchDir = await fs.mkdtemp(path.join(os.tmpdir(), 'composite-gitignore-scratch-'));
    projectDir = await fs.mkdtemp(path.join(os.tmpdir(), 'composite-gitignore-project-'));
  });

  afterEach(async () => {
    await fs.rm(scratchDir, { recursive: true, force: true }).catch(() => {});
    await fs.rm(projectDir, { recursive: true, force: true }).catch(() => {});
  });

  it('lists and greps a mount when the composite root has no .gitignore', async () => {
    await fs.mkdir(path.join(scratchDir, 'notes'));
    await fs.writeFile(path.join(scratchDir, 'notes', 'todo.txt'), 'composite-probe line\n');
    await fs.writeFile(path.join(projectDir, 'other.txt'), 'composite-probe elsewhere\n');
    const workspace = new Workspace({
      filesystem: new CompositeFilesystem({
        mounts: {
          '/scratch': new LocalFilesystem({ basePath: scratchDir }),
          '/project': new LocalFilesystem({ basePath: projectDir }),
        },
      }),
    });
    await workspace.init();
    try {
      const tools = await createWorkspaceTools(workspace);

      const listing = await tools[WORKSPACE_TOOLS.FILESYSTEM.LIST_FILES].execute(
        { path: '/scratch/notes' },
        { workspace },
      );
      expect(listing).toContain('todo.txt');

      const matches = await tools[WORKSPACE_TOOLS.FILESYSTEM.GREP].execute(
        { pattern: 'composite-probe', path: '/scratch' },
        { workspace },
      );
      expect(matches).toContain('todo.txt');
      expect(matches).not.toContain('other.txt');
    } finally {
      await workspace.destroy();
    }
  });
});
