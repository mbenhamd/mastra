import * as os from 'node:os';
import * as path from 'node:path';
import { LocalFilesystem } from '@mastra/core/workspace';
import { describe, expect, it, vi } from 'vitest';

import { requestSandboxAccessTool } from '../request-sandbox-access.js';

function createMockLocalFilesystem() {
  const tmpDir = os.tmpdir();
  const fs = new LocalFilesystem({ basePath: path.join(tmpDir, 'test-sandbox-access'), contained: true });
  const spy = vi.spyOn(fs, 'setAllowedPaths');
  return { fs, setAllowedPaths: spy };
}

function createHarnessCtx() {
  return {
    session: {
      state: {
        get: () => ({ sandboxAllowedPaths: [] }),
        set: vi.fn(),
      },
    },
  };
}

/**
 * The tool pauses via the native tool-suspension primitive. The first pass calls
 * `suspend({ kind, path, reason })` and returns; the host resumes the tool with the
 * user's answer as `resumeData`. These helpers exercise both passes.
 */
function suspendPass(harnessCtx: any, fs?: any) {
  const suspend = vi.fn();
  const context = {
    agent: { suspend },
    requestContext: { get: (key: string) => (key === 'harness' ? harnessCtx : undefined) },
    workspace: fs ? { filesystem: fs } : {},
  };
  return { context, suspend };
}

function resumePass(answer: string, harnessCtx: any, fs?: any) {
  const context = {
    agent: { resumeData: answer },
    requestContext: { get: (key: string) => (key === 'harness' ? harnessCtx : undefined) },
    workspace: fs ? { filesystem: fs } : {},
  };
  return { context };
}

describe('request_access', () => {
  it('suspends with the request payload on the first pass', async () => {
    const harnessCtx = createHarnessCtx();
    const { context, suspend } = suspendPass(harnessCtx);

    const result = await (requestSandboxAccessTool as any).execute(
      { path: '/outside/project/dir', reason: 'need to read config' },
      context,
    );

    // Suspending returns no value; the suspend payload carries the request details.
    expect(result).toBeUndefined();
    expect(suspend).toHaveBeenCalledTimes(1);
    expect(suspend.mock.calls[0]![0]).toEqual({
      kind: 'sandbox_access_request',
      path: '/outside/project/dir',
      reason: 'need to read config',
    });
  });

  it('calls setAllowedPaths on workspace filesystem when access is approved', async () => {
    const { fs, setAllowedPaths } = createMockLocalFilesystem();
    const harnessCtx = createHarnessCtx();
    const { context } = resumePass('yes', harnessCtx, fs);

    const result = await (requestSandboxAccessTool as any).execute(
      { path: '/outside/project/dir', reason: 'need to read config' },
      context,
    );

    expect(result.isError).toBe(false);
    expect(result.content).toContain('Access granted');

    // The grant must be persisted to harness state so the workspace factory
    // re-derives the same allowlist on the next tool call (otherwise the
    // factory's setAllowedPaths rebuild clobbers the in-turn widen below).
    expect(harnessCtx.session.state.set).toHaveBeenCalledTimes(1);
    expect(harnessCtx.session.state.set).toHaveBeenCalledWith({
      sandboxAllowedPaths: ['/outside/project/dir'],
    });

    // The key assertion: setAllowedPaths must be called mid-turn
    expect(setAllowedPaths).toHaveBeenCalledTimes(1);
    const arg = setAllowedPaths.mock.calls[0]![0];
    expect(typeof arg).toBe('function');
    // The updater should append the new path
    const updater = arg as (current: readonly string[]) => string[];
    expect(updater([])).toEqual(['/outside/project/dir']);
    expect(updater(['/existing'])).toEqual(['/existing', '/outside/project/dir']);
  });

  it('widens the live filesystem via the harness context when tool context has no workspace', async () => {
    // Regression: in the real runtime the tool-execution context does NOT
    // expose `workspace` (it is undefined). The live, resolved workspace is
    // only reachable through the harness request context. Granting access must
    // widen that filesystem so same-turn `view` calls can read the path.
    const { fs, setAllowedPaths } = createMockLocalFilesystem();
    const harnessCtx: any = {
      session: {
        state: {
          get: () => ({ sandboxAllowedPaths: [] }),
          set: vi.fn(),
        },
      },
      workspace: { filesystem: fs },
    };
    // Tool context intentionally has NO workspace, matching production.
    const context = {
      agent: { resumeData: 'yes' },
      requestContext: { get: (key: string) => (key === 'harness' ? harnessCtx : undefined) },
    };

    const result = await (requestSandboxAccessTool as any).execute(
      { path: '/outside/project/dir', reason: 'need to read config' },
      context,
    );

    expect(result.isError).toBe(false);
    expect(result.content).toContain('Access granted');
    // The live filesystem from the harness context must be widened.
    expect(setAllowedPaths).toHaveBeenCalledTimes(1);
    const updater = setAllowedPaths.mock.calls[0]![0] as (c: readonly string[]) => string[];
    expect(updater(['/existing'])).toEqual(['/existing', '/outside/project/dir']);
  });

  it('does not call setAllowedPaths when access is denied', async () => {
    const { fs, setAllowedPaths } = createMockLocalFilesystem();
    const harnessCtx = createHarnessCtx();
    const { context } = resumePass('no', harnessCtx, fs);

    const result = await (requestSandboxAccessTool as any).execute(
      { path: '/outside/project/dir', reason: 'need to read config' },
      context,
    );

    expect(result.content).toContain('Access denied');
    expect(setAllowedPaths).not.toHaveBeenCalled();
  });

  it('works when workspace has no filesystem', async () => {
    const mockHarnessCtx = {
      emitEvent: vi.fn(),
      registerQuestion: vi.fn(({ resolve }: { questionId: string; resolve: (answer: string) => void }) => {
        resolve('yes');
      }),
      getState: () => ({ sandboxAllowedPaths: [] }),
      setState: vi.fn(),
    };

    const context = {
      requestContext: {
        get: (key: string) => (key === 'harness' ? mockHarnessCtx : undefined),
      },
      workspace: { path: '/mock/project' },
    };

    const result = await (requestSandboxAccessTool as any).execute(
      { path: '/outside/project/dir', reason: 'testing' },
      context,
    );

    // Should still succeed — just won't call setAllowedPaths
    expect(result.isError).toBe(false);
    expect(result.content).toContain('Access granted');
  });

  it('expands tilde paths instead of nesting under project root', async () => {
    const { fs, setAllowedPaths } = createMockLocalFilesystem();

    const mockHarnessCtx = {
      emitEvent: vi.fn(),
      registerQuestion: vi.fn(({ resolve }: { questionId: string; resolve: (answer: string) => void }) => {
        resolve('yes');
      }),
      getState: () => ({ sandboxAllowedPaths: [] }),
      setState: vi.fn(),
    };

    const context = {
      requestContext: {
        get: (key: string) => (key === 'harness' ? mockHarnessCtx : undefined),
      },
      workspace: {
        path: '/mock/project',
        filesystem: fs,
      },
    };

    const result = await (requestSandboxAccessTool as any).execute(
      { path: '~/.config/opencode', reason: 'need config access' },
      context,
    );

    expect(result.isError).toBe(false);
    expect(result.content).toContain('Access granted');
    // Must resolve to the real home dir, not nest under project root
    const expectedPath = os.homedir() + '/.config/opencode';
    expect(result.content).toContain(expectedPath);
    expect(result.content).not.toContain('already granted');

    // setAllowedPaths should be called with the expanded path
    expect(setAllowedPaths).toHaveBeenCalledTimes(1);
    const arg = setAllowedPaths.mock.calls[0]![0];
    const updater = arg as (current: readonly string[]) => string[];
    expect(updater([])).toEqual([expectedPath]);
  });

  it('works when filesystem lacks setAllowedPaths method', async () => {
    const harnessCtx = createHarnessCtx();
    const context = {
      requestContext: {
        get: (key: string) => (key === 'harness' ? mockHarnessCtx : undefined),
      },
      workspace: {
        path: '/mock/project',
        filesystem: {}, // no setAllowedPaths
      },
    };

    const result = await (requestSandboxAccessTool as any).execute(
      { path: '/outside/project/dir', reason: 'testing' },
      context,
    );

    expect(result.isError).toBe(false);
    expect(result.content).toContain('Access granted');
  });

  it('registers a Harness v1 durable question when no legacy event emitter is present', async () => {
    const registerQuestion = vi.fn(async () => undefined);
    const suspend = vi.fn(async () => {
      throw new Error('suspended');
    });

    const context = {
      agent: {
        runId: 'run-1',
        toolCallId: 'tool-1',
        suspend,
      },
      requestContext: {
        get: (key: string) =>
          key === 'harness'
            ? {
                registerQuestion,
                getState: () => ({ sandboxAllowedPaths: [] }),
                setState: vi.fn(),
              }
            : undefined,
      },
      workspace: { path: '/mock/project' },
    };

    await expect(
      (requestSandboxAccessTool as any).execute(
        { path: '/outside/project/dir', reason: 'need to read config' },
        context,
      ),
    ).rejects.toThrow('suspended');

    expect(registerQuestion).toHaveBeenCalledWith(
      expect.objectContaining({
        questionId: expect.stringMatching(/^sandbox_/),
        question: expect.stringContaining('/outside/project/dir'),
        runId: 'run-1',
        toolCallId: 'tool-1',
        selectionMode: 'single_select',
      }),
    );
    expect(suspend).toHaveBeenCalled();
    expect(suspend.mock.calls[0]?.[0]).toEqual({});
  });

  it('registers a native Harness v1 sandbox-access request when available', async () => {
    const registerSandboxAccess = vi.fn(async () => undefined);
    const suspend = vi.fn(async () => {
      throw new Error('suspended');
    });

    const context = {
      agent: {
        runId: 'run-1',
        toolCallId: 'tool-1',
        suspend,
      },
      requestContext: {
        get: (key: string) =>
          key === 'harness'
            ? {
                registerSandboxAccess,
                registerQuestion: vi.fn(),
                getState: () => ({ sandboxAllowedPaths: [] }),
                setState: vi.fn(),
              }
            : undefined,
      },
      workspace: { path: '/mock/project' },
    };

    await expect(
      (requestSandboxAccessTool as any).execute(
        { path: '/outside/project/dir', reason: 'need to read config' },
        context,
      ),
    ).rejects.toThrow('suspended');

    expect(registerSandboxAccess).toHaveBeenCalledWith(
      expect.objectContaining({
        requestId: expect.stringMatching(/^sandbox_/),
        semanticType: 'file',
        reason: 'need to read config',
        payload: { path: '/outside/project/dir' },
        runId: 'run-1',
        toolCallId: 'tool-1',
      }),
    );
    expect(suspend).toHaveBeenCalled();
    expect(suspend.mock.calls[0]?.[0]).toEqual({});
  });

  it('does not require the legacy question registrar for native Harness v1 sandbox-access requests', async () => {
    const registerSandboxAccess = vi.fn(async () => undefined);
    const suspend = vi.fn(async () => {
      throw new Error('suspended');
    });

    const context = {
      agent: {
        runId: 'run-1',
        toolCallId: 'tool-1',
        suspend,
      },
      requestContext: {
        get: (key: string) =>
          key === 'harness'
            ? {
                registerSandboxAccess,
                getState: () => ({ sandboxAllowedPaths: [] }),
                setState: vi.fn(),
              }
            : undefined,
      },
      workspace: { path: '/mock/project' },
    };

    await expect(
      (requestSandboxAccessTool as any).execute(
        { path: '/outside/project/dir', reason: 'need to read config' },
        context,
      ),
    ).rejects.toThrow('suspended');

    expect(registerSandboxAccess).toHaveBeenCalledWith(
      expect.objectContaining({
        requestId: expect.stringMatching(/^sandbox_/),
        semanticType: 'file',
        reason: 'need to read config',
        payload: { path: '/outside/project/dir' },
        runId: 'run-1',
        toolCallId: 'tool-1',
      }),
    );
  });

  it('uses the legacy prompt path when a legacy event emitter is present even if agent.suspend exists', async () => {
    const registerQuestion = vi.fn(({ resolve }: { questionId: string; resolve: (answer: string) => void }) => {
      resolve('yes');
    });
    const emitEvent = vi.fn();
    const suspend = vi.fn(async () => {
      throw new Error('should not suspend through legacy harness context');
    });
    const setState = vi.fn();

    const context = {
      agent: {
        runId: 'run-1',
        toolCallId: 'tool-1',
        suspend,
      },
      requestContext: {
        get: (key: string) =>
          key === 'harness'
            ? {
                emitEvent,
                registerQuestion,
                getState: () => ({ sandboxAllowedPaths: [] }),
                setState,
              }
            : undefined,
      },
      workspace: { path: '/mock/project' },
    };

    const result = await (requestSandboxAccessTool as any).execute(
      { path: '/outside/project/dir', reason: 'need to read config' },
      context,
    );

    expect(result.isError).toBe(false);
    expect(result.content).toContain('Access granted');
    expect(suspend).not.toHaveBeenCalled();
    expect(registerQuestion).toHaveBeenCalledWith(
      expect.objectContaining({
        questionId: expect.stringMatching(/^sandbox_/),
        resolve: expect.any(Function),
      }),
    );
    expect(emitEvent).toHaveBeenCalledWith(
      expect.objectContaining({
        type: 'sandbox_access_request',
        path: '/outside/project/dir',
      }),
    );
    expect(setState).toHaveBeenCalledWith({ sandboxAllowedPaths: ['/outside/project/dir'] });
  });

  it('resumes a Harness v1 sandbox question from agent resumeData', async () => {
    const { fs, setAllowedPaths } = createMockLocalFilesystem();
    const setState = vi.fn();

    const context = {
      agent: {
        resumeData: { answer: 'yes' },
      },
      requestContext: {
        get: (key: string) =>
          key === 'harness'
            ? {
                registerQuestion: vi.fn(),
                getState: () => ({ sandboxAllowedPaths: [] }),
                setState,
              }
            : undefined,
      },
      workspace: {
        path: '/mock/project',
        filesystem: fs,
      },
    };

    const result = await (requestSandboxAccessTool as any).execute(
      { path: '/outside/project/dir', reason: 'need to read config' },
      context,
    );

    expect(result.isError).toBe(false);
    expect(result.content).toContain('Access granted');
    expect(setState).toHaveBeenCalledWith({ sandboxAllowedPaths: ['/outside/project/dir'] });
    expect(setAllowedPaths).toHaveBeenCalledTimes(1);
  });

  it('resumes a native Harness v1 sandbox-access approval from agent resumeData', async () => {
    const { fs, setAllowedPaths } = createMockLocalFilesystem();
    const setState = vi.fn();

    const context = {
      agent: {
        resumeData: { approved: true, reason: 'ok' },
      },
      requestContext: {
        get: (key: string) =>
          key === 'harness'
            ? {
                registerSandboxAccess: vi.fn(),
                getState: () => ({ sandboxAllowedPaths: [] }),
                setState,
              }
            : undefined,
      },
      workspace: {
        path: '/mock/project',
        filesystem: fs,
      },
    };

    const result = await (requestSandboxAccessTool as any).execute(
      { path: '/outside/project/dir', reason: 'need to read config' },
      context,
    );

    expect(result.isError).toBe(false);
    expect(result.content).toContain('Access granted');
    expect(setState).toHaveBeenCalledWith({ sandboxAllowedPaths: ['/outside/project/dir'] });
    expect(setAllowedPaths).toHaveBeenCalledTimes(1);
  });

  it('does not require a registration surface when native sandbox-access resumeData is already present', async () => {
    const { fs, setAllowedPaths } = createMockLocalFilesystem();

    const context = {
      agent: {
        resumeData: { approved: true },
      },
      requestContext: {
        get: () => undefined,
      },
      workspace: {
        filesystem: fs,
      },
    };

    const result = await (requestSandboxAccessTool as any).execute(
      { path: '/outside/project/dir', reason: 'need to read config' },
      context,
    );

    expect(result.isError).toBe(false);
    expect(result.content).toContain('Access granted');
    expect(setAllowedPaths).toHaveBeenCalledTimes(1);
  });
});
