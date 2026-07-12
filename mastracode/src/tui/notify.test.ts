import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

const mocks = vi.hoisted(() => ({
  exec: vi.fn(),
  execFile: vi.fn(),
}));

vi.mock('node:child_process', () => ({
  exec: mocks.exec,
  execFile: mocks.execFile,
}));

import { sendNotification } from './notify.js';

describe('sendNotification', () => {
  beforeEach(() => {
    mocks.exec.mockReset();
    mocks.execFile.mockReset();
    vi.spyOn(process, 'platform', 'get').mockReturnValue('darwin');
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('passes escaped notification text to osascript without a shell', () => {
    const message = 'Backslash \\ and quote " and shell \'; do evil';

    sendNotification('agent_done', { mode: 'system', message });

    expect(mocks.execFile).toHaveBeenCalledTimes(1);
    expect(mocks.exec).not.toHaveBeenCalled();
    const [command, args, callback] = mocks.execFile.mock.calls[0]!;
    expect(command).toBe('osascript');
    expect(args).toEqual([
      '-e',
      String.raw`display notification "Backslash \\ and quote \" and shell '; do evil" with title "Mastra Code"`,
    ]);
    expect(() => callback(new Error('osascript unavailable'))).not.toThrow();
  });

  it('replaces NUL bytes and ignores synchronous notifier failures', () => {
    mocks.execFile.mockImplementationOnce(() => {
      throw new Error('invalid argument');
    });

    expect(() => sendNotification('agent_done', { mode: 'system', message: 'Before\0after' })).not.toThrow();

    expect(mocks.execFile).toHaveBeenCalledWith(
      'osascript',
      ['-e', 'display notification "Before�after" with title "Mastra Code"'],
      expect.any(Function),
    );
    expect(mocks.exec).not.toHaveBeenCalled();
  });

  it('does not invoke a native notifier on unsupported platforms', () => {
    vi.spyOn(process, 'platform', 'get').mockReturnValue('linux');

    sendNotification('agent_done', { mode: 'system', message: 'Done' });

    expect(mocks.execFile).not.toHaveBeenCalled();
    expect(mocks.exec).not.toHaveBeenCalled();
  });
});
