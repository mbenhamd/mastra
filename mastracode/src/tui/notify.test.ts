import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

const mocks = vi.hoisted(() => ({
  exec: vi.fn(),
  child: {
    on: vi.fn(),
    unref: vi.fn(),
    stdin: {
      on: vi.fn(),
      end: vi.fn(),
    },
  },
  spawn: vi.fn(),
}));

vi.mock('node:child_process', () => ({
  exec: mocks.exec,
  spawn: mocks.spawn,
}));

import { sendNotification } from './notify.js';

describe('sendNotification', () => {
  beforeEach(() => {
    mocks.exec.mockReset();
    mocks.child.on.mockReset();
    mocks.child.unref.mockReset();
    mocks.child.stdin.on.mockReset();
    mocks.child.stdin.end.mockReset();
    mocks.spawn.mockReset();
    mocks.spawn.mockReturnValue(mocks.child);
    vi.spyOn(process, 'platform', 'get').mockReturnValue('darwin');
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('passes notification text as data to a static AppleScript without a shell', () => {
    const message = 'Backslash \\ and quote " and shell \'; do evil\n-e unexpected';

    sendNotification('agent_done', { mode: 'system', message });

    expect(mocks.spawn).toHaveBeenCalledTimes(1);
    expect(mocks.exec).not.toHaveBeenCalled();
    expect(mocks.spawn).toHaveBeenCalledWith('/usr/bin/osascript', ['-', message, 'Mastra Code'], {
      stdio: ['pipe', 'ignore', 'ignore'],
    });
    expect(mocks.child.stdin.end).toHaveBeenCalledWith(
      'on run argv\ndisplay notification (item 1 of argv) with title (item 2 of argv)\nend run',
    );
    expect(mocks.child.on).toHaveBeenCalledWith('error', expect.any(Function));
    expect(mocks.child.stdin.on).toHaveBeenCalledWith('error', expect.any(Function));
    expect(mocks.child.unref).toHaveBeenCalledTimes(1);
  });

  it('replaces NUL bytes and ignores synchronous notifier failures', () => {
    mocks.spawn.mockImplementationOnce(() => {
      throw new Error('invalid argument');
    });

    expect(() => sendNotification('agent_done', { mode: 'system', message: 'Before\0after' })).not.toThrow();

    expect(mocks.spawn).toHaveBeenCalledWith('/usr/bin/osascript', ['-', 'Before�after', 'Mastra Code'], {
      stdio: ['pipe', 'ignore', 'ignore'],
    });
    expect(mocks.exec).not.toHaveBeenCalled();
  });

  it('does not invoke a native notifier on unsupported platforms', () => {
    vi.spyOn(process, 'platform', 'get').mockReturnValue('linux');

    sendNotification('agent_done', { mode: 'system', message: 'Done' });

    expect(mocks.spawn).not.toHaveBeenCalled();
    expect(mocks.exec).not.toHaveBeenCalled();
  });
});
