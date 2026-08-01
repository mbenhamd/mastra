import { afterEach, describe, expect, it, vi } from 'vitest';

const spawnMock = vi.hoisted(() => vi.fn());
vi.mock('node:child_process', () => ({ spawn: spawnMock }));

import { openUrlInBrowser } from '../open-url.js';

function stubPlatform(platform: NodeJS.Platform): () => void {
  const original = Object.getOwnPropertyDescriptor(process, 'platform')!;
  Object.defineProperty(process, 'platform', { value: platform });
  return () => Object.defineProperty(process, 'platform', original);
}

function childStub() {
  return { on: vi.fn(), unref: vi.fn() };
}

describe('openUrlInBrowser (PF-2587 hardening)', () => {
  afterEach(() => {
    spawnMock.mockReset();
    vi.unstubAllEnvs();
  });

  it.each([['not a url'], ['javascript:alert(1)'], ['file:///etc/passwd'], ['ftp://example.com']])(
    'never spawns for %j',
    url => {
      openUrlInBrowser(url);
      expect(spawnMock).not.toHaveBeenCalled();
    },
  );

  it('spawns the absolute System32 rundll32 on Windows instead of a PATH-relative launcher', () => {
    const restore = stubPlatform('win32');
    vi.stubEnv('SystemRoot', String.raw`D:\CustomWindows`);
    spawnMock.mockReturnValue(childStub());
    try {
      openUrlInBrowser('https://example.com/login');
    } finally {
      restore();
    }
    expect(spawnMock).toHaveBeenCalledTimes(1);
    const [cmd, args, opts] = spawnMock.mock.calls[0]!;
    expect(cmd).toBe(String.raw`D:\CustomWindows\System32\rundll32.exe`);
    expect(args).toEqual(['url.dll,FileProtocolHandler', 'https://example.com/login']);
    expect(opts).toMatchObject({ stdio: 'ignore', detached: true });
  });

  it('falls back to C:\\Windows when the configured root is not absolute', () => {
    const restore = stubPlatform('win32');
    vi.stubEnv('SystemRoot', 'relative-root');
    vi.stubEnv('WINDIR', '');
    spawnMock.mockReturnValue(childStub());
    try {
      openUrlInBrowser('https://example.com');
    } finally {
      restore();
    }
    expect(spawnMock.mock.calls[0]![0]).toBe(String.raw`C:\Windows\System32\rundll32.exe`);
  });

  it('contains synchronous spawn failures', () => {
    const restore = stubPlatform('linux');
    spawnMock.mockImplementation(() => {
      throw new Error('spawn blew up');
    });
    try {
      expect(() => openUrlInBrowser('https://example.com')).not.toThrow();
    } finally {
      restore();
    }
  });
});
