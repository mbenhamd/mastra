import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

const mocks = vi.hoisted(() => {
  const child = {
    on: vi.fn(),
    unref: vi.fn(),
  };
  return {
    child,
    exec: vi.fn(),
    spawn: vi.fn(() => child),
  };
});

vi.mock('node:child_process', () => ({
  exec: mocks.exec,
  spawn: mocks.spawn,
}));

vi.mock('@earendil-works/pi-tui', () => {
  class Box {
    children: unknown[] = [];
    addChild(child: unknown) {
      this.children.push(child);
    }
  }

  class Container extends Box {
    clear() {
      this.children = [];
    }
  }

  return {
    Box,
    Container,
    Spacer: class {
      constructor(public height: number) {}
    },
    Text: class {
      constructor(public text: string) {}
    },
    getKeybindings: () => ({ matches: () => false }),
  };
});

vi.mock('../../auth/index.js', () => ({
  getOAuthProviders: () => [],
}));

vi.mock('../theme.js', () => ({
  theme: {
    bg: () => (text: string) => text,
    fg: (_color: string, text: string) => text,
  },
}));

vi.mock('./masked-input.js', () => ({
  MaskedInput: class {
    focused = false;
    onSubmit?: () => void;
    onEscape?: () => void;
    getValue() {
      return '';
    }
    setValue() {}
    handleInput() {}
  },
}));

import { LoginDialogComponent } from './login-dialog.js';

describe('LoginDialogComponent browser opening', () => {
  const tui = { requestRender: vi.fn() };

  beforeEach(() => {
    mocks.exec.mockClear();
    mocks.spawn.mockClear();
    mocks.child.on.mockClear();
    mocks.child.unref.mockClear();
    tui.requestRender.mockClear();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it.each([
    { platform: 'darwin', command: 'open', leadingArgs: [] },
    { platform: 'win32', command: 'rundll32', leadingArgs: ['url.dll,FileProtocolHandler'] },
    { platform: 'linux', command: 'xdg-open', leadingArgs: [] },
  ])('opens an HTTPS URL with argument arrays on $platform', ({ platform, command, leadingArgs }) => {
    vi.spyOn(process, 'platform', 'get').mockReturnValue(platform);
    const url = 'https://auth.example/callback?state="; touch /tmp/pwned';
    const canonicalUrl = new URL(url).href;
    const dialog = new LoginDialogComponent(tui as any, 'test-provider', vi.fn());

    dialog.showAuth(url);

    expect(mocks.spawn).toHaveBeenCalledWith(command, [...leadingArgs, canonicalUrl], {
      stdio: 'ignore',
      detached: true,
    });
    expect(mocks.exec).not.toHaveBeenCalled();
    expect(mocks.child.on).toHaveBeenCalledWith('error', expect.any(Function));
    expect(mocks.child.unref).toHaveBeenCalledTimes(1);
    expect(tui.requestRender).toHaveBeenCalledTimes(1);
  });

  it('opens a well-formed HTTP URL', () => {
    vi.spyOn(process, 'platform', 'get').mockReturnValue('linux');
    const url = 'http://localhost:3000/oauth/callback';
    const dialog = new LoginDialogComponent(tui as any, 'test-provider', vi.fn());

    dialog.showAuth(url);

    expect(mocks.spawn).toHaveBeenCalledWith('xdg-open', [url], {
      stdio: 'ignore',
      detached: true,
    });
    expect(mocks.exec).not.toHaveBeenCalled();
  });

  it('canonicalizes control bytes before displaying or opening a URL', () => {
    vi.spyOn(process, 'platform', 'get').mockReturnValue('linux');
    const url = 'https://auth.example/callback?state=before\0\x1b\x07after';
    const canonicalUrl = new URL(url).href;
    const dialog = new LoginDialogComponent(tui as any, 'test-provider', vi.fn());

    expect(() => dialog.showAuth(url)).not.toThrow();

    expect(canonicalUrl).not.toContain('\0');
    expect(canonicalUrl).not.toContain('\x1b');
    expect(canonicalUrl).not.toContain('\x07');
    expect(mocks.spawn).toHaveBeenCalledWith('xdg-open', [canonicalUrl], {
      stdio: 'ignore',
      detached: true,
    });
    const content = (dialog as any).contentContainer.children as Array<{ text?: string }>;
    expect(content[0]?.text).toBe(canonicalUrl);
    expect(content[1]?.text).toContain(canonicalUrl);
    expect(mocks.exec).not.toHaveBeenCalled();
  });

  it('ignores synchronous browser-launch failures', () => {
    vi.spyOn(process, 'platform', 'get').mockReturnValue('linux');
    mocks.spawn.mockImplementationOnce(() => {
      throw new Error('spawn failed');
    });
    const dialog = new LoginDialogComponent(tui as any, 'test-provider', vi.fn());

    expect(() => dialog.showAuth('https://auth.example/login')).not.toThrow();

    expect(tui.requestRender).toHaveBeenCalledTimes(1);
    expect(mocks.exec).not.toHaveBeenCalled();
  });

  it.each(['javascript:alert(1)', 'not a valid URL'])('does not open unsafe URL %s', url => {
    vi.spyOn(process, 'platform', 'get').mockReturnValue('linux');
    const dialog = new LoginDialogComponent(tui as any, 'test-provider', vi.fn());

    dialog.showAuth(url);

    expect(mocks.spawn).not.toHaveBeenCalled();
    expect(mocks.exec).not.toHaveBeenCalled();
    expect(tui.requestRender).toHaveBeenCalledTimes(1);
  });

  it('renders a rejected URL without creating a terminal hyperlink', () => {
    vi.spyOn(process, 'platform', 'get').mockReturnValue('linux');
    const url = 'javascript:alert(1)\x1b]8;;https://evil.example\x07\x9b\x9d\x9c';
    const dialog = new LoginDialogComponent(tui as any, 'test-provider', vi.fn());

    dialog.showAuth(url);

    const content = (dialog as any).contentContainer.children as Array<{ text?: string }>;
    expect(content).toHaveLength(1);
    expect(content[0]?.text).toBe('javascript:alert(1)�]8;;https://evil.example����');
    expect(mocks.spawn).not.toHaveBeenCalled();
    expect(mocks.exec).not.toHaveBeenCalled();
  });
});
