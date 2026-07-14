import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

const mocks = vi.hoisted(() => {
  const child = {
    on: vi.fn(),
    unref: vi.fn(),
  };
  return {
    child,
    exec: vi.fn(),
    hyperlink: vi.fn((text: string, url: string) => `<link:${url}>${text}</link>`),
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
    hyperlink: mocks.hyperlink,
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
    mocks.hyperlink.mockClear();
    mocks.spawn.mockClear();
    mocks.child.on.mockClear();
    mocks.child.unref.mockClear();
    tui.requestRender.mockClear();
  });

  afterEach(() => {
    vi.restoreAllMocks();
    vi.unstubAllEnvs();
  });

  it.each([
    { platform: 'darwin', command: 'open', leadingArgs: [] },
    {
      platform: 'win32',
      command: String.raw`D:\Windows\System32\rundll32.exe`,
      leadingArgs: ['url.dll,FileProtocolHandler'],
    },
    { platform: 'linux', command: 'xdg-open', leadingArgs: [] },
  ])('opens an HTTPS URL with argument arrays on $platform', ({ platform, command, leadingArgs }) => {
    vi.spyOn(process, 'platform', 'get').mockReturnValue(platform);
    vi.stubEnv('SystemRoot', String.raw`D:\Windows`);
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

  it('does not resolve the Windows launcher from the current workspace', () => {
    vi.spyOn(process, 'platform', 'get').mockReturnValue('win32');
    vi.stubEnv('SystemRoot', String.raw`C:\Windows`);
    const dialog = new LoginDialogComponent(tui as any, 'test-provider', vi.fn());

    dialog.showAuth('https://auth.example/login');

    expect(mocks.spawn).toHaveBeenCalledWith(
      String.raw`C:\Windows\System32\rundll32.exe`,
      ['url.dll,FileProtocolHandler', 'https://auth.example/login'],
      { stdio: 'ignore', detached: true },
    );
    expect(mocks.spawn).not.toHaveBeenCalledWith('rundll32', expect.anything(), expect.anything());
  });

  it('ignores a relative Windows system root', () => {
    vi.spyOn(process, 'platform', 'get').mockReturnValue('win32');
    vi.stubEnv('SystemRoot', 'workspace-bin');
    const dialog = new LoginDialogComponent(tui as any, 'test-provider', vi.fn());

    dialog.showAuth('https://auth.example/login');

    expect(mocks.spawn).toHaveBeenCalledWith(
      String.raw`C:\Windows\System32\rundll32.exe`,
      ['url.dll,FileProtocolHandler', 'https://auth.example/login'],
      { stdio: 'ignore', detached: true },
    );
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
    expect(mocks.hyperlink).toHaveBeenCalledWith('Ctrl+click to open', canonicalUrl);
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
    expect(mocks.hyperlink).not.toHaveBeenCalled();
    expect(mocks.exec).not.toHaveBeenCalled();
  });

  it('renders provider instructions without C0, DEL, C1, OSC-8, or OSC-52 controls', () => {
    vi.spyOn(process, 'platform', 'get').mockReturnValue('linux');
    const instructions = 'Code\0\x1b]8;;https://evil.example\x07click\x1b]8;;\x07\x1b]52;c;secret\x07\x7f\x9b\x9d\x9c';
    const dialog = new LoginDialogComponent(tui as any, 'test-provider', vi.fn());

    dialog.showAuth('https://auth.example/login', instructions);

    const content = (dialog as any).contentContainer.children as Array<{ text?: string }>;
    expect(content[3]?.text).toBe('Code��]8;;https://evil.example�click�]8;;��]52;c;secret�����');
    expect(content[3]?.text).not.toMatch(/[\u0000-\u001F\u007F-\u009F]/);
  });

  it('sanitizes provider prompt, placeholder, and progress text at the shared rendering boundary', () => {
    const dialog = new LoginDialogComponent(tui as any, 'test-provider', vi.fn());
    void dialog.showPrompt('Prompt\x1b[31m', 'value\x9b');
    dialog.showProgress('Progress\x1b]52;c;secret\x07');

    const content = (dialog as any).contentContainer.children as Array<{ text?: string }>;
    const rendered = content.flatMap(child => (child.text === undefined ? [] : [child.text]));
    expect(rendered).toContain('Prompt�[31m');
    expect(rendered).toContain('e.g., value�');
    expect(rendered).toContain('Progress�]52;c;secret�');
    expect(rendered.join('')).not.toMatch(/[\u0000-\u001F\u007F-\u009F]/);
  });
});
