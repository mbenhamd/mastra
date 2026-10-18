/**
 * Best-effort browser opening for OAuth and other login flows.
 */

import { spawn } from 'node:child_process';
import { win32 } from 'node:path';

/**
 * Open a URL in the default browser without going through a shell.
 * Only well-formed http(s) URLs are opened; anything else is ignored
 * (the URL is still displayed for the user to open manually).
 *
 * On Windows the launcher is resolved to an absolute System32 path instead
 * of a PATH-relative `rundll32`, so a writable directory earlier on PATH
 * cannot substitute the binary (PF-2587).
 */
export function openUrlInBrowser(url: string): void {
  let parsed: URL;
  try {
    parsed = new URL(url);
  } catch {
    return;
  }
  if (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') {
    return;
  }

  const configuredWindowsRoot = process.env.SystemRoot || process.env.WINDIR;
  const windowsRoot =
    configuredWindowsRoot && win32.isAbsolute(configuredWindowsRoot) ? configuredWindowsRoot : String.raw`C:\Windows`;
  const windowsLauncher = win32.join(windowsRoot, 'System32', 'rundll32.exe');

  const [cmd, args]: [string, string[]] =
    process.platform === 'darwin'
      ? ['open', [url]]
      : process.platform === 'win32'
        ? [windowsLauncher, ['url.dll,FileProtocolHandler', url]]
        : ['xdg-open', [url]];

  try {
    const child = spawn(cmd, args, { stdio: 'ignore', detached: true });
    // Opening the browser is best-effort — the URL is shown to the user as well.
    child.on('error', () => {});
    child.unref();
  } catch {
    // Ignore synchronous argument/spawn failures too.
  }
}
