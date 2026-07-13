/**
 * Notification utility for alerting the user when the TUI needs attention.
 * Sends a terminal bell and optionally a native OS notification.
 */

import { spawn } from 'node:child_process';
import type { HookManager } from '../hooks/manager.js';

export type NotificationMode = 'bell' | 'system' | 'both' | 'off';

export type NotificationReason = 'agent_done' | 'ask_question' | 'tool_approval' | 'plan_approval' | 'sandbox_access';

/**
 * Send a notification to the user.
 * - "bell": writes \x07 to stdout (terminal bell)
 * - "system": sends a native OS notification (macOS only for now)
 * - "both": bell + system
 * - "off": no-op
 *
 * Also fires Notification hooks if a hookManager is provided.
 */
export function sendNotification(
  reason: NotificationReason,
  opts: {
    mode: NotificationMode;
    message?: string;
    hookManager?: HookManager;
  },
): void {
  const { mode, message, hookManager } = opts;

  if (mode === 'off') {
    // Still fire hooks even when built-in notifications are off
    hookManager?.runNotification(reason, message);
    return;
  }

  if (mode === 'bell' || mode === 'both') {
    process.stdout.write('\x07');
  }

  if (mode === 'system' || mode === 'both') {
    sendSystemNotification(reason, message);
  }

  hookManager?.runNotification(reason, message);
}

function sendSystemNotification(reason: NotificationReason, message?: string): void {
  if (process.platform === 'darwin') {
    const title = 'Mastra Code';
    const body = (message || reasonToMessage(reason)).replaceAll('\0', '\uFFFD');
    // Keep the AppleScript static and pass notification content through argv.
    // Feeding the script on stdin via the documented `-` program-file marker
    // makes every later value data, even when model or tool output starts with
    // an osascript option such as `-e`.
    const script = 'on run argv\n' + 'display notification (item 1 of argv) with title (item 2 of argv)\n' + 'end run';
    try {
      const child = spawn('/usr/bin/osascript', ['-', body, title], { stdio: ['pipe', 'ignore', 'ignore'] });
      child.on('error', () => {
        // Best-effort: ignore asynchronous notification failures.
      });
      child.stdin.on('error', () => {
        // The child can exit before stdin is written when osascript is unavailable.
      });
      child.stdin.end(script);
    } catch {
      // Best-effort: ignore synchronous argument/spawn failures
    }
  }
  // Linux/Windows: could add notify-send / powershell in the future
}

function reasonToMessage(reason: NotificationReason): string {
  switch (reason) {
    case 'agent_done':
      return 'Agent finished — waiting for your input';
    case 'ask_question':
      return 'Agent has a question for you';
    case 'tool_approval':
      return 'Tool requires your approval';
    case 'plan_approval':
      return 'Plan requires your approval';
    case 'sandbox_access':
      return 'Sandbox access requested';
  }
}
