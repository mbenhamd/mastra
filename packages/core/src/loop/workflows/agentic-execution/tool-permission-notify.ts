/**
 * §O4 — optional permission-deny observability seam for the shared agentic loop.
 *
 * A caller (e.g. the harness-v1 session) MAY thread a `__mastra_onToolDenied`
 * callback on the request context alongside its `__mastra_toolPermissionPolicy`
 * resolver. The loop calls it whenever a tool is blocked by that policy — at the
 * PRE-EXPOSURE gate (the tool is dropped from the model surface) or at ACTION
 * time (a tool-call is refused) — so the caller can surface WHY a tool was
 * denied instead of the denial being silent (pre-exposure) or a generic result
 * (action).
 *
 * The callback is OPTIONAL (a strict no-op when absent, so non-harness agents
 * are unaffected), SYNCHRONOUS, fire-and-forget, and error-isolated — it must
 * never delay or break the loop, and it carries no tool args (denial telemetry
 * stays cheap + non-sensitive).
 */

export interface ToolDeniedInfo {
  toolName: string;
  stage: 'pre-exposure' | 'action';
  /** Present only for an action-time denial. */
  toolCallId?: string;
  /** True when the denial came from a forced `toolChoice` naming a denied tool (the run then throws). */
  forcedToolChoice?: boolean;
}

export type ToolDeniedCallback = (info: ToolDeniedInfo) => void;

export const TOOL_DENIED_CALLBACK_KEY = '__mastra_onToolDenied';

/** Invoke the optional deny-observability callback, isolated from the loop. */
export function notifyToolDenied(
  requestContext: { get(key: string): unknown } | undefined,
  info: ToolDeniedInfo,
): void {
  const cb = requestContext?.get(TOOL_DENIED_CALLBACK_KEY) as ToolDeniedCallback | undefined;
  if (typeof cb !== 'function') return;
  try {
    cb(info);
  } catch {
    // Observability only — never let a faulty callback break tool execution.
  }
}
