import { ASK_USER_TOOL_ID, SUBMIT_PLAN_TOOL_ID } from '../../tools/builtin';
import { HarnessValidationError } from './errors';
import { HARNESS_SUBAGENT_OUTCOME_REPORT_TOOL_ID } from './terminal-subagent-result';
import type { PermissionPolicy, ToolCategory } from './types';

// ---------------------------------------------------------------------------
// Permission helpers (§4.2e). Tiny shape validators kept module-scoped so the
// session methods stay focused on persistence + event emission.
// ---------------------------------------------------------------------------

const TOOL_CATEGORIES: readonly ToolCategory[] = ['read', 'edit', 'execute', 'mcp', 'other'];

const PERMISSION_POLICIES: readonly PermissionPolicy[] = ['allow', 'ask', 'deny'];

/**
 * Harness-owned control tools that must remain callable for the protocol itself
 * to make progress. `ask_user` / `submit_plan` are the suspension boundary and
 * `report_subagent_outcome` is the child session's mandatory terminal envelope.
 *
 * Plan mutation/check/delegation tools and `spawn_subagent` are intentionally
 * absent: they mutate durable state or expand execution capability, so they obey
 * the same permission policy and subagent capability allowlist as application
 * tools. Kept in sync with `_buildToolSurface`.
 */
export const UNGATED_HARNESS_BUILTIN_TOOL_IDS: ReadonlySet<string> = new Set<string>([
  HARNESS_SUBAGENT_OUTCOME_REPORT_TOOL_ID,
  ASK_USER_TOOL_ID,
  SUBMIT_PLAN_TOOL_ID,
]);

export function assertToolCategory(method: string, value: unknown): asserts value is ToolCategory {
  if (typeof value !== 'string' || !TOOL_CATEGORIES.includes(value as ToolCategory)) {
    throw new HarnessValidationError(method, `unknown ToolCategory ${JSON.stringify(value)}`);
  }
}

export function assertToolName(method: string, value: unknown): asserts value is string {
  if (typeof value !== 'string' || value.length === 0) {
    throw new HarnessValidationError(method, 'toolName must be a non-empty string');
  }
}

export function assertPolicy(method: string, value: unknown): asserts value is PermissionPolicy {
  if (typeof value !== 'string' || !PERMISSION_POLICIES.includes(value as PermissionPolicy)) {
    throw new HarnessValidationError(method, `policy must be one of ${PERMISSION_POLICIES.join(' | ')}`);
  }
}
