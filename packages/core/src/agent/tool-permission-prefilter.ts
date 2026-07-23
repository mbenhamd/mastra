import type { RequestContext } from '../request-context';

export const TOOL_PERMISSION_POLICY_KEY = '__mastra_toolPermissionPolicy';
/**
 * JSON-safe durable marker. It records only that an authoritative policy must
 * be available at the action boundary; it never stores a decision or closure.
 */
export const TOOL_PERMISSION_POLICY_REQUIRED_KEY = '__mastra_toolPermissionPolicyRequired';
/**
 * Trusted assertion that {@link TOOL_PERMISSION_POLICY_KEY} is an immutable
 * per-turn snapshot. Durable foreach execution may use this assertion only to
 * choose a concurrency limit; every tool call still re-evaluates the policy at
 * its side-effect boundary.
 *
 * Callers must not set this for a policy backed by mutable state. Without the
 * assertion, durable execution remains sequential whenever a policy is
 * required so a newly observed `ask` cannot race a sibling side effect.
 */
export const TOOL_PERMISSION_POLICY_STABLE_KEY = '__mastra_toolPermissionPolicyStable';

export type ToolPermissionDecision = 'allow' | 'ask' | 'deny';
export type ToolPermissionPolicy = (toolName: string) => ToolPermissionDecision;

const DEFAULT_RUN_KEY = '__default__';
const MAX_RETAINED_RUNS_PER_CONTEXT = 64;
const MAX_RETAINED_DENIED_TOOL_NAMES = 10_000;

const deniedToolNamesByContext = new WeakMap<RequestContext, Map<string, Set<string>>>();

function runKey(runId?: string): string {
  return runId ?? DEFAULT_RUN_KEY;
}

function rememberDeniedToolName(requestContext: RequestContext, runId: string | undefined, toolName: string): void {
  let runs = deniedToolNamesByContext.get(requestContext);
  if (!runs) {
    runs = new Map();
    deniedToolNamesByContext.set(requestContext, runs);
  }

  const key = runKey(runId);
  let names = runs.get(key);
  if (!names) {
    if (runs.size >= MAX_RETAINED_RUNS_PER_CONTEXT) {
      const oldestKey = runs.keys().next().value as string | undefined;
      if (oldestKey !== undefined) runs.delete(oldestKey);
    }
    names = new Set();
    runs.set(key, names);
  }

  if (names.size < MAX_RETAINED_DENIED_TOOL_NAMES) names.add(toolName);
}

/**
 * Returns true when a tool can be omitted before its provider schema and
 * execution wrapper are constructed. This is an optimization only: callers
 * must retain the final pre-provider and action-time permission gates.
 */
export function shouldOmitToolBeforeConversion(
  requestContext: RequestContext,
  runId: string | undefined,
  toolName: string,
): boolean {
  const policy = requestContext.get(TOOL_PERMISSION_POLICY_KEY) as ToolPermissionPolicy | undefined;
  if (typeof policy !== 'function') return false;
  try {
    if (policy(toolName) !== 'deny') return false;
  } catch {
    // The action-time gate treats an unavailable policy as deny. The optional
    // conversion prefilter must fail closed the same way instead of aborting
    // tool-surface construction before the authoritative gate can run.
  }
  rememberDeniedToolName(requestContext, runId, toolName);
  return true;
}

/** Names omitted by the early conversion optimization for this execution. */
export function readPreconvertedDeniedToolNames(
  requestContext: RequestContext | undefined,
  runId: string | undefined,
): readonly string[] {
  if (!requestContext) return [];
  return [...(deniedToolNamesByContext.get(requestContext)?.get(runKey(runId)) ?? [])];
}
