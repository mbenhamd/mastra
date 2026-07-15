import type { RequireToolApproval, ToolApprovalContext } from './types';

type RequestContextLike = Record<string, unknown> | { entries(): Iterable<[string, unknown]> };

function toPlainRequestContext(requestContext?: RequestContextLike): Record<string, unknown> {
  if (!requestContext) return {};
  if (typeof (requestContext as { entries?: unknown }).entries === 'function') {
    return Object.fromEntries((requestContext as { entries(): Iterable<[string, unknown]> }).entries());
  }
  return requestContext as Record<string, unknown>;
}

/**
 * Plain request-context view for approval policies, with internal transport keys
 * (`__mastra_*`) excluded so policies only see public entries (#17337).
 */
function toPolicyRequestContext(requestContext?: RequestContextLike): Record<string, unknown> {
  return Object.fromEntries(
    Object.entries(toPlainRequestContext(requestContext)).filter(([k]) => !k.startsWith('__mastra_')),
  );
}

export interface ResolveToolApprovalParams {
  tool: unknown;
  args?: Record<string, unknown>;
  /**
   * Global approval setting (#17337): `true` floors every call, a FUNCTION is
   * evaluated per call with a {@link ToolApprovalContext} (throwing policies
   * fail safe to `required`).
   */
  requireToolApproval?: RequireToolApproval;
  requestContext?: RequestContextLike;
  workspace?: object;
  logger?: { error: (...args: any[]) => void };
  toolName?: string;
}

/** Resolved tool-approval requirement, including any reason(s) the predicate surfaced. */
export interface ResolvedToolApproval {
  required: boolean;
  /** Human-readable reasons the tool requires approval (from a conditional predicate). */
  reasons: string[];
}

/** Normalize a conditional predicate's return into a `{ required, reason }` shape. */
function normalizeApprovalDecision(result: unknown): { required: boolean; reason?: string } {
  if (typeof result === 'object' && result !== null && 'required' in result) {
    const decision = result as { required?: unknown; reason?: unknown };
    return {
      required: Boolean(decision.required),
      reason: typeof decision.reason === 'string' && decision.reason.length > 0 ? decision.reason : undefined,
    };
  }
  return { required: Boolean(result) };
}

/**
 * Resolve whether a tool requires approval AND why.
 *
 * Precedence (upstream #17337 semantics): the global `requireToolApproval`
 * (boolean, or a per-call function policy) and the tool's own static flags
 * (`requireApproval: true`, AI SDK boolean `needsApproval`) SEED the decision;
 * a per-tool conditional predicate (`needsApprovalFn` / function-typed
 * `requireApproval` / function-typed `needsApproval`), when present, is
 * authoritative and OVERRIDES the seed — it may return `false` to allow a call
 * the global policy or static flag would otherwise gate. A throwing policy or
 * predicate fails safe (`required: true`) with no reason.
 *
 * The predicate may return a boolean (no reason) or a `{ required, reason }`
 * object; its `reason` is collected only when it actually requires approval.
 */
export async function resolveToolApprovalRequirement({
  tool,
  args,
  requireToolApproval,
  requestContext,
  workspace,
  logger,
  toolName,
}: ResolveToolApprovalParams): Promise<ResolvedToolApproval> {
  // Evaluate the global policy first — it can be a per-call function (#17337).
  let globalRequiresApproval: boolean;
  if (typeof requireToolApproval === 'function') {
    try {
      const ctx: ToolApprovalContext = {
        toolName: toolName ?? '',
        args: args ?? {},
        requestContext: toPolicyRequestContext(requestContext),
        workspace: workspace as ToolApprovalContext['workspace'],
      };
      globalRequiresApproval = Boolean(await requireToolApproval(ctx));
    } catch (error) {
      logger?.error(`Error evaluating global requireToolApproval for tool ${toolName ?? 'unknown'}:`, error);
      // Fail safe: a throwing policy requires approval.
      globalRequiresApproval = true;
    }
  } else {
    globalRequiresApproval = Boolean(requireToolApproval);
  }

  if (!tool) {
    return { required: globalRequiresApproval, reasons: [] };
  }

  const toolRequireApproval = (tool as any).requireApproval;
  const aiSdkNeedsApproval = (tool as any).needsApproval;
  const seedRequiresApproval = Boolean(
    globalRequiresApproval ||
    (typeof toolRequireApproval === 'boolean' && toolRequireApproval) ||
    (typeof aiSdkNeedsApproval === 'boolean' && aiSdkNeedsApproval),
  );
  const needsApprovalFn =
    typeof (tool as any).needsApprovalFn === 'function'
      ? (tool as any).needsApprovalFn
      : typeof toolRequireApproval === 'function'
        ? toolRequireApproval
        : typeof aiSdkNeedsApproval === 'function'
          ? aiSdkNeedsApproval
          : undefined;

  if (!needsApprovalFn) {
    return { required: seedRequiresApproval, reasons: [] };
  }

  try {
    const needsApprovalResult = await needsApprovalFn(args ?? {}, {
      requestContext: toPlainRequestContext(requestContext),
      workspace,
    });
    const { required: dynamicRequired, reason } = normalizeApprovalDecision(needsApprovalResult);
    // #17337 precedence: the per-tool predicate OVERRIDES the seed — it may
    // return false to allow a call the global policy/static flag would gate.
    const reasons = dynamicRequired && reason !== undefined ? [reason] : [];
    return { required: dynamicRequired, reasons };
  } catch (error) {
    logger?.error(`Error evaluating needsApprovalFn for tool ${toolName ?? 'unknown'}:`, error);
    return { required: true, reasons: [] };
  }
}

/**
 * Boolean-only view of {@link resolveToolApprovalRequirement}. Preserved as the stable exported
 * contract for callers that only need the gate (durable boolean checks, the loop/network paths).
 */
export async function resolveToolRequiresApproval(params: ResolveToolApprovalParams): Promise<boolean> {
  return (await resolveToolApprovalRequirement(params)).required;
}
