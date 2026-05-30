type RequestContextLike = Record<string, unknown> | { entries(): Iterable<[string, unknown]> };

function toPlainRequestContext(requestContext?: RequestContextLike): Record<string, unknown> {
  if (!requestContext) return {};
  if (typeof (requestContext as { entries?: unknown }).entries === 'function') {
    return Object.fromEntries((requestContext as { entries(): Iterable<[string, unknown]> }).entries());
  }
  return requestContext as Record<string, unknown>;
}

export interface ResolveToolApprovalParams {
  tool: unknown;
  args?: Record<string, unknown>;
  requireToolApproval?: boolean;
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
 * Resolve whether a tool requires approval AND why. Static floors (`requireToolApproval`,
 * tool `requireApproval: true`, AI SDK boolean `needsApproval`) force `required` but contribute
 * no reason text. A conditional predicate may return a boolean (no reason) or a
 * `{ required, reason }` object; its `reason` is collected only when it actually requires
 * approval. A throwing predicate fails safe (`required: true`) with no reason, as before.
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
  if (!tool) {
    return { required: Boolean(requireToolApproval), reasons: [] };
  }

  const toolRequireApproval = (tool as any).requireApproval;
  const aiSdkNeedsApproval = (tool as any).needsApproval;
  const staticRequiresApproval = Boolean(
    requireToolApproval ||
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
    return { required: staticRequiresApproval, reasons: [] };
  }

  try {
    const needsApprovalResult = await needsApprovalFn(args ?? {}, {
      requestContext: toPlainRequestContext(requestContext),
      workspace,
    });
    const { required: dynamicRequired, reason } = normalizeApprovalDecision(needsApprovalResult);
    const required = staticRequiresApproval || dynamicRequired;
    // Only the dynamic predicate carries a reason; a static floor has no text to report.
    const reasons = dynamicRequired && reason !== undefined ? [reason] : [];
    return { required, reasons };
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
