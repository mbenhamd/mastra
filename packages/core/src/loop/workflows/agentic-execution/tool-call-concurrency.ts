import type { ToolSet } from '@internal/ai-sdk-v5';
import type { ToolPermissionPolicy } from '../../../agent/tool-permission-prefilter';
import type { RequireToolApproval } from '../../../tools';

export type ToolCallForeachOptions = {
  concurrency: number;
};

export function resolveConfiguredToolCallConcurrency(toolCallConcurrency: number | undefined): number {
  return toolCallConcurrency && toolCallConcurrency > 0 ? toolCallConcurrency : 10;
}

export function effectiveToolSetRequiresSequentialExecution({
  requireToolApproval,
  tools,
  activeTools,
  permissionPolicy,
}: {
  // A function-valued global approval policy is evaluated per call at execution time;
  // before args are known we conservatively treat it like `true` and force sequential
  // execution so approval suspensions never race with concurrent tool calls.
  requireToolApproval?: RequireToolApproval;
  tools?: ToolSet;
  activeTools?: readonly string[];
  permissionPolicy?: ToolPermissionPolicy;
}): boolean {
  if (requireToolApproval) {
    return true;
  }

  if (!tools) {
    return false;
  }

  const activeToolEntries =
    activeTools === undefined
      ? Object.entries(tools)
      : activeTools.flatMap(toolName => {
          const tool = tools[toolName];
          return tool ? ([[toolName, tool]] as const) : [];
        });

  return activeToolEntries.some(([toolName, tool]) => {
    // The Harness permission resolver is a per-turn snapshot. An `ask` tool
    // must make the whole foreach sequential so a sibling side effect cannot
    // start before the approval suspends. `allow` keeps safe batches parallel;
    // `deny` tools are removed before provider exposure and are also refused at
    // the action boundary. A throwing policy fails conservatively here.
    if (permissionPolicy) {
      try {
        if (permissionPolicy(toolName) === 'ask') return true;
      } catch {
        return true;
      }
    }
    const maybeTool = tool as {
      hasSuspendSchema?: unknown;
      requireApproval?: unknown;
      needsApproval?: unknown;
      needsApprovalFn?: unknown;
    };
    return Boolean(
      maybeTool.hasSuspendSchema || maybeTool.requireApproval || maybeTool.needsApproval || maybeTool.needsApprovalFn,
    );
  });
}

export function resolveToolCallConcurrency({
  requireToolApproval,
  tools,
  activeTools,
  permissionPolicy,
  configuredConcurrency,
}: {
  requireToolApproval?: RequireToolApproval;
  tools?: ToolSet;
  activeTools?: readonly string[];
  permissionPolicy?: ToolPermissionPolicy;
  configuredConcurrency: number;
}): number {
  return effectiveToolSetRequiresSequentialExecution({
    requireToolApproval,
    tools,
    activeTools,
    permissionPolicy,
  })
    ? 1
    : configuredConcurrency;
}

export function updateToolCallForeachConcurrency(
  options: ToolCallForeachOptions,
  args: Parameters<typeof resolveToolCallConcurrency>[0],
) {
  options.concurrency = resolveToolCallConcurrency(args);
}
