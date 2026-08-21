import type { ToolSet } from '@internal/ai-sdk-v5';
import type { ToolPermissionPolicy } from '../../../agent/tool-permission-prefilter';
import type { RequireToolApproval } from '../../../tools';
import type { ToolCallConcurrency, ToolCallConcurrencyStrategy } from '../../types';

export type ToolCallForeachOptions = {
  concurrency: number;
};

const DEFAULT_TOOL_CALL_CONCURRENCY = 10;

/**
 * Normalize the public `toolCallConcurrency` option (a number or an object with
 * `limit`/`strategy`) into a resolved `{ limit, strategy }` pair.
 */
export function normalizeToolCallConcurrency(toolCallConcurrency: ToolCallConcurrency | undefined): {
  limit: number;
  strategy: ToolCallConcurrencyStrategy;
} {
  if (typeof toolCallConcurrency === 'object' && toolCallConcurrency !== null) {
    const limit = toolCallConcurrency.limit;
    return {
      limit: typeof limit === 'number' && limit > 0 ? limit : DEFAULT_TOOL_CALL_CONCURRENCY,
      strategy: toolCallConcurrency.strategy ?? 'available',
    };
  }
  return {
    limit: toolCallConcurrency && toolCallConcurrency > 0 ? toolCallConcurrency : DEFAULT_TOOL_CALL_CONCURRENCY,
    strategy: 'available',
  };
}

export function resolveConfiguredToolCallConcurrency(toolCallConcurrency: ToolCallConcurrency | undefined): number {
  return normalizeToolCallConcurrency(toolCallConcurrency).limit;
}

export function effectiveToolSetRequiresSequentialExecution({
  requireToolApproval,
  tools,
  activeTools,
  permissionPolicy,
  strategy = 'available',
  calledToolNames,
}: {
  // A function-valued global approval policy is evaluated per call at execution time;
  // before args are known we conservatively treat it like `true` and force sequential
  // execution so approval suspensions never race with concurrent tool calls.
  requireToolApproval?: RequireToolApproval;
  tools?: ToolSet;
  activeTools?: readonly string[];
  permissionPolicy?: ToolPermissionPolicy;
  strategy?: ToolCallConcurrencyStrategy;
  // The tool names the model actually called this step. Only consulted under the
  // `'called'` strategy; when omitted there, nothing forces sequential (a batch
  // that called no suspend/approval tool cannot suspend this step).
  calledToolNames?: readonly string[];
}): boolean {
  if (requireToolApproval) {
    return true;
  }

  if (!tools) {
    return false;
  }

  const consideredToolEntries =
    strategy === 'called'
      ? (calledToolNames ?? []).flatMap(toolName => {
          const tool = tools[toolName];
          return tool ? ([[toolName, tool]] as const) : [];
        })
      : activeTools === undefined
        ? Object.entries(tools)
        : activeTools.flatMap(toolName => {
            const tool = tools[toolName];
            return tool ? ([[toolName, tool]] as const) : [];
          });

  return consideredToolEntries.some(([toolName, tool]) => {
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
  strategy,
  calledToolNames,
}: {
  requireToolApproval?: RequireToolApproval;
  tools?: ToolSet;
  activeTools?: readonly string[];
  permissionPolicy?: ToolPermissionPolicy;
  configuredConcurrency: number;
  strategy?: ToolCallConcurrencyStrategy;
  calledToolNames?: readonly string[];
}): number {
  return effectiveToolSetRequiresSequentialExecution({
    requireToolApproval,
    tools,
    activeTools,
    permissionPolicy,
    strategy,
    calledToolNames,
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

/**
 * Per-batch concurrency: scans only the tools the model actually CALLED this
 * step. Sequential execution exists to keep approval/suspension flows from
 * racing sibling side effects — a property of the calls that will EXECUTE:
 * every per-call hazard (permission-policy `ask`, suspend schemas, static or
 * dynamic approval flags) is checked against the called subset, so a batch
 * containing any such call still serializes. A registered ask/suspend tool the
 * model did NOT call cannot park or approve anything this step, and scanning
 * it anyway forced every turn on surfaces that expose ask-family tools down to
 * one-at-a-time execution — observed live as "parallel" research fan-outs and
 * multi-spawn subagent batches running serially. The global function-valued
 * `requireToolApproval` still short-circuits to sequential inside the resolver
 * (args are unknown before execution). Hallucinated names resolve to no tool
 * entry and are ignored; a called name outside the step's active set still
 * scans its registered entry, which only ever errs toward sequential.
 */
export function resolveCalledBatchToolCallConcurrency({
  toolCalls,
  requireToolApproval,
  tools,
  permissionPolicy,
  configuredConcurrency,
}: {
  toolCalls: ReadonlyArray<{ toolName?: unknown }>;
  requireToolApproval?: RequireToolApproval;
  tools?: ToolSet;
  permissionPolicy?: ToolPermissionPolicy;
  configuredConcurrency: number;
}): number {
  const calledToolNames = [
    ...new Set(
      toolCalls
        .map(toolCall => toolCall.toolName)
        .filter((toolName): toolName is string => typeof toolName === 'string'),
    ),
  ];
  return resolveToolCallConcurrency({
    requireToolApproval,
    tools,
    activeTools: calledToolNames,
    permissionPolicy,
    configuredConcurrency,
  });
}
