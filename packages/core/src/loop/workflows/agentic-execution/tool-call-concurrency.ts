import type { ToolSet } from '@internal/ai-sdk-v5';
import type { ToolPermissionPolicy } from '../../../agent/tool-permission-prefilter';
import type { IMastraLogger } from '../../../logger';
import type { RequestContext } from '../../../request-context';
import type { RequireToolApproval } from '../../../tools';
import { resolveToolApprovalRequirement } from '../../../tools/approval';
import type { ResolvedToolApproval } from '../../../tools/approval';
import { findProviderToolByName } from '../../../tools/provider-tool-utils';
import { getNeedsApprovalFn } from '../../../tools/toolchecks';
import type { ToolApprovalContext } from '../../../tools/types';
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
  dynamicApprovalEvaluated = false,
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
  // Set when the caller evaluates function approval policies per emitted call itself.
  // Function-valued policies (run-wide or a tool's `needsApprovalFn`) are then skipped here;
  // static approval flags and suspend schemas still apply.
  dynamicApprovalEvaluated?: boolean;
}): boolean {
  if (requireToolApproval === true || (requireToolApproval && !dynamicApprovalEvaluated)) {
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
    if (maybeTool.hasSuspendSchema) {
      return true;
    }
    // Function-valued policies the caller already evaluated per emitted call
    // (with the call's actual arguments) do not force sequential execution
    // here; their verdicts are applied at the tool-call step instead.
    if (dynamicApprovalEvaluated && getNeedsApprovalFn(tool)) {
      return false;
    }
    return Boolean(maybeTool.requireApproval || maybeTool.needsApproval || maybeTool.needsApprovalFn);
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

/**
 * Resolves concurrency for a step once the model's tool calls are known.
 *
 * Each called tool's approval policy is evaluated with the call's actual arguments (the same rule
 * the tool-call step applies), so a function policy that returns `false` does not force sequential
 * execution. Called tools with a suspend schema, or whose policy requires approval for this call,
 * still force sequential execution. Under `'available'`, any active tool with a static approval
 * flag or suspend schema also forces sequential execution, as before.
 */
export async function resolveEmittedToolCallConcurrency({
  toolCalls,
  approvalVerdicts,
  approvalRequirements,
  requestContext,
  workspace,
  logger,
  ...args
}: Parameters<typeof resolveToolCallConcurrency>[0] & {
  toolCalls: readonly { toolCallId?: string; toolName: string; args?: unknown }[];
  approvalVerdicts?: Map<string, boolean>;
  /**
   * Fork: the full `{ required, reasons }` requirement per emitted call, so the
   * tool-call step can reuse both the verdict and its reasons without evaluating
   * a (possibly stateful) approval policy a second time.
   */
  approvalRequirements?: Map<string, ResolvedToolApproval>;
  requestContext?: RequestContext;
  workspace?: ToolApprovalContext['workspace'];
  logger?: IMastraLogger;
}): Promise<number> {
  // Under 'available', static approval flags and suspend schemas on any active tool still
  // force sequential execution; only function policies are resolved per emitted call below.
  if (
    args.strategy !== 'called' &&
    effectiveToolSetRequiresSequentialExecution({ ...args, dynamicApprovalEvaluated: true })
  ) {
    return 1;
  }

  // The Harness permission resolver's `ask` makes the whole batch sequential so a
  // sibling side effect cannot start before the approval suspends. It is checked per
  // emitted call under every strategy (the `'available'` scan above also covers
  // uncalled active tools) and fails closed when the resolver throws. The verdict is
  // deliberately NOT cached with the approval verdicts below: the tool-call step
  // re-applies the policy itself so a per-turn yolo can still clear it.
  let permissionAsk = false;
  if (args.permissionPolicy) {
    for (const toolCall of toolCalls) {
      try {
        if (args.permissionPolicy(toolCall.toolName) === 'ask') permissionAsk = true;
      } catch {
        permissionAsk = true;
      }
    }
  }

  const verdicts = await Promise.all(
    toolCalls.map(async toolCall => {
      // Mirror the tool-call step's lookup (key, provider name, then tool id).
      const tool =
        args.tools?.[toolCall.toolName] ||
        findProviderToolByName(args.tools, toolCall.toolName) ||
        Object.values(args.tools || {}).find(t => 'id' in t && t.id === toolCall.toolName);
      if (!tool) {
        return true;
      }
      if ((tool as { hasSuspendSchema?: unknown }).hasSuspendSchema) {
        return true;
      }
      // Same argument view the tool-call step evaluates: model-authored resume
      // control fields are stripped before any policy sees the call.
      const toolArgs =
        typeof toolCall.args === 'object' && toolCall.args !== null
          ? (({
              resumeData: _resumeData,
              suspendedToolCallId: _suspendedToolCallId,
              suspendedToolRunId: _suspendedToolRunId,
              ...rest
            }) => rest)(toolCall.args as Record<string, unknown>)
          : (toolCall.args as Record<string, unknown> | undefined);
      const requirement = await resolveToolApprovalRequirement({
        tool,
        args: toolArgs,
        requireToolApproval: args.requireToolApproval,
        requestContext,
        workspace: workspace as object | undefined,
        logger,
        toolName: toolCall.toolName,
      });
      const verdict = requirement.required;
      if (toolCall.toolCallId) {
        approvalVerdicts?.set(toolCall.toolCallId, verdict);
        approvalRequirements?.set(toolCall.toolCallId, requirement);
      }
      return verdict;
    }),
  );

  return permissionAsk || verdicts.some(Boolean) ? 1 : args.configuredConcurrency;
}
